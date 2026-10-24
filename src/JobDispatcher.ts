import PQueue from "p-queue";
import type { Worker, JobExecution, JobOutcome } from "./Worker";
import { failedOutcome } from "./Failure";

export interface JobStore {
  readonly dispatchLimitReached?: boolean;
  claimNext(): JobExecution | null;
  complete(job: JobExecution, result: string): void;
  fail(job: JobExecution, error: string): void;
  release(job: JobExecution, error: string): void;
  renew(job: JobExecution): void;
}

/** One in-memory circuit. Only its designated probe can recover an outage. */
export class JobDispatcher {
  private state: "closed" | "open" | "half-open" | "blocked" = "closed";
  private backoff = 30_000;
  private ready = false;
  private probe?: JobExecution;
  private timer?: ReturnType<typeof setTimeout>;
  private running?: Promise<void>;

  constructor(
    private readonly pool: PQueue,
    private readonly worker: Worker,
    private readonly jobs: JobStore,
  ) {}

  get blocked(): boolean { return this.state === "blocked"; }

  start(signal: AbortSignal = new AbortController().signal): Promise<void> {
    if (this.running) return this.running;
    const controller = new AbortController();
    const runSignal = AbortSignal.any([signal, controller.signal]);
    let active = 0;
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const done = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    this.running = done;
    const finish = () => {
      clearTimeout(this.timer);
      this.timer = undefined;
      runSignal.removeEventListener("abort", pump);
      this.running = undefined;
      if (runSignal.aborted) reject(runSignal.reason);
      else resolve();
    };
    const open = () => {
      this.state = "open";
      this.ready = false;
      clearTimeout(this.timer);
      this.timer = setTimeout(() => {
        this.timer = undefined;
        if (this.state !== "open") return;
        this.ready = true;
        pump();
      }, this.backoff);
    };
    const apply = (job: JobExecution, outcome: JobOutcome) => {
      const isProbe = this.probe === job && this.state === "half-open";
      if (outcome.status === "completed") this.jobs.complete(job, outcome.result);
      else if (outcome.kind === "job") this.jobs.fail(job, outcome.error);
      else this.jobs.release(job, outcome.error);

      if (outcome.status === "failed" && outcome.kind === "blocked") {
        this.state = "blocked";
        this.probe = undefined;
        clearTimeout(this.timer);
        this.timer = undefined;
      } else if (this.state !== "blocked") {
        if (outcome.status === "failed" && outcome.kind === "transient") {
          if (isProbe) {
            this.backoff = Math.min(this.backoff * 2, 300_000);
            this.probe = undefined;
            open();
          } else if (this.state === "closed") open();
        } else if (isProbe) {
          this.probe = undefined;
          this.state = "closed";
          this.backoff = 30_000;
        }
      }
    };
    const launch = (job: JobExecution) => {
      active++;
      void this.pool.add(async () => {
        const heartbeat = setInterval(() => {
          try { this.jobs.renew(job); }
          catch (error) { controller.abort(error); }
        }, 60_000);
        try {
          let outcome: JobOutcome;
          try { outcome = await this.worker.run(job, runSignal); }
          catch (error) { outcome = failedOutcome(error); }
          runSignal.throwIfAborted();
          apply(job, outcome);
        } finally { clearInterval(heartbeat); }
      }).catch(error => { controller.abort(error); }).finally(() => {
        active--;
        pump();
      });
    };
    const pump = () => {
      try {
        if (!runSignal.aborted) {
          while (active < this.pool.concurrency && !this.pool.isPaused) {
            // A spent budget must not wait for circuit backoff or dispatch a probe.
            if (this.jobs.dispatchLimitReached) {
              if (active === 0) finish();
              return;
            }
            if (this.state === "blocked" || this.state === "half-open") break;
            if (this.state === "open" && !this.ready) break;
            const job = this.jobs.claimNext();
            if (!job) {
              if (active === 0) finish();
              return;
            }
            if (this.state === "open") {
              this.state = "half-open";
              this.probe = job;
            }
            launch(job);
          }
        }
        if (active === 0 && (runSignal.aborted || this.state === "blocked")) finish();
      } catch (error) { controller.abort(error); }
    };
    runSignal.addEventListener("abort", pump);
    pump();
    return done;
  }
}

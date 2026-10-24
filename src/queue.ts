import { writeFileSync } from "node:fs";
import { basename, resolve, sep } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import PQueue from "p-queue";
import type { Worker } from "./Worker";
import { JobDispatcher, type JobStore } from "./JobDispatcher";
import type { JobExecution } from "./Worker";
import type { QueueProgress, QueueSettings, QueueSettingsUpdate, QueueStore } from "./types";

export type { JobStatus, Job, ClaimedJob } from "./types";

export function normalizeDatabasePath(path: string): string {
  const name = basename(path);

  if (
    path.includes("\0") ||
    path.endsWith(sep) ||
    name === "" ||
    name === "." ||
    name === ".."
  ) {
    throw new Error("Database path must include a valid filename");
  }

  return path.endsWith(".db") ? path : `${path}.db`;
}

export interface RunOptions {
  /** Maximum dispatches for this run, including failures and retries. Omit to drain the queue. */
  maxJobs?: number;
  onProgress?: (progress: QueueProgress) => void;
}

interface QueueDependencies {
  createWorker?(): Worker;
  withExistingQueueAsync?<T>(path: string, operation: (store: QueueStore) => Promise<T>): Promise<T>;
  initializeQueue(path: string, settings: QueueSettings): void;
  withExistingQueue<T>(
    path: string,
    operation: (store: QueueStore) => T,
  ): T;
}

/** Construct application operations without opening databases or starting workers. */
export function createQueueService(deps: QueueDependencies) {
  const runs = new Map<string, AbortController>();
  return {
    async runQueue(path: string, options: RunOptions = {}): Promise<QueueProgress> {
      const databasePath = resolve(normalizeDatabasePath(path));
      if (runs.has(databasePath)) throw new Error("This queue is already running.");
      if (!deps.withExistingQueueAsync || !deps.createWorker) {
        throw new Error("Queue execution dependencies are not configured.");
      }
      const controller = new AbortController();
      runs.set(databasePath, controller);
      try {
        const worker = deps.createWorker();
        return await deps.withExistingQueueAsync(databasePath, store =>
          processQueue(store, worker, controller.signal, options),
        );
      } finally {
        runs.delete(databasePath);
      }
    },

    shutdown(): void {
      for (const controller of runs.values()) {
        controller.abort(new Error("Queue execution interrupted by session shutdown."));
      }
    },

    initQueue(
      path: string,
      prompt: string,
      agent: string,
      poolSize: number = 4,
      maxAttempts: number = 3,
    ): void {
      deps.initializeQueue(normalizeDatabasePath(path), {
        prompt, agent, poolSize, maxAttempts,
      });
    },

    reset(path: string): number {
      return deps.withExistingQueue(normalizeDatabasePath(path), store => {
        return store.reset();
      });
    },

    updateSettings(path: string, options: QueueSettingsUpdate): void {
      deps.withExistingQueue(normalizeDatabasePath(path), store => {
        store.updateSettings(options);
      });
    },

    exportQueue(path: string, output: string): number {
      const jobs = deps.withExistingQueue(normalizeDatabasePath(path), store =>
        store.getJobsForExport(),
      );
      const jsonl = jobs.map(job => JSON.stringify(job) + "\n").join("");
      // Never overwrite an existing file, including the source database.
      writeFileSync(output, jsonl, { encoding: "utf8", flag: "wx" });
      return jobs.length;
    },

    addJob(path: string, inputs: readonly string[]): number[] {
      if (inputs.length === 0 || inputs.some(input => !input.trim())) {
        throw new Error("Provide at least one nonblank job input.");
      }
      return deps.withExistingQueue(normalizeDatabasePath(path), store => {
        return store.addJob(inputs);
      });
    },
  };
}

export type QueueService = ReturnType<typeof createQueueService>;

/** Run jobs up to the dispatch limit, waiting for interrupted leases when necessary. */
export async function processQueue(
  store: QueueStore,
  worker: Worker,
  signal: AbortSignal,
  options: RunOptions = {},
): Promise<QueueProgress> {
  if (options.maxJobs !== undefined &&
      (!Number.isSafeInteger(options.maxJobs) || options.maxJobs < 1)) {
    throw new Error("maxJobs must be a positive integer.");
  }
  let dispatched = 0;
  const limitReached = () => dispatched >= (options.maxJobs ?? Infinity);
  const settings = store.getSettings();
  if (!Number.isSafeInteger(settings.poolSize) || settings.poolSize < 1 ||
      !Number.isSafeInteger(settings.maxAttempts) || settings.maxAttempts < 1) {
    throw new Error("poolSize and maxAttempts must be positive integers.");
  }

  const report = () => {
    const progress = store.getProgress();
    options.onProgress?.(progress);
    return progress;
  };
  const requireLease = (updated: boolean, job: JobExecution) => {
    if (!updated) throw new Error(`Lost lease for job ${job.id}.`);
  };
  const jobs: JobStore = {
    get dispatchLimitReached() { return limitReached(); },
    claimNext() {
      if (limitReached()) return null;
      const claimed = store.claimNextJob(settings.maxAttempts, 300);
      if (!claimed) return null;
      dispatched++;
      report();
      return {
        id: claimed.id,
        leaseToken: claimed.lease_token,
        agent: settings.agent,
        prompt: `${settings.prompt} ${claimed.input}`,
      };
    },
    complete(job, result) {
      requireLease(store.completeJob(job, result), job);
      report();
    },
    fail(job, error) {
      requireLease(store.failJob(job, error), job);
      report();
    },
    release(job, error) {
      requireLease(store.releaseJob(job, error), job);
      report();
    },
    renew(job) {
      requireLease(store.renewLease(job, 300), job);
    },
  };
  const dispatcher = new JobDispatcher(new PQueue({ concurrency: settings.poolSize }), worker, jobs);

  while (true) {
    signal.throwIfAborted();
    store.recoverExpiredJobs(settings.maxAttempts);
    report();
    await dispatcher.start(signal);
    const progress = report();
    if (limitReached() || dispatcher.blocked || (progress.pending === 0 && progress.running === 0)) return progress;
    // A previous process may have left unexpired leases. Do not steal live work.
    await delay(1_000, undefined, { signal });
  }
}

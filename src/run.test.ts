import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { setImmediate as flush } from "node:timers/promises";
import { processQueue, createQueueService } from "./queue";
import { SqliteStore } from "./sqlite-store";
import type { QueueProgress, QueueStore } from "./types";
import type { JobExecution, Worker } from "./Worker";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

function fixture(poolSize = 2, maxAttempts = 3) {
  const db = new DatabaseSync(":memory:");
  const store = new SqliteStore(db);
  store.init({ prompt: "Process", agent: "worker", poolSize, maxAttempts });
  return { db, store, signal: new AbortController().signal };
}

test("drain fills only available slots, passes configured inputs, and reports committed results", async () => {
  const { db, store, signal } = fixture(2);
  try {
    const inputs = [" first \n", "second", "third"];
    const ids = store.addJob(inputs);
    const progress: QueueProgress[] = [];
    const executions: JobExecution[] = [];
    const finishes = inputs.map(() => deferred());
    let settled = false;
    const run = processQueue(store, {
      async run(job) {
        executions.push(job);
        await finishes[ids.indexOf(job.id)].promise;
        return { status: "completed", result: `result ${job.id}` };
      },
    }, signal, {
      onProgress(snapshot) {
        assert.deepEqual(snapshot, store.getProgress(), "report only persisted state");
        progress.push(snapshot);
      },
    });
    void run.then(() => { settled = true; });
    assert.deepEqual(executions.map(job => job.id), ids.slice(0, 2));
    assert.deepEqual(store.getProgress(), { pending: 1, running: 2, completed: 0, failed: 0 });

    finishes[1].resolve();
    await flush();
    assert.deepEqual(executions.map(({ id, agent, prompt }) => ({ id, agent, prompt })),
      ids.map((id, index) => ({ id, agent: "worker", prompt: `Process ${inputs[index]}` })));
    finishes[0].resolve();
    await flush();
    assert.equal(settled, false, "wait for the final worker even when no jobs remain pending");
    finishes[2].resolve();
    const summary = await run;

    assert.deepEqual(summary, { pending: 0, running: 0, completed: 3, failed: 0 });
    assert.deepEqual(progress[0], { pending: 3, running: 0, completed: 0, failed: 0 });
    assert.deepEqual(progress.at(-1), summary);
    assert.deepEqual(store.getJobsForExport().map(row => ({ ...row })), inputs.map((input, index) => ({
      input, status: "completed", result: `result ${ids[index]}`,
    })));
  } finally { db.close(); }
});

test("thrown and returned job failures remain terminal without stopping other jobs", async () => {
  const { db, store, signal } = fixture();
  try {
    store.addJob(["throws", "fails", "succeeds"]);
    const attempts = new Map<number, number>();
    const worker: Worker = {
      async run(job) {
        attempts.set(job.id, (attempts.get(job.id) ?? 0) + 1);
        if (job.id === 1) throw new Error("worker threw");
        if (job.id === 2) return { status: "failed", error: "worker failed", kind: "job" };
        return { status: "completed", result: "ok" };
      },
    };
    assert.deepEqual(await processQueue(store, worker, signal), {
      pending: 0, running: 0, completed: 1, failed: 2,
    });
    assert.deepEqual([...attempts].sort(), [[1, 1], [2, 1], [3, 1]]);
    assert.deepEqual(db.prepare("SELECT status, attempts, result, error FROM jobs ORDER BY id").all()
      .map(row => ({ ...row })), [
      { status: "failed", attempts: 1, result: null, error: "worker threw" },
      { status: "failed", attempts: 1, result: null, error: "worker failed" },
      { status: "completed", attempts: 1, result: "ok", error: null },
    ]);
    await processQueue(store, worker, signal);
    assert.deepEqual([...attempts].sort(), [[1, 1], [2, 1], [3, 1]], "later drains must not retry terminal jobs");
  } finally { db.close(); }
});

for (const poolSize of [1, 2, 5]) {
  test(`dispatch limit counts successes and failures with pool size ${poolSize}`, async () => {
    const { db, store, signal } = fixture(poolSize);
    try {
      store.addJob(["succeeds", "throws", "fails", "later", "last"]);
      const executed: number[] = [];
      const worker: Worker = {
        async run(job) {
          executed.push(job.id);
          if (job.id === 2) throw new Error("worker threw");
          if (job.id === 3) return { status: "failed", error: "failed", kind: "job" };
          return { status: "completed", result: "ok" };
        },
      };
      assert.deepEqual(await processQueue(store, worker, signal, { maxJobs: 3 }), {
        pending: 2, running: 0, completed: 1, failed: 2,
      });
      assert.deepEqual(executed, [1, 2, 3]);
      assert.deepEqual(await processQueue(store, worker, signal, { maxJobs: 3 }), {
        pending: 0, running: 0, completed: 3, failed: 2,
      });
      assert.deepEqual(executed, [1, 2, 3, 4, 5], "each run gets a fresh budget");
    } finally { db.close(); }
  });
}

test("dispatch limit waits for started workers without claiming extra work", async () => {
  const { db, store, signal } = fixture(5);
  try {
    store.addJob(["one", "two", "three"]);
    const finish = deferred();
    const executed: number[] = [];
    let settled = false;
    const run = processQueue(store, {
      async run(job) {
        executed.push(job.id);
        await finish.promise;
        return { status: "completed", result: "ok" };
      },
    }, signal, { maxJobs: 2 });
    void run.then(() => { settled = true; });
    await flush();
    assert.deepEqual(executed, [1, 2]);
    assert.equal(settled, false);
    finish.resolve();
    assert.deepEqual(await run, { pending: 1, running: 0, completed: 2, failed: 0 });
  } finally { db.close(); }
});

test("dispatch limit returns immediately after a transient failure exhausts the budget", async () => {
  const { db, store, signal } = fixture(1);
  try {
    store.addJob(["retry later"]);
    let executed = 0;
    assert.deepEqual(await processQueue(store, {
      async run() {
        executed++;
        return { status: "failed", error: "temporary outage", kind: "transient" };
      },
    }, signal, { maxJobs: 1 }), { pending: 1, running: 0, completed: 0, failed: 0 });
    assert.equal(executed, 1);
  } finally { db.close(); }
});

test("dispatch limit rejects invalid values before claiming jobs", async () => {
  const { db, store, signal } = fixture();
  try {
    store.addJob(["untouched"]);
    for (const maxJobs of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      await assert.rejects(processQueue(store, {
        run() { assert.fail("must not execute with an invalid limit"); },
      }, signal, { maxJobs }), /maxJobs must be a positive integer/);
    }
    assert.deepEqual(store.getProgress(), { pending: 1, running: 0, completed: 0, failed: 0 });
  } finally { db.close(); }
});

test("blocked processing returns pending work and a manual run starts a fresh circuit", async () => {
  const { db, store, signal } = fixture(1);
  try {
    store.addJob(["one", "two"]);
    let executions = 0;
    assert.deepEqual(await processQueue(store, {
      async run() {
        executions++;
        throw new Error("insufficient_quota");
      },
    }, signal), { pending: 2, running: 0, completed: 0, failed: 0 });
    assert.equal(executions, 1);
    const row = db.prepare("SELECT attempts, error FROM jobs WHERE id = 1").get()!;
    assert.equal(row.attempts, 0);
    assert.equal(row.error, "insufficient_quota");
    assert.deepEqual(await processQueue(store, {
      async run() { return { status: "completed", result: "ok" }; },
    }, signal), { pending: 0, running: 0, completed: 2, failed: 0 });
  } finally { db.close(); }
});

for (const maxAttempts of [1, 2]) {
  test(`expired work ${maxAttempts === 1 ? "fails at the attempt limit" : "retries below the attempt limit"}`, async () => {
    const { db, store, signal } = fixture(1, maxAttempts);
    try {
      store.addJob(["interrupted"]);
      store.claimNextJob(maxAttempts, -1);
      const executed: number[] = [];
      const summary = await processQueue(store, {
        async run(job) {
          executed.push(job.id);
          return { status: "completed", result: "recovered" };
        },
      }, signal);
      const exhausted = maxAttempts === 1;
      assert.deepEqual(executed, exhausted ? [] : [1]);
      assert.deepEqual(summary, { pending: 0, running: 0, completed: exhausted ? 0 : 1, failed: exhausted ? 1 : 0 });
      assert.deepEqual(store.getJobsForExport().map(row => ({ ...row })), [{
        input: "interrupted", status: exhausted ? "failed" : "completed", result: exhausted ? null : "recovered",
      }]);
      assert.equal(db.prepare("SELECT attempts FROM jobs").get()!.attempts, maxAttempts);
    } finally { db.close(); }
  });
}

test("live leases remain untouched while waiting, and cancellation interrupts that wait", async () => {
  const { db, store } = fixture();
  const controller = new AbortController();
  try {
    store.addJob(["owned by another run"]);
    store.claimNextJob(3, 300);
    const before = db.prepare("SELECT * FROM jobs").get();
    let settled = false;
    const run = processQueue(store, {
      run() { assert.fail("do not execute work with an unexpired lease"); },
    }, controller.signal);
    const reason = new Error("stop waiting");
    const rejected = assert.rejects(run, { name: "AbortError", cause: reason });
    void run.finally(() => { settled = true; }).catch(() => {});
    await flush();
    assert.equal(settled, false, "a live lease is unfinished work, not a drained queue");
    controller.abort(reason);
    await rejected;
    assert.deepEqual(db.prepare("SELECT * FROM jobs").get(), before,
      "waiting and cancellation must not steal or alter another run's lease");
  } finally { db.close(); }
});

test("long jobs renew their leases and stop renewing after completion", async context => {
  context.mock.timers.enable({ apis: ["setInterval"] });
  const { db, store, signal } = fixture();
  try {
    store.addJob(["long job"]);
    const renew = context.mock.method(store, "renewLease");
    let finish!: () => void;
    const run = processQueue(store, {
      async run() {
        await new Promise<void>(resolve => { finish = resolve; });
        return { status: "completed", result: "ok" };
      },
    }, signal);
    // Simulate a lease nearing expiry without waiting five minutes.
    db.exec("UPDATE jobs SET lease_until = 0");
    context.mock.timers.tick(60_000);
    assert.ok(Number(db.prepare("SELECT lease_until FROM jobs").get()!.lease_until) > Date.now() / 1000,
      "the heartbeat must extend the persisted lease");
    context.mock.timers.tick(60_000);
    assert.equal(renew.mock.callCount(), 2);
    finish();
    await run;
    context.mock.timers.tick(60_000);
    assert.equal(renew.mock.callCount(), 2);
  } finally { db.close(); }
});

test("lease renewal failure aborts the run visibly", async context => {
  context.mock.timers.enable({ apis: ["setInterval"] });
  const { db, store, signal } = fixture();
  try {
    store.addJob(["long job"]);
    store.renewLease = () => false;
    const run = processQueue(store, {
      async run(_job, signal) {
        await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
        return { status: "failed", error: "aborted", kind: "job" };
      },
    }, signal);
    context.mock.timers.tick(60_000);
    await assert.rejects(run, /Lost lease for job 1/);
    assert.deepEqual(store.getProgress(), { pending: 0, running: 1, completed: 0, failed: 0 },
      "lease loss must not persist the cancelled worker's outcome");
  } finally { db.close(); }
});

test("empty queues finish without spawning workers", async () => {
  const { db, store, signal } = fixture();
  try {
    assert.deepEqual(await processQueue(store, { run() { assert.fail("must not run"); } }, signal), {
      pending: 0, running: 0, completed: 0, failed: 0,
    });
  } finally { db.close(); }
});

test("storage errors abort sibling workers and reject only after workers settle", async () => {
  const { db, store, signal } = fixture();
  try {
    store.addJob(["one", "two", "three"]);
    const siblingAborted = deferred();
    const releaseSibling = deferred();
    const finishFirst = deferred();
    const executed: number[] = [];
    let settled = false;
    const failure = new Error("database unavailable");
    store.completeJob = () => { throw failure; };
    const worker: Worker = {
      async run(job, signal) {
        executed.push(job.id);
        if (job.id === 1) {
          await finishFirst.promise;
          return { status: "completed", result: "ok" };
        }
        signal.addEventListener("abort", siblingAborted.resolve, { once: true });
        await releaseSibling.promise;
        return { status: "completed", result: "late result" };
      },
    };
    const run = processQueue(store, worker, signal);
    const rejected = assert.rejects(run, error => error === failure);
    void run.catch(() => { settled = true; });
    finishFirst.resolve();
    await siblingAborted.promise;
    await flush();
    assert.equal(settled, false, "keep storage open until the aborted sibling finishes");
    releaseSibling.resolve();
    await rejected;
    assert.deepEqual(executed, [1, 2], "storage failure must prevent further claims");
    assert.deepEqual(store.getProgress(), { pending: 1, running: 2, completed: 0, failed: 0 },
      "discard late results and leave interrupted leases for recovery");
  } finally { db.close(); }
});

test("service rejects duplicate runs and permits running again after completion", async () => {
  const { db, store } = fixture();
  try {
    store.addJob(["one"]);
    let finish!: () => void;
    const queues = createQueueService({
      initializeQueue() {},
      withExistingQueue<T>(_path: string, operation: (store: QueueStore) => T) { return operation(store); },
      withExistingQueueAsync: async (_path, operation) => operation(store),
      createWorker: () => ({
        async run() {
          await new Promise<void>(resolve => { finish = resolve; });
          return { status: "completed", result: "ok" };
        },
      }),
    });
    const run = queues.runQueue("jobs");
    await assert.rejects(queues.runQueue("./jobs.db"), /already running/);
    finish();
    assert.equal((await run).completed, 1);
    store.addJob(["two"]);
    const rerun = queues.runQueue("./jobs.db");
    finish();
    assert.deepEqual(await rerun, { pending: 0, running: 0, completed: 2, failed: 0 });
  } finally { db.close(); }
});

test("session shutdown preserves interrupted work and releases the queue for a later run", async () => {
  const { db, store } = fixture();
  try {
    store.addJob(["one"]);
    const queues = createQueueService({
      initializeQueue() {},
      withExistingQueue: (_path, operation) => operation(store),
      withExistingQueueAsync: async (_path, operation) => operation(store),
      createWorker: () => ({
        async run(_job, signal) {
          await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
          signal.throwIfAborted();
          return { status: "completed", result: "unreachable" };
        },
      }),
    });
    const run = queues.runQueue("jobs");
    queues.shutdown();
    await assert.rejects(run, /session shutdown/);
    // Interrupted work retains its lease for safe recovery on a later run.
    assert.deepEqual(store.getProgress(), { pending: 0, running: 1, completed: 0, failed: 0 });
    // Expire the interrupted lease so the next run can safely recover it.
    db.exec("UPDATE jobs SET lease_until = 0");
    const rerun = queues.runQueue("jobs");
    queues.shutdown();
    await assert.rejects(rerun, /session shutdown/, "shutdown must release the duplicate-run guard");
  } finally { db.close(); }
});

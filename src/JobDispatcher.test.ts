import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { setImmediate } from "node:timers/promises";
import PQueue from "p-queue";
import { JobDispatcher, type JobStore } from "./JobDispatcher.ts";
import type { JobExecution, JobOutcome, Worker } from "./Worker.ts";

const completed: JobOutcome = { status: "completed", result: "answer" };
const jobFailure: JobOutcome = { status: "failed", kind: "job", error: "invalid input" };
const transient: JobOutcome = { status: "failed", kind: "transient", error: "HTTP 503 Service Unavailable" };
const blocked: JobOutcome = { status: "failed", kind: "blocked", error: "insufficient_quota" };
const flush = () => setImmediate();

// Control worker completion independently of virtual time. Use the real pool;
// record store calls without reimplementing persistence or attempt accounting.
function fixture(t: TestContext, { concurrency = 3, count = 12 } = {}) {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  const pending = Array.from({ length: count }, (_, index): JobExecution => ({
    id: index + 1, leaseToken: `lease-${index + 1}`, agent: "test", prompt: `input ${index + 1}`,
  }));
  const runs: {
    job: JobExecution;
    signal: AbortSignal;
    resolve: (outcome: JobOutcome) => void;
    reject: (error: unknown) => void;
  }[] = [];
  const jobs = {
    claimNext: t.mock.fn(() => pending.shift() ?? null),
    complete: t.mock.fn<JobStore["complete"]>(),
    fail: t.mock.fn<JobStore["fail"]>(),
    release: t.mock.fn<JobStore["release"]>(),
    renew: t.mock.fn<JobStore["renew"]>(),
  };
  const worker: Worker = {
    run(job, signal) {
      return new Promise((resolve, reject) => runs.push({ job, signal, resolve, reject }));
    },
  };
  const pool = new PQueue({ concurrency });
  const dispatcher = new JobDispatcher(pool, worker, jobs);
  const controller = new AbortController();
  const done = dispatcher.start(controller.signal);
  // Observe rejection immediately, including when a scenario deliberately aborts.
  void done.catch(() => {});
  t.after(async () => {
    controller.abort(new Error("test cleanup"));
    for (const run of runs) run.resolve(completed);
    await done.catch(() => {});
  });
  return {
    dispatcher, pool, jobs, runs, done, controller,
    ids: () => runs.map(run => run.job.id),
    claims: () => jobs.claimNext.mock.callCount(),
    async finish(index: number, outcome: JobOutcome) {
      assert.ok(runs[index], `worker invocation ${index} must exist`);
      runs[index].resolve(outcome);
      await flush();
    },
    async tick(milliseconds: number) {
      t.mock.timers.tick(milliseconds);
      await flush();
    },
  };
}

function calls<T extends (...args: any[]) => any>(fn: { mock: { calls: { arguments: Parameters<T> }[] } }) {
  return fn.mock.calls.map(call => call.arguments);
}

test("fill available slots, refill as jobs finish, and resolve only after the last job settles", async t => {
  const f = fixture(t, { concurrency: 2, count: 3 });
  let drained = false;
  void f.done.then(() => { drained = true; });
  assert.equal(f.dispatcher.start(), f.done, "concurrent starts share one run");
  assert.deepEqual(f.ids(), [1, 2]);
  assert.equal(f.claims(), 2, "do not claim work that cannot start");

  await f.finish(1, jobFailure);
  assert.deepEqual(f.ids(), [1, 2, 3], "a job failure must not stop unrelated work");
  assert.deepEqual(calls(f.jobs.fail), [[f.runs[1].job, jobFailure.error]]);
  await f.finish(0, completed);
  assert.equal(drained, false, "an empty store does not mean active work has finished");
  await f.finish(2, completed);
  await f.done;
  assert.deepEqual(calls(f.jobs.complete), [0, 2].map(i => [f.runs[i].job, "answer"]));
  assert.deepEqual(calls(f.jobs.release), [], "terminal outcomes must not be retried");
  const claims = f.claims();
  await f.tick(300_000);
  assert.equal(f.claims(), claims, "a drained run must leave no scheduled work");
});

test("an empty store resolves without starting a worker", async t => {
  const f = fixture(t, { count: 0 });
  await f.done;
  assert.deepEqual(f.ids(), []);
  assert.equal(f.dispatcher.blocked, false);
});

test("transient failures release leases and stop claims for 30 seconds without cancelling active work", async t => {
  const f = fixture(t);
  await f.finish(0, transient);
  assert.deepEqual(calls(f.jobs.release), [[f.runs[0].job, transient.error]]);
  assert.deepEqual(calls(f.jobs.fail), []);
  assert.ok(f.runs.every(run => !run.signal.aborted));
  await f.tick(20_000);
  await f.finish(1, transient);
  await f.finish(2, completed);
  assert.deepEqual(calls(f.jobs.complete), [[f.runs[2].job, "answer"]]);

  assert.equal(f.dispatcher.start(), f.done, "start cannot bypass the cooldown");
  await f.tick(9_999);
  assert.equal(f.claims(), 3, "late failures neither restart nor shorten the cooldown");
  await f.tick(1);
  assert.deepEqual(f.ids(), [1, 2, 3, 4], "retry with the store's next job, not the failed job");
  await f.finish(3, transient);
  await f.tick(59_999);
  assert.equal(f.claims(), 4);
  await f.tick(1);
  assert.equal(f.claims(), 5, "only the failed probe doubles the delay");
});

test("failed probes back off exponentially, capped at five minutes", async t => {
  const f = fixture(t, { concurrency: 1 });
  await f.finish(0, transient);
  for (const [index, seconds] of [30, 60, 120, 240, 300, 300].entries()) {
    await f.tick(seconds * 1000 - 1);
    assert.equal(f.claims(), index + 1, `no probe before ${seconds} seconds`);
    await f.tick(1);
    assert.equal(f.claims(), index + 2, `one probe at ${seconds} seconds`);
    await f.finish(index + 1, transient);
    assert.equal(f.claims(), index + 2, "a failed probe starts a new cooldown");
  }
});

for (const [label, outcome] of [["success", completed], ["job failure", jobFailure]] as const) {
  test(`probe ${label} restores full concurrency and resets the next cooldown`, async t => {
    const f = fixture(t);
    await f.finish(0, transient);
    await f.finish(1, completed);
    await f.finish(2, completed);
    await f.tick(30_000);
    await f.finish(3, transient); // Raise the delay before proving recovery resets it.
    await f.tick(60_000);
    assert.deepEqual(f.ids(), [1, 2, 3, 4, 5]);
    await f.tick(300_000);
    assert.equal(f.claims(), 5, "only one unresolved probe may run, regardless of elapsed time");

    await f.finish(4, outcome);
    assert.deepEqual(f.ids(), [1, 2, 3, 4, 5, 6, 7, 8]);
    const writes = outcome.status === "completed" ? calls(f.jobs.complete) : calls(f.jobs.fail);
    assert.deepEqual(writes.at(-1), [f.runs[4].job, outcome.status === "completed" ? outcome.result : outcome.error]);
    await f.finish(5, transient);
    await f.tick(29_999);
    assert.equal(f.claims(), 8);
    await f.tick(1);
    assert.equal(f.claims(), 9, "recovery resets the delay to 30 seconds");
  });
}

for (const [label, outcome] of [["success", completed], ["job failure", jobFailure], ["transient failure", transient]] as const) {
  test(`an older job's ${label} cannot decide whether an outstanding probe recovers`, async t => {
    const f = fixture(t);
    await f.finish(0, transient);
    await f.tick(30_000);
    assert.deepEqual(f.ids(), [1, 2, 3, 4]);
    await f.finish(1, outcome);
    await f.finish(2, completed);
    await f.tick(300_000);
    assert.equal(f.claims(), 4, "older results cannot close the circuit or schedule another probe");
    await f.finish(3, completed);
    assert.deepEqual(f.ids(), [1, 2, 3, 4, 5, 6, 7], "the designated probe alone restores dispatch");
  });
}

test("a ready probe waits for pool capacity rather than claiming a job early", async t => {
  const f = fixture(t);
  f.pool.concurrency = 1;
  await f.finish(0, transient);
  await f.tick(30_000);
  assert.equal(f.claims(), 3);
  await f.finish(1, completed);
  assert.equal(f.claims(), 3, "the remaining active job occupies the only slot");
  await f.finish(2, completed);
  assert.deepEqual(f.ids(), [1, 2, 3, 4]);
});

for (const [label, outcome] of [["success", completed], ["job failure", jobFailure], ["transient failure", transient]] as const) {
  test(`blocked overrides an outstanding probe, even when that probe later reports ${label}`, async t => {
    const f = fixture(t);
    await f.finish(0, transient);
    await f.tick(30_000);
    await f.finish(1, blocked);
    assert.equal(f.dispatcher.blocked, true);
    assert.ok(f.runs.every(run => !run.signal.aborted), "blocked drains rather than cancels active work");
    let drained = false;
    void f.done.then(() => { drained = true; });
    await f.finish(3, outcome);
    assert.equal(drained, false, "blocked must wait for every active job");
    await f.finish(2, completed);
    await f.done;
    assert.deepEqual(calls(f.jobs.release).slice(0, 2), [
      [f.runs[0].job, transient.error], [f.runs[1].job, blocked.error],
    ]);
    assert.ok(calls(f.jobs.complete).some(([job, result]) => job === f.runs[2].job && result === "answer"));
    await f.dispatcher.start();
    await f.tick(86_400_000);
    assert.equal(f.dispatcher.blocked, true);
    assert.equal(f.claims(), 4, "neither old results, elapsed time, nor start can clear blocked");
  });
}

test("blocked cancels a pending cooldown and cannot be downgraded by a late transient failure", async t => {
  const f = fixture(t);
  await f.finish(0, transient);
  await f.tick(10_000);
  await f.finish(1, blocked);
  await f.finish(2, transient);
  await f.done;
  assert.equal(f.dispatcher.blocked, true);
  await f.dispatcher.start();
  await f.tick(20_000);
  assert.equal(f.claims(), 3, "the original cooldown must not admit a probe");
  await f.tick(300_000);
  assert.equal(f.claims(), 3, "a late transient must not replace blocked with another cooldown");
});

test("a blocked probe stops permanently instead of scheduling another retry", async t => {
  const f = fixture(t, { concurrency: 1 });
  await f.finish(0, transient);
  await f.tick(30_000);
  await f.finish(1, blocked);
  await f.done;
  assert.equal(f.dispatcher.blocked, true);
  assert.deepEqual(calls(f.jobs.release), [
    [f.runs[0].job, transient.error], [f.runs[1].job, blocked.error],
  ]);
  await f.tick(300_000);
  assert.equal(f.claims(), 2);
});

for (const [label, error, outcome] of [
  ["job", new Error("invalid input"), jobFailure],
  ["transient", new Error("HTTP 503 Service Unavailable"), transient],
  ["blocked", Object.assign(new Error("provider refused"), { code: "insufficient_quota" }), { ...blocked, error: "provider refused" }],
] as const) {
  test(`a rejected worker promise follows the ${label} outcome policy`, async t => {
    const f = fixture(t, { concurrency: 1 });
    f.runs[0].reject(error);
    await flush();
    const writes = label === "job" ? calls(f.jobs.fail) : calls(f.jobs.release);
    assert.deepEqual(writes, [[f.runs[0].job, outcome.error]]);
    assert.equal(f.claims(), label === "job" ? 2 : 1);
    await f.tick(30_000);
    assert.equal(f.claims(), label === "blocked" ? 1 : 2);
    assert.equal(f.dispatcher.blocked, label === "blocked");
  });
}

test("renew active leases every minute and stop renewing each settled job", async t => {
  const f = fixture(t, { concurrency: 2, count: 2 });
  await f.tick(59_999);
  assert.deepEqual(calls(f.jobs.renew), []);
  await f.tick(1);
  assert.deepEqual(calls(f.jobs.renew), f.runs.map(run => [run.job]));
  await f.finish(0, completed);
  await f.tick(60_000);
  assert.deepEqual(calls(f.jobs.renew).slice(2), [[f.runs[1].job]]);
  await f.finish(1, completed);
  await f.done;
  await f.tick(60_000);
  assert.equal(f.jobs.renew.mock.callCount(), 3, "a drained run must not renew stale leases");
});

test("cancellation during cooldown rejects immediately and prevents the scheduled probe", async t => {
  const f = fixture(t, { concurrency: 1 });
  await f.finish(0, transient);
  const error = new Error("shutdown during outage");
  f.controller.abort(error);
  await assert.rejects(f.done, reason => reason === error);
  await f.tick(300_000);
  assert.equal(f.claims(), 1, "cancelled retries must not claim another lease");
});

for (const source of ["caller", "renew", "complete", "claimNext"] as const) {
  test(`${source} failure aborts active workers, drains them, and rejects without writing late results`, async t => {
    const f = fixture(t, { concurrency: 2 });
    const error = new Error(`${source} interrupted dispatch`);
    let settled = false;
    void f.done.catch(() => { settled = true; });
    if (source === "caller") f.controller.abort(error);
    else if (source === "renew") {
      f.jobs.renew.mock.mockImplementation(() => { throw error; });
      await f.tick(60_000);
    } else {
      f.jobs[source].mock.mockImplementation(() => { throw error; });
      await f.finish(0, completed);
    }
    assert.ok(f.runs.every(run => run.signal.aborted));
    assert.equal(settled, false, "do not return while workers can still access resources");
    const writes = f.jobs.complete.mock.callCount();
    for (const run of f.runs) run.resolve(completed);
    await assert.rejects(f.done, reason => reason === error);
    assert.equal(f.jobs.complete.mock.callCount(), writes, "discard results after cancellation");
    assert.deepEqual(calls(f.jobs.fail), []);
    assert.deepEqual(calls(f.jobs.release), [], "leave interrupted leases for recovery");
    const claims = f.claims();
    await f.tick(300_000);
    assert.equal(f.claims(), claims, "cancellation must prevent further claims");
  });
}

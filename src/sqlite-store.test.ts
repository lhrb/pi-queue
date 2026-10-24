import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  initializeQueue,
  SqliteStore,
  withExistingQueueAsync,
} from "./sqlite-store.ts";
import type { ClaimedJob, JobLease, QueueStore } from "./types.ts";

const settings = {
  prompt: "prompt",
  agent: "agent",
  poolSize: 1,
  maxAttempts: 3,
};
const lease = (job: ClaimedJob): JobLease => ({
  id: job.id,
  leaseToken: job.lease_token,
});

function fixture(t: TestContext) {
  const db = new DatabaseSync(":memory:");
  t.after(() => db.close());
  const store = new SqliteStore(db);
  store.init(settings);
  const row = (id: number) => ({
    ...db.prepare("SELECT * FROM jobs WHERE id = ?").get(id)!,
  });
  const claim = (maxAttempts = settings.maxAttempts, leaseSeconds = 300) => {
    const job = store.claimNextJob(maxAttempts, leaseSeconds);
    assert.ok(job, "expected an eligible pending job");
    return job;
  };
  return { db, store, row, claim };
}

function queuePath(t: TestContext) {
  const directory = mkdtempSync(join(tmpdir(), "queue-store-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return join(directory, "jobs.db");
}

test("settings updates preserve omitted values and accept empty text", (t) => {
  const { store } = fixture(t);
  assert.deepEqual({ ...store.getSettings() }, settings);
  store.updateSettings({ poolSize: 4, maxAttempts: 5 });
  assert.deepEqual({ ...store.getSettings() }, { ...settings, poolSize: 4, maxAttempts: 5 });
  store.updateSettings({ prompt: "", agent: "replacement" });
  store.updateSettings({});
  assert.deepEqual({ ...store.getSettings() }, {
    prompt: "", agent: "replacement", poolSize: 4, maxAttempts: 5,
  });
});

test("claims select eligible jobs in insertion order and count each execution once", (t) => {
  const { db, store, claim, row } = fixture(t);
  assert.equal(store.claimNextJob(3, 300), null);
  const [exhausted, first, second] = store.addJob(["exhausted", "first", "second"]);
  db.prepare("UPDATE jobs SET attempts = 3 WHERE id = ?").run(exhausted);

  const job = claim();
  assert.equal(job.id, first, "skip pending jobs at the attempt limit");
  assert.equal(job.input, "first");
  assert.equal(job.status, "running");
  assert.equal(job.attempts, 1);
  assert.equal(typeof job.lease_token, "string");
  assert.ok(job.lease_token.length > 0);
  const now = Number(db.prepare("SELECT unixepoch() AS now").get()!.now);
  assert.ok(job.lease_until > now, "a claim grants time to execute");
  assert.ok(job.lease_until <= now + 300);
  assert.equal(job.result, null);
  assert.equal(job.error, null);
  const next = claim();
  assert.equal(next.id, second, "a running job cannot be claimed again");
  assert.notEqual(next.lease_token, job.lease_token);
  assert.equal(store.claimNextJob(3, 300), null);

  assert.equal(store.completeJob(lease(job), "saved result"), true);
  assert.equal(row(first).attempts, 1, "completion must not count the claim twice");
  assert.equal(row(first).status, "completed");
  assert.equal(row(first).result, "saved result");
  assert.equal(row(first).error, null);
  assert.equal(row(first).lease_token, null);
  assert.equal(row(first).lease_until, null);
});

test("shared failures refund only the current attempt so outages never exhaust retries", (t) => {
  const { db, store, row, claim } = fixture(t);
  const [id] = store.addJob(["input"]);
  // Preserve an earlier crashed execution while repeatedly refunding outage attempts.
  db.prepare("UPDATE jobs SET attempts = 1 WHERE id = ?").run(id);
  for (let outage = 0; outage < settings.maxAttempts + 1; outage++) {
    const job = claim();
    assert.equal(job.attempts, 2);
    assert.equal(job.error, null, "reclaim clears the previous outage diagnostic");
    assert.equal(store.releaseJob(lease(job), "HTTP 503 Service Unavailable"), true);
    const released = row(id);
    assert.equal(released.status, "pending");
    assert.equal(released.attempts, 1, "refund must preserve earlier consumed attempts");
    assert.equal(released.error, "HTTP 503 Service Unavailable");
    assert.equal(released.lease_token, null);
    assert.equal(released.lease_until, null);
    assert.equal(store.reset(), 0, "outages retry without manual reset");
  }
  assert.equal(store.completeJob(lease(claim()), "recovered"), true);
  assert.equal(row(id).attempts, 2);
  assert.equal(row(id).error, null);
});

test("job failures stay terminal until reset; reset leaves other jobs untouched", (t) => {
  const { store, row, claim } = fixture(t);
  const [failed, completed, running, pending] = store.addJob([
    "invalid input", "complete", "active", "waiting",
  ]);
  const first = claim();
  assert.equal(store.failJob(lease(first), "invalid input"), true);
  assert.equal(row(failed).status, "failed");
  assert.equal(row(failed).attempts, 1, "failure must not count the claim twice");
  assert.equal(row(failed).error, "invalid input");
  assert.equal(row(failed).lease_token, null);
  assert.equal(row(failed).lease_until, null);
  const successful = claim();
  assert.equal(successful.id, completed, "failed jobs do not retry automatically");
  assert.equal(store.completeJob(lease(successful), "saved"), true);
  assert.equal(claim().id, running);
  const untouched = [completed, running, pending].map(row);

  assert.equal(store.reset(), 1);
  const reset = row(failed);
  assert.equal(reset.status, "pending");
  assert.equal(reset.attempts, 0);
  assert.equal(reset.error, null);
  assert.equal(reset.result, null);
  assert.equal(reset.lease_token, null);
  assert.equal(reset.lease_until, null);
  assert.deepEqual([completed, running, pending].map(row), untouched);
  assert.equal(store.reset(), 0, "reset is idempotent");
  const retried = claim();
  assert.equal(retried.id, failed);
  assert.equal(retried.attempts, 1);
  assert.notEqual(retried.lease_token, first.lease_token);
});

for (const operation of ["completeJob", "failJob", "releaseJob", "renewLease"] as const) {
  test(`${operation} rejects nonowners and inactive leases without changing any job`, (t) => {
    const { store, row, claim } = fixture(t);
    const [id, other] = store.addJob(["input", "other"]);
    const apply = (owner: JobLease) => operation === "renewLease"
      ? store.renewLease(owner, 120)
      : store[operation](owner, "late worker output");
    const reject = (owner: JobLease, reason: string) => {
      const before = [row(id), row(other)];
      assert.equal(apply(owner), false, reason);
      assert.deepEqual([row(id), row(other)], before, reason);
    };

    const first = claim();
    reject({ id, leaseToken: "wrong" }, "token must match");
    reject({ id: other, leaseToken: first.lease_token }, "job ID must match");
    reject({ id: 999, leaseToken: first.lease_token }, "missing jobs are rejected");
    assert.equal(store.releaseJob(lease(first), "outage"), true);
    reject(lease(first), "pending jobs have no owner");
    const second = claim();
    reject(lease(first), "reclaim invalidates the old worker");
    assert.equal(store.failJob(lease(second), "invalid"), true);
    reject(lease(second), "failed jobs reject late writes");
    assert.equal(store.reset(), 1);
    const third = claim();
    reject(lease(second), "reset does not restore an old lease");
    assert.equal(store.completeJob(lease(third), "saved"), true);
    reject(lease(third), "completed jobs reject duplicate writes");
  });
}

test("renewal extends the current lease without consuming another attempt", (t) => {
  const { store, row, claim } = fixture(t);
  store.addJob(["input"]);
  const job = claim(3, 1);
  assert.equal(store.renewLease(lease(job), 300), true);
  const renewed = row(job.id);
  assert.ok(Number(renewed.lease_until) > job.lease_until);
  assert.deepEqual(
    { ...renewed, lease_until: job.lease_until, updated_at: job.updated_at },
    { ...job },
    "renewal changes only the lease deadline and modification time",
  );
});

test("recovery retries expired work below the limit and fails exhausted work, leaving other jobs unchanged", (t) => {
  const { db, store, claim, row } = fixture(t);
  const [retry, exhausted, active, pendingExhausted, pending, completed, failed] = store.addJob([
    "retry", "exhausted", "active", "pending exhausted", "pending", "completed", "failed",
  ]);
  const expired = claim();
  claim();
  claim();
  // Set deadlines in SQLite rather than sleeping or mocking the JavaScript clock.
  db.exec(`UPDATE jobs SET lease_until = unixepoch() WHERE id IN (${retry}, ${exhausted});
    UPDATE jobs SET attempts = 3 WHERE id IN (${exhausted}, ${pendingExhausted});
    UPDATE jobs SET status = 'completed', result = 'saved' WHERE id = ${completed};
    UPDATE jobs SET status = 'failed', error = 'saved' WHERE id = ${failed};`);
  const untouched = [active, pending, completed, failed].map(row);

  store.recoverExpiredJobs(3);
  for (const [id, status, attempts, diagnostic] of [
    [retry, "pending", 1, /lease expired/i],
    [exhausted, "failed", 3, /lease expired/i],
    [pendingExhausted, "failed", 3, /attempts exhausted/i],
  ] as const) {
    const recovered = row(id);
    assert.equal(recovered.status, status, `recovered status for job ${id}`);
    assert.equal(recovered.attempts, attempts, `preserve consumed attempts for job ${id}`);
    assert.equal(recovered.lease_token, null);
    assert.equal(recovered.lease_until, null);
    assert.match(String(recovered.error), diagnostic);
  }
  assert.deepEqual([active, pending, completed, failed].map(row), untouched);
  assert.equal(store.completeJob(lease(expired), "stale"), false);
  const recovered = db.prepare("SELECT * FROM jobs ORDER BY id").all();
  store.recoverExpiredJobs(3);
  assert.deepEqual(db.prepare("SELECT * FROM jobs ORDER BY id").all(), recovered, "recovery is idempotent");
  const retried = claim();
  assert.equal(retried.id, retry);
  assert.equal(retried.attempts, 2);
  assert.equal(retried.error, null);
  assert.notEqual(retried.lease_token, expired.lease_token);
});

test("export returns only input, status, and result in insertion order", (t) => {
  const { store, claim } = fixture(t);
  assert.deepEqual(store.getJobsForExport(), []);
  store.addJob(["complete", "fail", "running", "pending"]);
  assert.equal(store.completeJob(lease(claim()), "saved result"), true);
  assert.equal(store.failJob(lease(claim()), "error"), true);
  claim();
  assert.deepEqual(store.getJobsForExport().map((job) => ({ ...job })), [
    { input: "complete", status: "completed", result: "saved result" },
    { input: "fail", status: "failed", result: null },
    { input: "running", status: "running", result: null },
    { input: "pending", status: "pending", result: null },
  ]);
});

test("progress counts every status and includes statuses with no jobs", (t) => {
  const { store, claim } = fixture(t);
  assert.deepEqual(store.getProgress(), { pending: 0, running: 0, completed: 0, failed: 0 });
  store.addJob(["complete", "fail", "running", "pending", "another pending"]);
  assert.deepEqual(store.getProgress(), { pending: 5, running: 0, completed: 0, failed: 0 });
  assert.equal(store.completeJob(lease(claim()), "result"), true);
  assert.equal(store.failJob(lease(claim()), "error"), true);
  claim();
  assert.deepEqual(store.getProgress(), { pending: 2, running: 1, completed: 1, failed: 1 });
});

test("async helper rejects missing or uninitialized queues without invoking the operation", async (t) => {
  const path = queuePath(t);
  let called = false;
  const operation = async () => { called = true; };
  await assert.rejects(withExistingQueueAsync(path, operation));
  assert.equal(called, false);
  assert.equal(existsSync(path), false, "opening must not create a missing queue");
  const invalid = new DatabaseSync(path);
  invalid.close();
  await assert.rejects(withExistingQueueAsync(path, operation));
  assert.equal(called, false);
});

test("async helper stays open across awaits, returns the result, persists writes, then closes", async (t) => {
  const path = queuePath(t);
  initializeQueue(path, settings);
  let captured: QueueStore | undefined;
  const result = await withExistingQueueAsync(path, async (store) => {
    captured = store;
    await new Promise<void>((resolve) => setImmediate(resolve));
    return store.addJob(["input"]);
  });
  assert.deepEqual(result, [1]);
  assert.throws(() => captured!.getSettings(), /closed|not open/i);
  assert.deepEqual(await withExistingQueueAsync(path, async (store) => store.getProgress()), {
    pending: 1, running: 0, completed: 0, failed: 0,
  });
});

for (const timing of ["synchronous throw", "asynchronous rejection"] as const) {
  test(`async helper closes the database and preserves the error after ${timing}`, async (t) => {
    const path = queuePath(t);
    initializeQueue(path, settings);
    let captured: QueueStore | undefined;
    const failure = new Error("operation failed");
    await assert.rejects(withExistingQueueAsync(path, (store) => {
      captured = store;
      if (timing === "synchronous throw") throw failure;
      return Promise.resolve().then(() => { throw failure; });
    }), (error) => error === failure);
    assert.throws(() => captured!.getSettings(), /closed|not open/i);
  });
}

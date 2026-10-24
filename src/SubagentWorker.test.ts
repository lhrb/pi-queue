import assert from "node:assert/strict";
import { mock, test } from "node:test";
import { SUBAGENT_EVENTS, type SubagentsService } from "@gotgenes/pi-subagents";
import { SubagentWorker, type EventBus } from "./SubagentWorker.ts";
import type { FailureKind, JobExecution } from "./Worker.ts";

const execution: JobExecution = {
  id: 1, leaseToken: "lease", agent: "worker", prompt: "input",
};

function fixture() {
  const listeners = new Map<string, Set<(data: unknown) => void>>();
  const events: EventBus = {
    on(name, listener) {
      if (!listeners.has(name)) listeners.set(name, new Set());
      listeners.get(name)!.add(listener);
      return () => { listeners.get(name)!.delete(listener); };
    },
    emit(name, data) {
      for (const listener of listeners.get(name) ?? []) listener(data);
    },
  };
  let nextId = 1;
  const spawn = mock.fn<SubagentsService["spawn"]>(() => `agent-${nextId++}`);
  const abort = mock.fn<SubagentsService["abort"]>(() => true);
  const worker = new SubagentWorker(events, { spawn, abort } as unknown as SubagentsService);
  return {
    events, worker, spawn, abort,
    subscriptions: () => [...listeners.values()].reduce((count, set) => count + set.size, 0),
  };
}

test("runs the requested agent and prompt in an isolated background context", async () => {
  const harness = fixture();
  const controller = new AbortController();
  const run = harness.worker.run(execution, controller.signal);

  assert.deepEqual(harness.spawn.mock.calls.map(call => call.arguments), [
    ["worker", "input", { inheritContext: false, foreground: false }],
  ]);
  harness.events.emit(SUBAGENT_EVENTS.COMPLETED, { id: "agent-1", result: "answer" });

  assert.deepEqual(await run, { status: "completed", result: "answer" });
  assert.equal(harness.subscriptions(), 0, "completion releases event subscriptions");
  controller.abort();
  assert.equal(harness.abort.mock.callCount(), 0, "later cancellation must not abort a completed child");
});

test("concurrent runs only settle from their own child's events", async () => {
  const harness = fixture();
  const first = harness.worker.run(execution, new AbortController().signal);
  const second = harness.worker.run({ ...execution, id: 2 }, new AbortController().signal);

  harness.events.emit(SUBAGENT_EVENTS.FAILED, { id: "unrelated", error: "ignore" });
  harness.events.emit(SUBAGENT_EVENTS.COMPLETED, { id: "unrelated", result: "ignore" });
  harness.events.emit(SUBAGENT_EVENTS.FAILED, { id: "agent-2", error: "Invalid job input" });
  assert.deepEqual(await second, { status: "failed", error: "Invalid job input", kind: "job" });
  assert.equal(harness.subscriptions(), 2, "the first run must remain subscribed");

  harness.events.emit(SUBAGENT_EVENTS.COMPLETED, { id: "agent-1", result: "" });
  assert.deepEqual(await first, { status: "completed", result: "" }, "an empty result is valid");
  assert.equal(harness.subscriptions(), 0);
});

test("ignores malformed events until a valid outcome arrives", async t => {
  t.mock.method(console, "warn", () => {});
  const harness = fixture();
  const run = harness.worker.run(execution, new AbortController().signal);

  for (const payload of [null, "invalid", {}, { id: 1, result: "wrong ID type" },
    { id: "agent-1" }, { id: "agent-1", result: 42 }]) {
    harness.events.emit(SUBAGENT_EVENTS.COMPLETED, payload);
  }
  for (const payload of [null, "invalid", {}, { id: 1, error: "wrong ID type" },
    { id: "agent-1", error: 42 }]) {
    harness.events.emit(SUBAGENT_EVENTS.FAILED, payload);
  }

  harness.events.emit(SUBAGENT_EVENTS.COMPLETED, { id: "agent-1", result: "valid" });
  assert.deepEqual(await run, { status: "completed", result: "valid" });
  assert.equal(harness.subscriptions(), 0);
});

// Classification controls whether the queue retries, blocks, or fails only this job.
const failures: { error: string; kind: FailureKind }[] = [
  { error: "insufficient_quota: You exceeded your current quota", kind: "blocked" },
  { error: "HTTP 429: insufficient_quota", kind: "blocked" },
  { error: "HTTP 401: Invalid API key", kind: "blocked" },
  { error: "HTTP 403: Permission denied", kind: "blocked" },
  { error: "Your account has insufficient credits", kind: "blocked" },
  { error: "Invalid credentials", kind: "blocked" },
  { error: "HTTP 429: Rate limit exceeded", kind: "transient" },
  { error: "HTTP 503: Service Unavailable", kind: "transient" },
  { error: "ETIMEDOUT: Connection timed out", kind: "transient" },
  { error: "Connection timeout", kind: "transient" },
  { error: "context_length_exceeded: Maximum context length exceeded", kind: "job" },
  { error: "Invalid job input", kind: "job" },
  { error: "Invalid token count in job input", kind: "job" },
  { error: "Token budget exceeded for this request", kind: "job" },
  { error: "Unexpected failure: something unfamiliar happened", kind: "job" },
];

for (const { error, kind } of failures) {
  test(`child failure ${JSON.stringify(error)} produces a ${kind} outcome`, async () => {
    const harness = fixture();
    const controller = new AbortController();
    const run = harness.worker.run(execution, controller.signal);
    harness.events.emit(SUBAGENT_EVENTS.FAILED, { id: "agent-1", error });

    assert.deepEqual(await run, { status: "failed", error, kind });
    assert.equal(harness.subscriptions(), 0);
    controller.abort();
    assert.equal(harness.abort.mock.callCount(), 0, "a failed child is already finished");
  });
}

test("a stopped child without an error produces a diagnostic job failure", async () => {
  const harness = fixture();
  const run = harness.worker.run(execution, new AbortController().signal);
  harness.events.emit(SUBAGENT_EVENTS.FAILED, { id: "agent-1", status: "stopped" });

  assert.deepEqual(await run, {
    status: "failed", error: "Subagent failed without an error message.", kind: "job",
  });
  assert.equal(harness.subscriptions(), 0);
});

// Exercise the spawn boundary without repeating the entire message-classification matrix.
for (const { label, thrown, error, kind } of [
  { label: "Error message", thrown: new Error("HTTP 503: Service Unavailable"),
    error: "HTTP 503: Service Unavailable", kind: "transient" },
  { label: "provider code", thrown: Object.assign(new Error("provider refused"), { code: "insufficient_quota" }),
    error: "provider refused", kind: "blocked" },
  { label: "non-Error value", thrown: "unexpected spawn failure",
    error: "unexpected spawn failure", kind: "job" },
]) {
  test(`spawn exceptions become failure outcomes: ${label}`, async () => {
    const harness = fixture();
    const controller = new AbortController();
    harness.spawn.mock.mockImplementation(() => { throw thrown; });

    assert.deepEqual(await harness.worker.run(execution, controller.signal), {
      status: "failed", error, kind,
    });
    assert.equal(harness.subscriptions(), 0);
    controller.abort();
    assert.equal(harness.abort.mock.callCount(), 0, "a spawn failure has no child to abort");
  });
}

test("an already-cancelled run rejects with the original reason without spawning or subscribing", async t => {
  const harness = fixture();
  const subscribe = t.mock.method(harness.events, "on");
  const controller = new AbortController();
  const reason = { message: "queue stopped" };
  controller.abort(reason);

  await assert.rejects(harness.worker.run(execution, controller.signal), error => error === reason);
  assert.equal(harness.spawn.mock.callCount(), 0);
  assert.equal(subscribe.mock.callCount(), 0);
  assert.equal(harness.abort.mock.callCount(), 0);
  assert.equal(harness.subscriptions(), 0);
});

for (const abortBehavior of ["emits a failure", "throws"] as const) {
  test(`cancellation keeps its original reason when child abort ${abortBehavior}`, async t => {
    t.mock.method(console, "error", () => {});
    const harness = fixture();
    const controller = new AbortController();
    const reason = new Error("interrupted");
    let subscriptionsAtAbort: number | undefined;
    harness.abort.mock.mockImplementation(id => {
      subscriptionsAtAbort = harness.subscriptions();
      if (abortBehavior === "throws") throw new Error("abort failed");
      harness.events.emit(SUBAGENT_EVENTS.FAILED, { id, error: "child stopped" });
      return true;
    });
    const run = harness.worker.run(execution, controller.signal);

    controller.abort(reason);

    await assert.rejects(run, error => error === reason);
    assert.deepEqual(harness.abort.mock.calls.map(call => call.arguments), [["agent-1"]]);
    assert.equal(subscriptionsAtAbort, 0, "unsubscribe before abort can emit a terminal event");
    assert.equal(harness.subscriptions(), 0);
  });
}

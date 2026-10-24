import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createQueueService } from "./queue.ts";
import { SqliteStore, initializeQueue, withExistingQueue } from "./sqlite-store.ts";
import queueExtension from "./index.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { QueueStore } from "./types.ts";

test("service construction has no effects and operations use injected dependencies", () => {
  const calls: unknown[] = [];
  const store = {
    addJob(inputs: readonly string[]) { calls.push(inputs); return [42, 43]; },
    updateSettings(options: unknown) { calls.push(options); },
  } as unknown as QueueStore;
  const queues = createQueueService({
    initializeQueue(path, settings) { calls.push([path, settings]); },
    withExistingQueue(path, operation) {
      calls.push(path);
      return operation(store);
    },
  });
  assert.deepEqual(calls, []);
  assert.deepEqual(queues.addJob("jobs", [" literal input ", "second"]), [42, 43]);
  queues.updateSettings("jobs", { poolSize: 2 });
  queues.initQueue("new", "prompt", "agent");
  assert.deepEqual(calls, [
    "jobs.db", [" literal input ", "second"],
    "jobs.db", { poolSize: 2 },
    ["new.db", { prompt: "prompt", agent: "agent", poolSize: 4, maxAttempts: 3 }],
  ]);
});

test("export writes JSONL, handles empty queues, and preserves existing files", t => {
  const directory = mkdtempSync(join(tmpdir(), "queue-export-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const path = join(directory, "jobs.db");
  const output = join(directory, "results.jsonl");
  const queues = createQueueService({ initializeQueue, withExistingQueue });
  queues.initQueue(path, "prompt", "agent");
  const empty = join(directory, "empty.jsonl");
  assert.equal(queues.exportQueue(path, empty), 0);
  assert.equal(readFileSync(empty, "utf8"), "");

  const input = 'literal "quotes"\nUnicode: ä';
  queues.addJob(path, [input, "duplicate", "duplicate"]);
  withExistingQueue(path, store => {
    const job = store.claimNextJob(3, 60)!;
    store.completeJob({ id: job.id, leaseToken: job.lease_token }, "result\nnext line");
  });
  assert.equal(queues.exportQueue(path.slice(0, -3), output), 3);
  const content = readFileSync(output, "utf8");
  assert.ok(content.endsWith("\n"));
  assert.deepEqual(content.trimEnd().split("\n").map(line => JSON.parse(line)), [
    { input, status: "completed", result: "result\nnext line" },
    { input: "duplicate", status: "pending", result: null },
    { input: "duplicate", status: "pending", result: null },
  ]);
  assert.throws(() => queues.exportQueue(path, output), /EEXIST/);
  assert.equal(readFileSync(output, "utf8"), content);
  const database = readFileSync(path);
  assert.throws(() => queues.exportQueue(path, path), /EEXIST/);
  assert.deepEqual(readFileSync(path), database);
  assert.throws(() => queues.exportQueue(path, join(directory, "missing", "output.jsonl")));
  const missingOutput = join(directory, "missing.jsonl");
  assert.throws(() => queues.exportQueue(join(directory, "missing.db"), missingOutput));
  assert.equal(existsSync(missingOutput), false);
});

test("existing queue helper rejects missing queues and closes after success or failure", () => {
  const directory = mkdtempSync(join(tmpdir(), "queue-test-"));
  const path = join(directory, "jobs.db");
  try {
    assert.throws(() => withExistingQueue(path, () => assert.fail("must not run")));
    assert.equal(existsSync(path), false);
    initializeQueue(path, { prompt: "prompt", agent: "agent", poolSize: 1, maxAttempts: 3 });

    let captured: QueueStore | undefined;
    assert.deepEqual(withExistingQueue(path, store => {
      captured = store;
      return store.addJob(["input"]);
    }), [1]);
    assert.throws(() => captured!.getSettings());

    const failure = new Error("operation failed");
    assert.throws(() => withExistingQueue(path, store => {
      captured = store;
      throw failure;
    }), error => error === failure);
    assert.throws(() => captured!.getSettings());
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("initialization rejects existing files without changing their contents", () => {
  const directory = mkdtempSync(join(tmpdir(), "queue-test-"));
  const path = join(directory, "jobs.db");
  const settings = { prompt: "prompt", agent: "agent", poolSize: 1, maxAttempts: 3 };
  try {
    writeFileSync(path, "existing content");
    assert.throws(() => initializeQueue(path, settings), {
      message: `File already exists: ${path}`,
    });
    assert.equal(readFileSync(path, "utf8"), "existing content");

    rmSync(path);
    initializeQueue(path, settings);
    const before = readFileSync(path);
    assert.throws(() => initializeQueue(path, settings), {
      message: `File already exists: ${path}`,
    });
    assert.deepEqual(readFileSync(path), before);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("addJob rejects empty batches and blank inputs before opening a queue", () => {
  const queues = createQueueService({
    initializeQueue() { assert.fail("must not initialize"); },
    withExistingQueue() { assert.fail("must not open"); },
  });
  for (const inputs of [[], [""], ["valid", " \n\t"]]) {
    assert.throws(() => queues.addJob("jobs", inputs), /nonblank/);
  }
});

test("SQLite batches preserve literal inputs, order, and duplicates", () => {
  const db = new DatabaseSync(":memory:");
  try {
    const store = new SqliteStore(db);
    store.init({ prompt: "prompt", agent: "agent", poolSize: 1, maxAttempts: 3 });
    const inputs = [" first \n", "duplicate", "duplicate"];
    assert.deepEqual(store.addJob(inputs), [1, 2, 3]);
    const rows = db.prepare("SELECT id, input, status FROM jobs ORDER BY id").all();
    assert.deepEqual(rows.map(row => ({ ...row })), inputs.map((input, index) => ({
      id: index + 1, input, status: "pending",
    })));
  } finally {
    db.close();
  }
});

test("reset makes a failed job pending with zero attempts", () => {
  const db = new DatabaseSync(":memory:");
  try {
    const store = new SqliteStore(db);
    store.init({ prompt: "prompt", agent: "agent", poolSize: 1, maxAttempts: 3 });
    const queues = createQueueService({
      initializeQueue() { assert.fail("must not initialize"); },
      withExistingQueue(_path, operation) { return operation(store); },
    });
    const [id] = queues.addJob("jobs", ["input"]);
    const claimed = store.claimNextJob(3, 300);
    assert.ok(claimed);
    assert.equal(store.failJob({ id, leaseToken: claimed.lease_token }, "failed"), true);
    const before = db.prepare("SELECT status, attempts FROM jobs WHERE id = ?").get(id)!;
    assert.equal(before.status, "failed");
    assert.ok(Number(before.attempts) > 0);

    assert.equal(queues.reset("jobs"), 1);
    assert.equal(queues.reset("jobs"), 0);

    const after = db.prepare("SELECT status, attempts FROM jobs WHERE id = ?").get(id)!;
    assert.deepEqual({ ...after }, { status: "pending", attempts: 0 });
  } finally {
    db.close();
  }
});

test("SQLite rolls back the entire batch on insertion failure", () => {
  const db = new DatabaseSync(":memory:");
  try {
    const store = new SqliteStore(db);
    store.init({ prompt: "prompt", agent: "agent", poolSize: 1, maxAttempts: 3 });
    store.addJob(["existing"]);
    db.exec(`CREATE TRIGGER reject_input BEFORE INSERT ON jobs
      WHEN NEW.input = 'reject' BEGIN SELECT RAISE(ABORT, 'rejected'); END`);
    assert.throws(() => store.addJob(["first", "reject", "last"]), /rejected/);
    assert.equal(db.prepare("SELECT count(*) AS count FROM jobs").get()!.count, 1);
    assert.deepEqual(store.addJob(["next"]), [2]);
  } finally {
    db.close();
  }
});

test("entry point registers commands and tools without executing operations", () => {
  const commands: string[] = [];
  const tools: string[] = [];
  const pi = {
    registerCommand(name: string) { commands.push(name); },
    registerTool(tool: { name: string }) { tools.push(tool.name); },
    on() {},
  } as unknown as ExtensionAPI;
  queueExtension(pi);
  assert.deepEqual(commands, ["queue"]);
  assert.deepEqual(tools, ["queue_add"]);
});

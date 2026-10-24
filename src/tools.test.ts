import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import test from "node:test";
import { Check } from "typebox/value";
import registerTools, { QueueAddParams } from "./tools.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { QueueService } from "./queue.ts";

type Params = { path: string; inputs: string[] };

function toolHarness(addJob: QueueService["addJob"] = (_path, inputs) => inputs.map((_, i) => i + 1)) {
  const tools: {
    name: string;
    exposure: string;
    parameters: typeof QueueAddParams;
    execute(id: string, params: Params): Promise<unknown>;
  }[] = [];
  const calls: [string, readonly string[]][] = [];
  registerTools({
    registerTool(tool: typeof tools[number]) { tools.push(tool); },
  } as unknown as ExtensionAPI, {
    addJob(path, inputs) {
      // Record attempts before failures so retry assertions remain meaningful.
      calls.push([path, [...inputs]]);
      return addJob(path, inputs);
    },
  } as QueueService);
  return { tools, execute: (params: Params) => tools[0].execute("call", params), calls };
}

test("registers queue_add for codemode without touching a queue", () => {
  const h = toolHarness();
  assert.equal(h.tools.length, 1);
  assert.equal(h.tools[0].name, "queue_add");
  assert.equal(h.tools[0].exposure, "codemode");
  assert.deepEqual(h.calls, []);
});

// The host validates this schema before execute; calling execute directly bypasses validation.
test("the registered schema accepts 1–100 nonblank literal inputs and explicit paths", () => {
  const { parameters } = toolHarness().tools[0];
  for (const params of [
    { path: "./my jobs", inputs: [" \n literal text \t"] },
    { path: "/tmp/jobs.db", inputs: Array(100).fill("duplicate") },
  ]) {
    assert.equal(Check(parameters, params), true, `Accept ${params.inputs.length} inputs at ${params.path}`);
  }
});

test("the registered schema rejects missing, blank, mistyped, extra, and out-of-range arguments", () => {
  const { parameters } = toolHarness().tools[0];
  const valid = { path: "jobs", inputs: ["input"] };
  const cases: [string, unknown][] = [
    ["missing path", { inputs: valid.inputs }],
    ["missing inputs", { path: valid.path }],
    ["empty path", { ...valid, path: "" }],
    ["whitespace path", { ...valid, path: " \n\t" }],
    ["non-string path", { ...valid, path: 42 }],
    ["empty batch", { ...valid, inputs: [] }],
    ["101 inputs", { ...valid, inputs: Array(101).fill("input") }],
    ["empty input", { ...valid, inputs: [""] }],
    ["blank input among valid inputs", { ...valid, inputs: ["valid", " \n\t"] }],
    ["non-string input", { ...valid, inputs: [42] }],
    ["inputs is not an array", { ...valid, inputs: "input" }],
    ["unknown argument", { ...valid, unexpected: true }],
  ];
  for (const [label, params] of cases) {
    assert.equal(Check(parameters, params), false, label);
  }
});

test("adds one unchanged batch, preserving literal text, duplicates, and returned ID order in every result format", async () => {
  const h = toolHarness(() => [42, 7, 99]);
  const params = { path: "./my jobs", inputs: [' \n"é" C:\\files\t ', "duplicate", "duplicate"] };
  const result = { jobIds: [42, 7, 99], added: 3 };
  assert.deepEqual(await h.execute(params), {
    content: [{ type: "text", text: JSON.stringify(result) }],
    details: result,
    structuredContent: result,
  });
  assert.deepEqual(h.calls, [["./my jobs", [' \n"é" C:\\files\t ', "duplicate", "duplicate"]]]);
});

test("accepts up to exactly 1 MiB of UTF-8 JSON and rejects one byte more before insertion", async t => {
  const limit = 1024 * 1024;
  for (const bytes of [limit - 1, limit, limit + 1]) {
    await t.test(`${bytes} bytes: ${bytes <= limit ? "accepted" : "rejected"}`, async () => {
      const h = toolHarness();
      // Count the entire serialized request: path, JSON syntax, escaping, and multibyte text.
      const params = { path: `queues/${"p".repeat(128)}`, inputs: ['é\n"\\', ""] };
      const overhead = Buffer.byteLength(JSON.stringify(params), "utf8");
      params.inputs[1] = "x".repeat(bytes - overhead);
      assert.equal(Buffer.byteLength(JSON.stringify(params), "utf8"), bytes);
      if (bytes <= limit) {
        await h.execute(params);
        assert.deepEqual(h.calls, [[params.path, params.inputs]]);
      } else {
        await assert.rejects(h.execute(params), /1 MiB.*smaller batches/);
        assert.deepEqual(h.calls, [], "Oversized batches must not reach the queue");
      }
    });
  }
});

test("propagates the original queue failure without retrying an append-only operation", async () => {
  const failure = new Error("Database unavailable");
  const h = toolHarness(() => { throw failure; });
  await assert.rejects(h.execute({ path: "jobs", inputs: ["input"] }), error => error === failure);
  assert.deepEqual(h.calls, [["jobs", ["input"]]], "A retry could append duplicate jobs");
});

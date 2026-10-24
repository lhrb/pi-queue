import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

test("package registers only queue extensions to avoid conflicts with installed subagents", () => {
  const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(manifest.pi.extensions, ["./src/index.ts"]);
});

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import {
  getSubagentsService,
  publishSubagentsService,
  unpublishSubagentsService,
  SUBAGENT_EVENTS,
  type SubagentsService,
} from "@gotgenes/pi-subagents";
import queueExtension from "./index.ts";
import type { EventBus } from "./SubagentWorker.ts";
import { initializeQueue, withExistingQueue } from "./sqlite-store.ts";

function eventBus(): EventBus {
  const emitter = new EventEmitter();
  return {
    on(name, listener) {
      emitter.on(name, listener);
      return () => { emitter.off(name, listener); };
    },
    emit(name, data) { emitter.emit(name, data); },
  };
}

for (const publishBeforeRegistration of [false, true]) {
  for (const childPublication of ["replace", "remove"] as const) {
    test(`limited commands retain parent service (publish before registration: ${publishBeforeRegistration}, child: ${childPublication})`, {
      timeout: 5_000,
    }, async t => {
      const previousService = getSubagentsService();
      const directory = mkdtempSync(join(tmpdir(), "queue-service-resume-test-"));
      const path = join(directory, "test.db");
      const parentEvents = eventBus();
      const childEvents = eventBus();
      const parentSpawns: string[] = [];
      const childSpawns: string[] = [];
      let command!: (args: string, ctx: ExtensionCommandContext) => Promise<void>;
      let start!: () => unknown;
      let shutdown!: () => unknown;
      let secondRun: Promise<void> | undefined;
    
      t.after(async () => {
        shutdown?.();
        await secondRun;
        if (previousService) publishSubagentsService(previousService);
        else unpublishSubagentsService();
        rmSync(directory, { recursive: true, force: true });
      });
    
      // Replace session/model execution, not the queue, worker, global accessor,
      // or event routing. Each service emits only on its owning session's bus.
      const childService = {
        spawn(_agent: string, prompt: string) {
          childSpawns.push(prompt);
          const id = `child-${childSpawns.length}`;
          queueMicrotask(() => childEvents.emit(SUBAGENT_EVENTS.COMPLETED, { id, result: "child result" }));
          return id;
        },
        abort() { return true; },
      } as unknown as SubagentsService;
      const parentService = {
        spawn(_agent: string, prompt: string) {
          parentSpawns.push(prompt);
          const id = `parent-${parentSpawns.length}`;
          queueMicrotask(() => {
            // pi-subagents publishes its service during extension initialization.
            // Child sessions inherit extensions unless explicitly excluded.
            if (childPublication === "replace") publishSubagentsService(childService);
            else unpublishSubagentsService();
            parentEvents.emit(SUBAGENT_EVENTS.COMPLETED, { id, result: `result ${id}` });
          });
          return id;
        },
        abort() { return true; },
      } as unknown as SubagentsService;
      unpublishSubagentsService();
      if (publishBeforeRegistration) publishSubagentsService(parentService);
    
      queueExtension({
        events: parentEvents,
        registerCommand(_name: string, definition: { handler: typeof command }) { command = definition.handler; },
        registerTool() {},
        on(name: string, handler: () => unknown) {
          if (name === "session_start") start = handler;
          if (name === "session_shutdown") shutdown = handler;
        },
      } as unknown as ExtensionAPI);
      if (!publishBeforeRegistration) publishSubagentsService(parentService);
      // Pi starts the session only after every extension factory has loaded.
      await start();
      const notices: [string, string][] = [];
      const ctx = {
        cwd: directory,
        hasUI: false,
        ui: {
          setStatus() {},
          notify(message: string, level: string) { notices.push([message, level]); },
        },
      } as unknown as ExtensionCommandContext;
      initializeQueue(path, { prompt: "Process", agent: "worker", poolSize: 4, maxAttempts: 3 });
      withExistingQueue(path, store => store.addJob(["first", "second"]));
      const args = `run "${path}" 1`;
    
      await command(args, ctx);
      assert.deepEqual(parentSpawns, ["Process first"]);
      assert.deepEqual(withExistingQueue(path, store => store.getProgress()), {
        pending: 1, running: 0, completed: 1, failed: 0,
      });
    
      // Repeated lifecycle notifications must not capture a child's global service.
      await start();
      secondRun = command(args, ctx);
      assert.deepEqual(withExistingQueue(path, store => store.getProgress()), {
        pending: 0, running: 1, completed: 1, failed: 0,
      });
      // Inspect dispatch immediately instead of hanging until a test timeout.
      // SQLite records the claim before the completion microtask settles it.
      assert.deepEqual(parentSpawns, ["Process first", "Process second"],
        `The second job must use the parent's service; child spawns: ${JSON.stringify(childSpawns)}`);
      assert.deepEqual(childSpawns, []);
      await secondRun;
      assert.deepEqual(withExistingQueue(path, store => store.getProgress()), {
        pending: 0, running: 0, completed: 2, failed: 0,
      });
      assert.equal(notices.length, 2);
      assert.ok(notices.every(([, level]) => level === "info"));
    });
  }
}

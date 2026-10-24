import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import registerCommands, { parseQueueCommand } from "./commands.ts";
import { createQueueService, type QueueService } from "./queue";
import { initializeQueue, withExistingQueue, withExistingQueueAsync } from "./sqlite-store";
import type { QueueProgress } from "./types";

describe("command syntax", () => {
  it("treats empty input as a request for help", () => {
    for (const input of ["", " \n\t", " help \t"]) {
      assert.deepEqual(parseQueueCommand(input), { action: "help" }, JSON.stringify(input));
    }
  });

  it("accepts one unchanged path for each single-path command", () => {
    for (const action of ["create", "configure", "info", "run", "reset", "inspect"]) {
      assert.deepEqual(parseQueueCommand(` ${action}\t./jobs `), { action, path: "./jobs" }, action);
    }
  });

  it("accepts an optional positive dispatch limit for run", () => {
    assert.deepEqual(parseQueueCommand('run "my jobs" 3'), {
      action: "run", path: "my jobs", maxJobs: 3,
    });
    for (const value of ["0", "-1", "1.5", "NaN", "Infinity", "9007199254740992", "extra"]) {
      assert.throws(() => parseQueueCommand(`run jobs ${value}`), /maxJobs must be a positive integer/);
    }
  });

  it("supports quoted path segments without shell expansion or backslash escapes", () => {
    for (const path of ['"my jobs.db"', "'my jobs.db'", 'my" jobs".db']) {
      assert.deepEqual(parseQueueCommand(`info ${path}`), { action: "info", path: "my jobs.db" }, path);
    }
    assert.deepEqual(parseQueueCommand('export "C:\\queues\\my jobs" \'$HOME/results file.jsonl\''), {
      action: "export",
      path: "C:\\queues\\my jobs",
      output: "$HOME/results file.jsonl",
    });
  });

  it("treats everything after the add path as literal input, not more command arguments", () => {
    const input = 'Summarize  "this document"\nwith its author\'s notes and C:\\files.  ';
    assert.deepEqual(parseQueueCommand(`add 'my jobs' \t ${input}`), {
      action: "add", path: "my jobs", input,
    });
  });

  it("rejects unknown commands, including inherited object names, with a help hint", () => {
    for (const action of ["unknown", "toString", "constructor"]) {
      assert.throws(() => parseQueueCommand(`${action} jobs`), {
        message: `Unknown queue command: ${action}. Use /queue help.`,
      });
    }
  });

  it("requires exactly the documented arguments and reports the relevant usage", () => {
    const cases = [
      ["help", ""],
      ...["create", "configure", "info", "reset", "inspect"].map(action => [action, "<path>"]),
      ["run", "<path> [maxJobs]"],
      ["export", "<path> <output.jsonl>"],
      ["add", "<path> <input>"],
    ];
    for (const [action, arguments_] of cases) {
      const usage = `/queue ${action}${arguments_ ? ` ${arguments_}` : ""}`;
      if (action !== "help") {
        for (const args of [action, `${action} ""`, `${action} '   '`]) {
          assert.throws(() => parseQueueCommand(args), { message: `Usage: ${usage}` }, args);
        }
      }
      if (action !== "add") {
        const valid = action === "help" ? "help" : action === "export" ? "export jobs out.jsonl" : `${action} jobs`;
        assert.throws(() => parseQueueCommand(`${valid}${action === "run" ? " 3" : ""} extra`), {
          message: `Unexpected arguments. Usage: ${usage}`,
        }, action);
      }
    }
    for (const args of ["export jobs", "export jobs ''", "add jobs", "add jobs \n\t"]) {
      const usage = args.startsWith("export") ? "/queue export <path> <output.jsonl>" : "/queue add <path> <input>";
      assert.throws(() => parseQueueCommand(args), { message: `Usage: ${usage}` }, args);
    }
  });

  it("rejects unclosed path quotes and null characters, even inside literal job input", () => {
    assert.throws(() => parseQueueCommand('inspect "unfinished'), {
      message: "Unclosed quote in command argument.",
    });
    for (const args of ["info jobs\0.db", "add jobs input\0text"]) {
      assert.throws(() => parseQueueCommand(args), { message: "Command must not contain null characters." });
    }
  });
});

// Only external boundaries are replaced: dialogs, queue operations, and the browser launcher.
// Record attempts before throwing so failure tests cannot pass when dispatch never happened.
function commandHarness({
  answers = [],
  hasUI = true,
  failure,
  runQueue,
}: {
  answers?: (string | undefined)[];
  hasUI?: boolean;
  failure?: unknown;
  runQueue?: QueueService["runQueue"];
} = {}) {
  let handler!: (args: string, ctx: ExtensionCommandContext) => Promise<void>;
  const calls: unknown[][] = [];
  const notices: [string, string][] = [];
  const statuses: [string, string | undefined][] = [];
  const inputTitles: string[] = [];
  let answerIndex = 0;
  const effect = (name: string, args: unknown[]) => {
    calls.push([name, ...args]);
    if (failure !== undefined) throw failure;
  };
  const queues: QueueService = {
    initQueue(...args) { effect("initQueue", args); },
    updateSettings(...args) { effect("updateSettings", args); },
    addJob(...args) { effect("addJob", args); return [42]; },
    reset(...args) { effect("reset", args); return 3; },
    exportQueue(...args) { effect("exportQueue", args); return 2; },
    async runQueue(path, options) {
      effect("runQueue", [path]);
      assert.ok(runQueue, "Provide runner behavior explicitly when testing run");
      return runQueue(path, options);
    },
    shutdown() { assert.fail("Commands must not shut down the queue service"); },
  };
  const ctx = {
    cwd: "/workspace",
    hasUI,
    ui: {
      async input(title: string) {
        inputTitles.push(title);
        return answers[answerIndex++];
      },
      setStatus(key: string, value: string | undefined) { statuses.push([key, value]); },
      notify(message: string, level: string) { notices.push([message, level]); },
    },
  } as unknown as ExtensionCommandContext;
  const registrations: string[] = [];
  const pi = {
    registerCommand(name: string, command: { handler: typeof handler }) {
      registrations.push(name);
      handler = command.handler;
    },
  } as unknown as ExtensionAPI;
  registerCommands(pi, queues, async path => { effect("openBrowser", [path]); });
  return { run: (args: string) => handler(args, ctx), calls, notices, statuses, inputTitles, registrations };
}

describe("command dispatch", () => {
  it("registers /queue without effects and provides help without opening dialogs", async () => {
    const h = commandHarness({ hasUI: false });
    assert.deepEqual(h.registrations, ["queue"]);
    assert.deepEqual(h.calls, []);
    assert.equal(h.notices.length, 0);
    await h.run("");
    assert.equal(h.notices.length, 1);
    const [help, level] = h.notices[0];
    assert.equal(level, "info");
    for (const usage of [
      "/queue create <path>", "/queue configure <path>", "/queue info <path>",
      "/queue add <path> <input>", "/queue run <path> [maxJobs]", "/queue reset <path>",
      "/queue export <path> <output.jsonl>", "/queue inspect <path>", "/queue help",
    ]) assert.ok(help.split("\n").includes(usage), `Help must list ${usage}`);
    assert.deepEqual(h.calls, []);
    assert.deepEqual(h.inputTitles, []);
  });

  it("passes the dispatch limit to the queue service", async () => {
    let received: number | undefined;
    const h = commandHarness({
      async runQueue(_path, options) {
        received = options?.maxJobs;
        return { pending: 2, running: 0, completed: 3, failed: 0 };
      },
    });
    await h.run('run "my jobs" 3');
    assert.equal(received, 3);
    assert.deepEqual(h.calls, [["runQueue", "my jobs"]]);
  });

  it("turns syntax errors into warnings without prompting or invoking the service", async () => {
    const h = commandHarness();
    await h.run("reset jobs extra");
    assert.deepEqual(h.notices, [["Unexpected arguments. Usage: /queue reset <path>", "warning"]]);
    assert.deepEqual(h.calls, []);
    assert.deepEqual(h.inputTitles, []);
  });

  it("adds one literal job, resets failed jobs, and exports through the service without a UI", async () => {
    const h = commandHarness({ hasUI: false });
    const input = 'literal "text"\nwith spaces  ';
    await h.run(`add "my jobs" ${input}`);
    await h.run('reset "my jobs"');
    await h.run('export "my jobs" "my results.jsonl"');
    assert.deepEqual(h.calls, [
      ["addJob", "my jobs", [input]],
      ["reset", "my jobs"],
      ["exportQueue", "my jobs", "my results.jsonl"],
    ]);
    assert.deepEqual(h.notices, [
      ["Job added: 42", "info"],
      ["Failed jobs reset: 3", "info"],
      ["Exported 2 jobs: my results.jsonl", "info"],
    ]);
    assert.deepEqual(h.inputTitles, []);
  });

  it("resolves inspect paths against the command directory and adds .db only when needed", async () => {
    for (const path of ["my jobs", "my jobs.db", "/workspace/my jobs.db"]) {
      const h = commandHarness({ hasUI: false });
      await h.run(`inspect "${path}"`);
      assert.deepEqual(h.calls, [["openBrowser", "/workspace/my jobs.db"]], path);
      assert.deepEqual(h.notices, [["Opened database browser: /workspace/my jobs.db", "info"]], path);
      assert.deepEqual(h.inputTitles, []);
    }
  });

  it("reports operation failures as errors, never as success", async () => {
    const cases = [
      ["create jobs", "initQueue"], ["configure jobs", "updateSettings"],
      ["add jobs input", "addJob"], ["reset jobs", "reset"],
      ["export jobs out.jsonl", "exportQueue"], ["inspect jobs", "openBrowser"],
      ["run jobs", "runQueue"],
    ];
    for (const [command, operation] of cases) {
      // Rejections need not be Error objects; both forms must remain readable.
      for (const failure of [new Error("Database unavailable"), "Database unavailable"]) {
        const h = commandHarness({ answers: ["worker", "Prompt", "2", "3"], failure });
        await h.run(command);
        assert.equal(h.calls.length, 1, command);
        assert.equal(h.calls[0][0], operation, command);
        assert.deepEqual(h.notices, [["Database unavailable", "error"]], command);
        if (operation === "runQueue") assert.deepEqual(h.statuses, [["queue:jobs", undefined]]);
      }
    }
  });
});

describe("settings dialogs", () => {
  it("creates with a required agent, an optional prompt, and default or explicit numeric settings", async () => {
    for (const { answers, settings } of [
      { answers: ["worker", "", " ", ""], settings: ["", "worker", 4, 3] },
      { answers: ["worker", " Prompt ", "1", "5"], settings: [" Prompt ", "worker", 1, 5] },
    ]) {
      const h = commandHarness({ answers });
      await h.run('create "my jobs"');
      assert.deepEqual(h.calls, [["initQueue", "my jobs", ...settings]]);
      assert.deepEqual(h.notices, [["Queue created: my jobs", "info"]]);
      assert.deepEqual(h.inputTitles, [
        "agent (required)", 'prompt (default: "")', "poolSize (4)", "maxAttempts (3)",
      ]);
    }
  });

  it("configures only nonblank fields; blank means keep, not clear or restore defaults", async () => {
    for (const { answers, update } of [
      { answers: ["", "worker", "2", " "], update: { agent: "worker", poolSize: 2 } },
      { answers: [" Prompt ", " ", "", "1"], update: { prompt: " Prompt ", maxAttempts: 1 } },
    ]) {
      const h = commandHarness({ answers });
      await h.run("configure jobs");
      assert.deepEqual(h.calls, [["updateSettings", "jobs", update]]);
      assert.deepEqual(h.notices, [["Queue settings updated: jobs", "info"]]);
    }
    const h = commandHarness({ answers: ["", " ", "", "\t"] });
    await h.run("configure jobs");
    assert.deepEqual(h.calls, []);
    assert.deepEqual(h.notices, []);
  });

  it("requires an interactive UI before asking for settings or writing anything", async () => {
    for (const action of ["create", "configure"]) {
      const h = commandHarness({ hasUI: false });
      await h.run(`${action} jobs`);
      assert.deepEqual(h.inputTitles, [], action);
      assert.deepEqual(h.calls, [], action);
      assert.deepEqual(h.notices, [["Queue settings require an interactive UI.", "warning"]], action);
    }
  });

  it("cancels silently at any dialog without committing partial settings or asking further questions", async () => {
    for (const action of ["create", "configure"]) {
      for (let index = 0; index < 4; index++) {
        const answers: (string | undefined)[] = action === "create"
          ? ["worker", "Prompt", "2", "3"] : ["Prompt", "worker", "2", "3"];
        answers[index] = undefined;
        const h = commandHarness({ answers });
        await h.run(`${action} jobs`);
        const label = `${action}, cancelled dialog ${index + 1}`;
        assert.deepEqual(h.calls, [], label);
        assert.deepEqual(h.notices, [], label);
        assert.equal(h.inputTitles.length, index + 1, label);
      }
    }
  });

  it("rejects a blank create agent instead of creating an unusable queue", async () => {
    for (const agent of ["", " \t"]) {
      const h = commandHarness({ answers: [agent, "Prompt", "2", "3"] });
      await h.run("create jobs");
      assert.deepEqual(h.calls, []);
      assert.deepEqual(h.notices, [["agent is required.", "error"]]);
      assert.equal(h.inputTitles.length, 1);
    }
  });

  it("requires both numeric fields to be positive safe integers before either settings operation commits", async () => {
    for (const action of ["create", "configure"]) {
      for (const [index, field] of [[2, "poolSize"], [3, "maxAttempts"]] as const) {
        for (const value of ["0", "-1", "1.5", "NaN", "Infinity", "9007199254740992"]) {
          const answers = ["worker", "Prompt", "2", "3"];
          answers[index] = value;
          const h = commandHarness({ answers });
          await h.run(`${action} jobs`);
          const label = `${action}: ${field}=${value}`;
          assert.deepEqual(h.calls, [], label);
          assert.deepEqual(h.notices, [[`${field} must be a positive integer.`, "error"]], label);
        }
      }
    }
  });
});

describe("run lifecycle", () => {
  it("processes two jobs exactly once across two /queue run test.db 1 commands", { timeout: 5_000 }, async t => {
    const directory = mkdtempSync(join(tmpdir(), "queue-resume-test-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const path = join(directory, "test.db");
    const executed: number[] = [];
    const queues = createQueueService({
      initializeQueue,
      withExistingQueue,
      withExistingQueueAsync,
      createWorker: () => ({
        async run(job) {
          executed.push(job.id);
          return { status: "completed", result: `result ${job.id}` };
        },
      }),
    });
    t.after(() => queues.shutdown());
    queues.initQueue(path, "Process", "worker");
    const ids = queues.addJob(path, ["first", "second"]);
    const h = commandHarness({ runQueue: queues.runQueue });
    const command = `run "${path}" 1`;

    await h.run(command);
    assert.deepEqual(executed, ids.slice(0, 1));
    assert.deepEqual(withExistingQueue(path, store => store.getProgress()), {
      pending: 1, running: 0, completed: 1, failed: 0,
    });

    await h.run(command);
    assert.deepEqual(executed, ids, "the second run must execute only the remaining job");
    assert.deepEqual(withExistingQueue(path, store => store.getProgress()), {
      pending: 0, running: 0, completed: 2, failed: 0,
    });
    assert.deepEqual(withExistingQueue(path, store => store.getJobsForExport())
      .map(row => ({ ...row })), [
      { input: "first", status: "completed", result: `result ${ids[0]}` },
      { input: "second", status: "completed", result: `result ${ids[1]}` },
    ]);
    assert.deepEqual(h.notices, [
      [`Queue finished: ${path}\nRunning: 0 · Completed: 1 · Failed: 0 · Pending: 1`, "info"],
      [`Queue finished: ${path}\nRunning: 0 · Completed: 2 · Failed: 0 · Pending: 0`, "info"],
    ]);
  });

  it("shows live progress while running, then summarizes the result and clears status on every exit", async () => {
    for (const outcome of ["success", "failed jobs", "runner error"] as const) {
      let finish!: (summary: QueueProgress) => void;
      let fail!: (error: Error) => void;
      const result = new Promise<QueueProgress>((resolve, reject) => { finish = resolve; fail = reject; });
      const h = commandHarness({
        hasUI: false,
        runQueue: async (_path, options) => {
          options?.onProgress?.({ running: 1, completed: 2, failed: 3, pending: 4 });
          return result;
        },
      });
      const running = h.run('run "my jobs"');
      assert.deepEqual(h.calls, [["runQueue", "my jobs"]], outcome);
      assert.deepEqual(h.statuses, [["queue:my jobs", "Running: 1 · Completed: 2 · Failed: 3 · Pending: 4"]], outcome);
      assert.deepEqual(h.notices, [], "Do not announce completion before the runner settles");

      if (outcome === "runner error") fail(new Error("Worker disconnected"));
      else finish({ running: 0, completed: 6, failed: outcome === "failed jobs" ? 1 : 0, pending: 0 });
      await running;

      assert.deepEqual(h.statuses, [
        ["queue:my jobs", "Running: 1 · Completed: 2 · Failed: 3 · Pending: 4"],
        ["queue:my jobs", undefined],
      ], outcome);
      assert.deepEqual(h.notices, outcome === "runner error"
        ? [["Worker disconnected", "error"]]
        : [[`Queue finished: my jobs\nRunning: 0 · Completed: 6 · Failed: ${outcome === "failed jobs" ? 1 : 0} · Pending: 0`,
          outcome === "failed jobs" ? "warning" : "info"]], outcome);
      assert.deepEqual(h.inputTitles, [], outcome);
    }
  });
});

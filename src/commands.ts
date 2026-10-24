import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { resolve } from "node:path";
import { normalizeDatabasePath, type QueueService } from "./queue";
import type { QueueProgress, QueueSettings, QueueSettingsUpdate } from "./types";

function formatProgress(progress: QueueProgress): string {
  return `Running: ${progress.running} · Completed: ${progress.completed} · Failed: ${progress.failed} · Pending: ${progress.pending}`;
}

export type QueueCommand =
  | { action: "create"; path: string }
  | { action: "configure"; path: string }
  | { action: "info"; path: string }
  | { action: "add"; path: string; input: string }
  | { action: "run"; path: string; maxJobs?: number }
  | { action: "reset"; path: string }
  | { action: "export"; path: string; output: string }
  | { action: "inspect"; path: string }
  | { action: "help" };

const usage = {
  create: "/queue create <path>",
  configure: "/queue configure <path>",
  info: "/queue info <path>",
  add: "/queue add <path> <input>",
  run: "/queue run <path> [maxJobs]",
  reset: "/queue reset <path>",
  export: "/queue export <path> <output.jsonl>",
  inspect: "/queue inspect <path>",
  help: "/queue help",
} as const;

const help = [
  "Queue commands:",
  ...Object.values(usage),
  "Create and configure will collect settings through dialogs.",
  "Quote paths containing spaces with single or double quotes.",
  "For add, everything after the path is literal job input.",
  "For run, maxJobs limits dispatches, including failures and retries; omit to run all jobs.",
].join("\n");

/** Parse syntax only. Path normalization and queue rules belong to use cases. */
export function parseQueueCommand(args: string): QueueCommand {
  if (args.includes("\0")) {
    throw new Error("Command must not contain null characters.");
  }

  let position = 0;

  const skipWhitespace = () => {
    while (position < args.length && /\s/.test(args[position])) position++;
  };

  // Paths support quoted segments. Backslashes remain literal; this is not a shell.
  const readArgument = (): string | undefined => {
    skipWhitespace();
    if (position === args.length) return undefined;

    let value = "";
    let quote: string | undefined;
    while (position < args.length) {
      const character = args[position];
      if (quote) {
        if (character === quote) quote = undefined;
        else value += character;
      } else if (character === '"' || character === "'") {
        quote = character;
      } else if (/\s/.test(character)) {
        break;
      } else {
        value += character;
      }
      position++;
    }

    if (quote) throw new Error("Unclosed quote in command argument.");
    return value;
  };

  const action = readArgument();
  if (action === undefined) return { action: "help" };
  if (!Object.hasOwn(usage, action)) {
    throw new Error(`Unknown queue command: ${action}. Use /queue help.`);
  }

  const requireArgument = (): string => {
    const value = readArgument();
    if (!value?.trim()) {
      throw new Error(`Usage: ${usage[action as keyof typeof usage]}`);
    }
    return value;
  };

  const requireEnd = () => {
    skipWhitespace();
    if (position !== args.length) {
      throw new Error(`Unexpected arguments. Usage: ${usage[action as keyof typeof usage]}`);
    }
  };

  switch (action) {
    case "help":
      requireEnd();
      return { action };
    case "create":
    case "configure":
    case "info":
    case "reset":
    case "inspect": {
      const path = requireArgument();
      requireEnd();
      return { action, path };
    }
    case "run": {
      const path = requireArgument();
      const limit = readArgument();
      const maxJobs = limit === undefined ? undefined : positiveInteger(limit, "maxJobs");
      requireEnd();
      return maxJobs === undefined ? { action, path } : { action, path, maxJobs };
    }
    case "export": {
      const path = requireArgument();
      const output = requireArgument();
      requireEnd();
      return { action, path, output };
    }
    case "add": {
      const path = requireArgument();
      skipWhitespace();
      const input = args.slice(position);
      if (!input.trim()) throw new Error(`Usage: ${usage.add}`);
      return { action, path, input };
    }
    default:
      throw new Error(`Unknown queue command: ${action}. Use /queue help.`);
  }
}

function positiveInteger(value: string, field: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1) {
    throw new Error(`${field} must be a positive integer.`);
  }
  return number;
}

async function collectCreateSettings(
  ctx: ExtensionCommandContext,
): Promise<QueueSettings | undefined> {
  const agent = await ctx.ui.input("agent (required)");
  if (agent === undefined) return;
  if (!agent.trim()) throw new Error("agent is required.");

  const prompt = await ctx.ui.input('prompt (default: "")');
  if (prompt === undefined) return;

  const poolSizeInput = await ctx.ui.input("poolSize (4)");
  if (poolSizeInput === undefined) return;
  const poolSize = poolSizeInput.trim() ? positiveInteger(poolSizeInput, "poolSize") : 4;

  const maxAttemptsInput = await ctx.ui.input("maxAttempts (3)");
  if (maxAttemptsInput === undefined) return;
  const maxAttempts = maxAttemptsInput.trim() ? positiveInteger(maxAttemptsInput, "maxAttempts") : 3;

  return { prompt, agent, poolSize, maxAttempts };
}

async function collectSettingsUpdate(
  ctx: ExtensionCommandContext,
): Promise<QueueSettingsUpdate | undefined> {
  const settings: QueueSettingsUpdate = {};

  const prompt = await ctx.ui.input("prompt (keep current)");
  if (prompt === undefined) return;
  if (prompt.trim()) settings.prompt = prompt;

  const agent = await ctx.ui.input("agent (keep current)");
  if (agent === undefined) return;
  if (agent.trim()) settings.agent = agent;

  const poolSize = await ctx.ui.input("poolSize (keep current)");
  if (poolSize === undefined) return;
  if (poolSize.trim()) settings.poolSize = positiveInteger(poolSize, "poolSize");

  const maxAttempts = await ctx.ui.input("maxAttempts (keep current)");
  if (maxAttempts === undefined) return;
  if (maxAttempts.trim()) settings.maxAttempts = positiveInteger(maxAttempts, "maxAttempts");

  return settings;
}

async function openDatabaseBrowser(path: string): Promise<void> {
  if (!(await stat(path)).isFile()) throw new Error(`Not a database file: ${path}`);
  await new Promise<void>((resolve, reject) => {
    const child = spawn("sqlitebrowser", [path], { detached: true, stdio: "ignore" });
    child.once("error", error => reject(new Error(`Could not open sqlitebrowser: ${error.message}`)));
    child.once("spawn", () => {
      child.unref();
      resolve();
    });
  });
}

export default function registerCommands(
  pi: ExtensionAPI,
  queues: QueueService,
  openBrowser: (path: string) => Promise<void> = openDatabaseBrowser,
): void {
  pi.registerCommand("queue", {
    description: "Manage job queues. Use /queue help for commands.",
    handler: async (args, ctx) => {
      let command: QueueCommand;
      try {
        command = parseQueueCommand(args);
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
        return;
      }

      if (command.action === "help") {
        ctx.ui.notify(help, "info");
        return;
      }

      if (command.action === "create" || command.action === "configure") {
        if (!ctx.hasUI) {
          ctx.ui.notify("Queue settings require an interactive UI.", "warning");
          return;
        }
        try {
          if (command.action === "create") {
            const settings = await collectCreateSettings(ctx);
            if (!settings) return;
            queues.initQueue(command.path, settings.prompt, settings.agent, settings.poolSize, settings.maxAttempts);
            ctx.ui.notify(`Queue created: ${command.path}`, "info");
          } else {
            const settings = await collectSettingsUpdate(ctx);
            if (!settings || Object.keys(settings).length === 0) return;
            queues.updateSettings(command.path, settings);
            ctx.ui.notify(`Queue settings updated: ${command.path}`, "info");
          }
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        }
        return;
      }

      if (command.action === "add") {
        try {
          const [jobId] = queues.addJob(command.path, [command.input]);
          ctx.ui.notify(`Job added: ${jobId}`, "info");
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        }
        return;
      }

      if (command.action === "reset") {
        try {
          const count = queues.reset(command.path);
          ctx.ui.notify(`Failed jobs reset: ${count}`, "info");
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        }
        return;
      }

      if (command.action === "export") {
        try {
          const count = queues.exportQueue(command.path, command.output);
          ctx.ui.notify(`Exported ${count} jobs: ${command.output}`, "info");
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        }
        return;

      }

      if (command.action === "inspect") {
        try {
          const path = resolve(ctx.cwd, normalizeDatabasePath(command.path));
          await openBrowser(path);
          ctx.ui.notify(`Opened database browser: ${path}`, "info");
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        }
        return;
      }

      if (command.action === "run") {
        const statusKey = `queue:${command.path}`;
        try {
          const summary = await queues.runQueue(command.path, {
            maxJobs: command.maxJobs,
            onProgress(progress) {
              ctx.ui.setStatus(statusKey, formatProgress(progress));
            },
          });
          ctx.ui.notify(`Queue finished: ${command.path}\n${formatProgress(summary)}`,
            summary.failed > 0 ? "warning" : "info");
        } catch (error) {
          ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
        } finally {
          ctx.ui.setStatus(statusKey, undefined);
        }
        return;
      }

      ctx.ui.notify(`Queue command '${command.action}' is not implemented yet.`, "info");
    },
  });
}

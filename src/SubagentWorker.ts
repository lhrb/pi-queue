import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { SUBAGENT_EVENTS, type SubagentsService } from "@gotgenes/pi-subagents";
import type { Worker, JobExecution, JobOutcome } from "./Worker";
import { failedOutcome } from "./Failure";

export type EventBus = ExtensionAPI["events"];

type SubagentCompletedEvent = {
  id: string;
  result: string;
};

type SubagentFailedEvent = {
  id: string;
  error?: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isCompleteEvent(value: unknown): value is SubagentCompletedEvent {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.result === "string"
  );
}

function isFailedEvent(value: unknown): value is SubagentFailedEvent {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    (value.error === undefined || typeof value.error === "string")
  );
}

export class SubagentWorker implements Worker {
  constructor(
    private readonly events: EventBus,
    private readonly subagents: SubagentsService,
  ) { }

  run(execution: JobExecution, signal: AbortSignal): Promise<JobOutcome> {
    if (signal.aborted) {
      return Promise.reject(signal.reason);
    }

    return new Promise<JobOutcome>((resolve, reject) => {
      let agentId: string | undefined;
      let settled = false;
      const subscriptions: (() => void)[] = [];

      const cleanup = () => {
        subscriptions.forEach((unsubscribe) => unsubscribe());
        signal.removeEventListener("abort", onAbort);
      };

      const finish = (result: JobOutcome) => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(result);
      };

      const fail = (error: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(error);
      };

      const onAbort = () => {
        if (settled) return;

        // Settle and unsubscribe before aborting, which may emit a failure event.
        fail(signal.reason);
        if (agentId) {
          try {
            this.subagents.abort(agentId);
          } catch (error) {
            console.error(`Could not abort subagent ${agentId}`, error);
          }
        }
      };

      subscriptions.push(
        this.events.on(SUBAGENT_EVENTS.COMPLETED, (data) => {
          if (!isCompleteEvent(data)) {
            console.warn("Invalid subagent completion payload", data);
            return;
          }

          if (data.id !== agentId) return;

          finish({ status: "completed", result: data.result });
        }),

        this.events.on(SUBAGENT_EVENTS.FAILED, (data: unknown) => {
          if (!isFailedEvent(data)) {
            console.warn("Invalid subagent failure payload", data);
            return;
          }

          if (data.id !== agentId) return;
          finish(failedOutcome(data.error));
        }),
      );

      signal.addEventListener("abort", onAbort, { once: true });

      try {
        agentId = this.subagents.spawn(execution.agent, execution.prompt, {
          inheritContext: false,
          foreground: false,
        });

      } catch (error) {
        finish(failedOutcome(error));
      }
    });
  }
}

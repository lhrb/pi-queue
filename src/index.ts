import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import registerCommands from "./commands";
import registerTools from "./tools";
import { createQueueService } from "./queue";
import { initializeQueue, withExistingQueue, withExistingQueueAsync } from "./sqlite-store";
import { getSubagentsService, type SubagentsService } from "@gotgenes/pi-subagents";
import { SubagentWorker } from "./SubagentWorker";

export default function queueExtension(pi: ExtensionAPI): void {
  let subagents: SubagentsService | undefined;
  pi.on("session_start", () => {
    // All extension factories have loaded, regardless of registration order.
    // Retain this session's service: child sessions can replace or remove the global.
    subagents ??= getSubagentsService();
  });

  const queues = createQueueService({
    initializeQueue,
    withExistingQueue,
    withExistingQueueAsync,
    createWorker() {
      if (!subagents) throw new Error("The subagents extension is not available.");
      return new SubagentWorker(pi.events, subagents);
    },
  });
  pi.on("session_shutdown", () => queues.shutdown());
  registerCommands(pi, queues);
  registerTools(pi, queues);
}

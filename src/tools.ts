import { Buffer } from "node:buffer";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { QueueService } from "./queue";
import { Type } from "typebox";

const maxBatchSize = 100;
const maxPayloadBytes = 1024 * 1024;

export const QueueAddParams = Type.Object({
  path: Type.String({
    minLength: 1,
    pattern: "\\S",
    description: "Explicit path to an existing queue database, relative to the working directory or absolute.",
  }),
  inputs: Type.Array(Type.String({
    minLength: 1,
    pattern: "\\S",
    description: "Prepared job input. Stored as literal text without trimming or interpretation.",
  }), {
    minItems: 1,
    maxItems: maxBatchSize,
    description: "Job inputs in insertion order. Use a one-item list to add one job.",
  }),
}, { additionalProperties: false });

export default function registerTools(
  pi: ExtensionAPI,
  queues: QueueService,
): void {
  pi.registerTool({
    name: "queue_add",
    label: "Add queue jobs",
    exposure: "codemode",
    description: [
      "Append 1–100 prepared jobs to an existing queue; never creates or runs queues.",
      "For file imports, prefer codemode to parse, transform, and submit batches without returning source data to model context.",
      "Each batch is atomic: all jobs are inserted or none. Multiple batches are independent.",
      "Returns { jobIds: number[], added: number }, with IDs matching input order.",
      "The UTF-8 JSON payload must not exceed 1 MiB. Split larger imports into smaller batches.",
      "Append-only and non-idempotent: identical inputs create separate jobs. Do not automatically retry uncertain outcomes.",
    ].join(" "),
    parameters: QueueAddParams,
    outputSchema: Type.Object({
      jobIds: Type.Array(Type.Integer({ minimum: 1 })),
      added: Type.Integer({ minimum: 1, maximum: maxBatchSize }),
    }),
    async execute(_toolCallId, params) {
      if (Buffer.byteLength(JSON.stringify(params), "utf8") > maxPayloadBytes) {
        throw new Error("Queue batch payload must not exceed 1 MiB. Submit smaller batches.");
      }

      const jobIds = queues.addJob(params.path, params.inputs);
      const result = { jobIds, added: jobIds.length };
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
        details: result,
        structuredContent: result,
      };
    },
  });
}

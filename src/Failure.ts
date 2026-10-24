import type { FailureKind, JobOutcome } from "./Worker";

/** Shared boundary policy for event messages and unexpected worker exceptions. */
export function classifyFailure(error: string): FailureKind {
  if (/context_length_exceeded|maximum context length|context window/i.test(error)) return "job";
  if (/insufficient_quota|exceeded your current quota|quota[_ ](?:exhausted|exceeded)|exceeded (?:your |the |account )?quota|(?:insufficient|exhausted|out of|not enough) credits|credits.*exhausted|credit balance.*(?:low|exhausted)|invalid[_ ]api[_ ]key|invalid credentials|authentication[_ ]error|authentication failed|authentication denied|authentication required|unauthorized|permission denied|permission_denied|\bHTTP\s+(?:401|403)\b/i.test(error)) return "blocked";
  if (/\bHTTP\s+(?:429|503)\b|rate[_ ]limit|service[_ ]unavailable|ETIMEDOUT|connection (?:timed out|timeout)/i.test(error)) return "transient";
  return "job";
}

export function failedOutcome(value: unknown): Extract<JobOutcome, { status: "failed" }> {
  const record = typeof value === "object" && value !== null ? value : undefined;
  const error = record && "message" in record && typeof record.message === "string"
    ? record.message
    : String(value ?? "Subagent failed without an error message.");
  const code = record && "code" in record ? String(record.code) : "";
  return { status: "failed", error, kind: classifyFailure(code && classifyFailure(code) !== "job" ? code : error) };
}

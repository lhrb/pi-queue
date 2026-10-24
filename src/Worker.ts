export type JobExecution = {
  id: number;
  leaseToken: string;
  agent: string;
  prompt: string;
}

export type FailureKind = "job" | "transient" | "blocked";

export type JobOutcome =
  | { status: "completed", result: string }
  | { status: "failed", error: string, kind: FailureKind }

export interface Worker {
  run(execution: JobExecution, signal: AbortSignal): Promise<JobOutcome>;
}

export interface QueueSettings {
  prompt: string;
  agent: string;
  poolSize: number;
  maxAttempts: number;
}

export type QueueSettingsUpdate = Partial<QueueSettings>;

export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface QueueProgress {
  pending: number;
  running: number;
  completed: number;
  failed: number;
}

export interface Job {
  id: number;
  input: string;
  status: JobStatus;
  attempts: number;
  lease_token: string | null;
  lease_until: number | null;
  created_at: number;
  updated_at: number;
  result: string | null;
  error: string | null;
}

export type ExportedJob = Pick<Job, "input" | "status" | "result">;

export interface ClaimedJob extends Job {
  status: "running";
  lease_token: string;
  lease_until: number;
}

export interface JobLease {
  id: number;
  leaseToken: string;
}

export interface QueueStore {
  init(settings: QueueSettings): void;
  getSettings(): QueueSettings;
  updateSettings(options: QueueSettingsUpdate): void;
  addJob(inputs: readonly string[]): number[];
  reset(): number;
  getProgress(): QueueProgress;
  getJobsForExport(): ExportedJob[];
  recoverExpiredJobs(maxAttempts: number): void;
  renewLease(lease: JobLease, leaseSeconds: number): boolean;
  claimNextJob(maxAttempts: number, leaseSeconds: number): ClaimedJob | null;
  completeJob(lease: JobLease, result: string): boolean;
  failJob(lease: JobLease, error: string): boolean;
  releaseJob(lease: JobLease, error: string): boolean;
}

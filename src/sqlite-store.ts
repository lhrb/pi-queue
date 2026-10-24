import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import { closeSync, openSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type {
  ClaimedJob,
  ExportedJob,
  JobLease,
  QueueProgress,
  QueueSettings,
  QueueSettingsUpdate,
  QueueStore,
} from "./types";

function openExistingQueue(path: string): DatabaseSync {
  const uri = pathToFileURL(resolve(path));
  uri.searchParams.set("mode", "rw");
  const db = new DatabaseSync(uri.href);
  try {
    new SqliteStore(db).getSettings();
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}

/** Open an existing queue for a synchronous operation, then always close it. */
export function withExistingQueue<T>(
  path: string,
  operation: (store: QueueStore) => T,
): T {
  const db = openExistingQueue(path);
  try {
    return operation(new SqliteStore(db));
  } finally {
    db.close();
  }
}

/** Keep an existing queue open until the asynchronous operation settles. */
export async function withExistingQueueAsync<T>(
  path: string,
  operation: (store: QueueStore) => Promise<T>,
): Promise<T> {
  const db = openExistingQueue(path);
  try {
    return await operation(new SqliteStore(db));
  } finally {
    db.close();
  }
}

/** Create a new queue without overwriting an existing file, then close its database. */
export function initializeQueue(path: string, settings: QueueSettings): void {
  // Reserve the filename atomically rather than checking existence before creation.
  let descriptor: number;
  try {
    descriptor = openSync(path, "wx");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      throw new Error(`File already exists: ${path}`);
    }
    throw error;
  }
  closeSync(descriptor);

  const db = new DatabaseSync(path);
  try {
    new SqliteStore(db).init(settings);
  } finally {
    db.close();
  }
}

export class SqliteStore implements QueueStore {
  constructor(private readonly db: DatabaseSync) {}

  init(settings: QueueSettings): void {
    this.db.exec("BEGIN");
    try {
      this.createTables();
      this.insertSettings(settings);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private createTables(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        prompt TEXT NOT NULL,
        agent TEXT NOT NULL,
        pool_size INTEGER NOT NULL DEFAULT 4,
        max_attempts INTEGER NOT NULL DEFAULT 3
      ) STRICT;

      CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY,
        input TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
          CHECK (status IN ('pending', 'running', 'completed', 'failed')),
        attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        lease_token TEXT,
        lease_until INTEGER,
        created_at INTEGER NOT NULL DEFAULT (unixepoch()),
        updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
        result TEXT,
        error TEXT
      ) STRICT;
    `);
  }

  private insertSettings(settings: QueueSettings): void {
    this.db.prepare(
      "INSERT INTO settings (id, prompt, agent, pool_size, max_attempts) VALUES (1, ?, ?, ?, ?)",
    ).run(settings.prompt, settings.agent, settings.poolSize, settings.maxAttempts);
  }

  getSettings(): QueueSettings {
    const row = this.db.prepare(
      `SELECT prompt, agent, pool_size AS poolSize, max_attempts AS maxAttempts
       FROM settings WHERE id = 1`,
    ).get();

    if (!row) {
      throw new Error("Queue settings do not exist. Initialize the queue first.");
    }

    return row as unknown as QueueSettings;
  }

  updateSettings(options: QueueSettingsUpdate): void {
    const result = this.db.prepare(`
      UPDATE settings
      SET prompt = COALESCE(?, prompt),
          agent = COALESCE(?, agent),
          pool_size = COALESCE(?, pool_size),
          max_attempts = COALESCE(?, max_attempts)
      WHERE id = 1
    `).run(
      options.prompt ?? null,
      options.agent ?? null,
      options.poolSize ?? null,
      options.maxAttempts ?? null,
    );

    if (result.changes === 0) {
      throw new Error("Queue settings do not exist. Initialize the queue first.");
    }
  }

  addJob(inputs: readonly string[]): number[] {
    const insert = this.db.prepare("INSERT INTO jobs (input) VALUES(?)");
    this.db.exec("BEGIN");
    try {
      const jobIds = inputs.map(input => {
        const id = Number(insert.run(input).lastInsertRowid);
        if (!Number.isSafeInteger(id)) {
          throw new Error("Job ID exceeds the safe integer range.");
        }
        return id;
      });
      this.db.exec("COMMIT");
      return jobIds;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }


  reset(): number {
    const { changes } = this.db.prepare(`
      UPDATE jobs
      SET status = 'pending',
          attempts = 0,
          lease_token = NULL,
          lease_until = NULL,
          result = NULL,
          error = NULL,
          updated_at = unixepoch()
      WHERE status = 'failed'
    `).run();
    return Number(changes);
  }

  getJobsForExport(): ExportedJob[] {
    return this.db.prepare(
      "SELECT input, status, result FROM jobs ORDER BY id",
    ).all() as unknown as ExportedJob[];
  }

  getProgress(): QueueProgress {
    const progress: QueueProgress = { pending: 0, running: 0, completed: 0, failed: 0 };
    const rows = this.db.prepare("SELECT status, count(*) AS count FROM jobs GROUP BY status").all();
    for (const row of rows) {
      progress[row.status as keyof QueueProgress] = Number(row.count);
    }
    return progress;
  }

  recoverExpiredJobs(maxAttempts: number): void {
    this.db.prepare(`
      UPDATE jobs
      SET status = CASE WHEN attempts < ? THEN 'pending' ELSE 'failed' END,
          error = CASE WHEN status = 'running'
            THEN 'Job lease expired.' ELSE 'Job attempts exhausted.' END,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = unixepoch()
      WHERE (status = 'running' AND lease_until <= unixepoch())
         OR (status = 'pending' AND attempts >= ?)
    `).run(maxAttempts, maxAttempts);
  }

  renewLease(lease: JobLease, leaseSeconds: number): boolean {
    const { changes } = this.db.prepare(`
      UPDATE jobs
      SET lease_until = unixepoch() + ?,
          updated_at = unixepoch()
      WHERE id = ?
        AND status = 'running'
        AND lease_token = ?
    `).run(leaseSeconds, lease.id, lease.leaseToken);
    return changes === 1;
  }

  claimNextJob(maxAttempts: number, leaseSeconds: number): ClaimedJob | null {
    const row = this.db.prepare(`
      UPDATE jobs
      SET status = 'running',
          attempts = attempts + 1,
          lease_token = ?,
          lease_until = unixepoch() + ?,
          updated_at = unixepoch(),
          result = NULL,
          error = NULL
      WHERE id = (
        SELECT id
        FROM jobs
        WHERE status = 'pending'
          AND attempts < ?
        ORDER BY id
        LIMIT 1
      )
      RETURNING *
    `).get(randomUUID(), leaseSeconds, maxAttempts);

    return row ? (row as unknown as ClaimedJob) : null;
  }

  completeJob(lease: JobLease, result: string): boolean {
    const { changes } = this.db.prepare(`
      UPDATE jobs
      SET status = 'completed',
          result = ?,
          error = NULL,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = unixepoch()
      WHERE id = ?
        AND status = 'running'
        AND lease_token = ?
    `).run(result, lease.id, lease.leaseToken);

    return changes === 1;
  }

  releaseJob(lease: JobLease, error: string): boolean {
    const { changes } = this.db.prepare(`
      UPDATE jobs
      SET status = 'pending',
          attempts = attempts - 1,
          error = ?,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = unixepoch()
      WHERE id = ?
        AND status = 'running'
        AND lease_token = ?
    `).run(error, lease.id, lease.leaseToken);
    return changes === 1;
  }

  failJob(lease: JobLease, error: string): boolean {
    const { changes } = this.db.prepare(`
      UPDATE jobs
      SET status = 'failed',
          error = ?,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = unixepoch()
      WHERE id = ?
        AND status = 'running'
        AND lease_token = ?
    `).run(error, lease.id, lease.leaseToken);

    return changes === 1;
  }
}

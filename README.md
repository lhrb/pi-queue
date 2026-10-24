# pi-queue

A Pi extension for durable, SQLite-backed LLM job queues. Workers run isolated background subagents through `@gotgenes/pi-subagents`.

## Development

Requires Node.js 22.19.0 or newer and Pi.

```sh
npm install
npm test
npm run typecheck
pi install npm:@gotgenes/pi-subagents
pi
```

`.pi/settings.json` loads this package from the project directory after project trust approval. The package manifest loads only the queue extension. Install `@gotgenes/pi-subagents` separately, once, to avoid duplicate tool registrations. Codemode is enabled for batch insertion through `queue_add`.

To install from another directory:

```sh
pi install npm:@gotgenes/pi-subagents
pi install /absolute/path/to/pi-queue
```

## Usage

Run these commands inside Pi:

```text
/queue create tasks.db
/queue add tasks.db Summarize the retry policy in this repository.
/queue add tasks.db Identify missing queue tests.
/queue run tasks.db
/queue info tasks.db
/queue export tasks.db results.jsonl
```

Creation prompts for an available subagent name, a shared prompt, worker concurrency, and maximum attempts. Defaults: four workers and three attempts.

Other commands:

```text
/queue configure tasks.db
/queue run tasks.db 10
/queue reset tasks.db
/queue inspect tasks.db
/queue help
```

The optional run limit counts dispatches, including failures and retries. Reset returns failed jobs to pending. Inspect requires `sqlitebrowser`.

`queue_add` appends 1–100 literal inputs to an existing queue. Each batch is atomic and limited to 1 MiB of UTF-8 JSON. Duplicate inputs create separate jobs. Never automatically retry an uncertain insertion.

## Recovery

- SQLite persists jobs, settings, results, and errors.
- Lease tokens protect job ownership; active workers renew leases.
- Expired leases allow recovery after interruption, subject to the attempt limit.
- Transient provider failures pause dispatch with exponential backoff and recovery probes.
- Authentication and quota failures stop dispatch until a manual run.
- Job-specific failures remain failed until reset.
- Session shutdown aborts workers and preserves interrupted work for later runs.

Recovery can execute interrupted jobs again. Design tasks with repeatable or idempotent external effects.

Queue databases and SQLite sidecar files are excluded from Git.

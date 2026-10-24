# Project
TypeScript Pi extension for durable SQLite-backed job queues, with background subagent workers and p-queue concurrency.

## Commands
- Requires Node.js >=22.19.0 and Pi.
- Install: `npm install`
- Develop: `pi`
- Test: `npm test`
- Typecheck: `npm run typecheck`

## Layout
- `src/index.ts`: extension registration, dependency wiring, and session lifecycle.
- `src/commands.ts`: `/queue` command parsing and interactive handlers for tui.
- `src/tools.ts`: `queue_add` llm tools.
- `src/queue.ts`: queue operations, run orchestration, progress, and shutdown.
- `src/sqlite-store.ts`: SQLite persistence, transactions, job claims, and lease recovery.
- `src/JobDispatcher.ts`: concurrent dispatch, lease renewal, failure handling, and circuit-breaker backoff.
- `src/SubagentWorker.ts`: background subagent execution through the worker interface.
- `src/*.test.ts`: unit and integration tests.

## Conventions
- Follow existing patterns.
- Lead every behavior change with a test. Write and run it first; confirm the expected failure before implementing.
- Implement until the test passes; never weaken the test to accommodate incorrect behavior.

## Verification
- Run relevant tests and `npm run typecheck` before finishing.
- Report checks that could not run.

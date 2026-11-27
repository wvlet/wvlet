# ADR: Build models one at a time in shared code on npm and Native, and keep the flow executor on the JVM

Date: 2026-11-26 · PR #2537

## Context

- `wvlet build` needed the JVM CLI, though npm is the primary install. Phase 3 of [the CLI unification](../plans/2026-11-26-cli-unification.md) brings it to the npm and Native `wvlet`.
- The compiler already plans a build in shared code (`ModelBuildPlanner`): the materialized models in dependency order, each with one save statement. The JVM runs the plan as a flow on `FlowExecutor`: parallel stages on threads, timeouts, retries, run records in a file, SQLite, or Postgres store, JDBC connectors, and staging of tables read across engines. A build doesn't use the activity item store.
- On Node.js every query is synchronous on the event loop ([the shared REPL ADR](2026-11-26-shared-repl-across-platforms.md)), so stage parallelism and timeouts can't work there, and the run stores are JVM code.

## Decision

- `build` is a shared command in `wvlet-cli-core` with one option set (`WvletBuildOption`, including `-p`), and its planning (`ModelBuildPlan`: compile, select, upstream check, dry-run SQL, the default profile with the work folder's `target/warehouse`) is shared wvlet-runner code.
- The JVM plugs its flow-executor build into the `executeBuild` hook, unchanged. npm and Native use the shared `ModelBuilder`, which runs each model's save statement in build order on the caller through the same engine sessions and `SqlConnectorProvider` as `wvlet run`.
- `ModelBuilder` honours `engine:`, `retries`, `retry_delay`, `backoff`, and `max_retry_delay` (with the shared `StageExecutionConfig` and a synchronous `Sleep`), and skips the models downstream of a failed one. It warns about `timeout` instead of enforcing it, writes no run record, rejects `--run-store`, and fails when a model reads one built on another engine.

## Consequences

- npm and Native builds write the same tables, at the same `target/warehouse` path, and print the same `flow build` result as the JVM build. The differences are listed in `website/docs/syntax/model-pipelines.md`.
- A failed build now ends with a `QUERY_EXECUTION_FAILURE` error on every platform, so the JVM logs one more error line than before.
- Bringing parallel builds, timeouts, or run records to npm needs queries off the event loop, the same follow-up as REPL cancellation.

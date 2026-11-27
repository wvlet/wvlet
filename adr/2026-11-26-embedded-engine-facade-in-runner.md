# ADR: Put the embedded engine API in wvlet-runner, over the CLI's engine session

Date: 2026-11-26 · PR #2512

## Context

- Applications want to run Wvlet queries in-process from the JVM, and later from TypeScript, C, and Python, not only compile them to SQL.
- `WvletEngine` (wvlet-engine) executes typed relations. Compiling a query string, registering tables with compile-time schemas, and running `save to`, `create table`, and `test` statements live in wvlet-runner (`WvletEngineSession`, `PlanExecutor`), where the CLI's `wvlet` connector already uses them.
- Every binding should return the same rows and errors as the CLI, and the later bindings run on Scala.js and Scala Native.

## Decision

- Add `wvlet.lang.embedded.WvletEmbed` to wvlet-runner's shared code. It owns one `WvletEngineSession`, compiles against that session's catalog, and runs statements through a `PlanExecutor` handed the same session (`initialEngineSessions`).
- Bindings (libwvlet's C API, the Python and TypeScript SDKs) marshal arguments and `WvletEmbedResult.toJson`; they add no query logic of their own.
- Land the JVM API first; the C entry points follow as marshalling only.

## Consequences

- Statement support grows with the CLI's engine connector: a statement the CLI runs on the engine also runs embedded, and the gaps (`RunnerSpecWvletEngineBasic.knownGaps`) are shared.
- An embedding app links wvlet-runner, which also brings the connector framework and its dependencies, not only the engine.
- The JS SDK needs a bundle linked from `runner.js` rather than `lang.js`; keeping `WvletCompiler` small needs a second entry point.
- Exposing `WvletEngine` alone was rejected: bindings would re-implement compilation, registration, and statement execution, and drift from the CLI.

# Embedding the engine

This page owns the public API for running Wvlet queries in an application's process, and the result contract every language binding returns. The plan and order of the bindings are in [`plans/2026-11-26-embedded-engine.md`](../plans/2026-11-26-embedded-engine.md); why the facade lives in `wvlet-runner` is in [the ADR](../adr/2026-11-26-embedded-engine-facade-in-runner.md).

## API

- `wvlet.lang.embedded.EmbeddedEngine` (`wvlet-runner`, shared code, so it builds for JVM, Scala.js, and Scala Native) is the one entry point. It wraps a `WvletEngineSession` and a `PlanExecutor` on that session, the same path a `wvlet` connector without a host takes in the CLI.
- Its compiler uses the session's compile-time catalog as the default catalog, so registered Parquet tables are typed at compile time on every platform. `PlanExecutor`'s `initialEngineSessions` makes the executor run on that same session.
- Queries return every row (`rowLimit = Int.MaxValue`), not the CLI's 40-row display limit.
- `query` throws `WvletLangException` for a compile error, a failed statement, and a failed `test`; errors nested in `PlanResult` and `QueryResultList` are found by walking the results in statement order.
- Calls on one engine are serialized. User docs: [`website/docs/bindings/jvm.md`](../website/docs/bindings/jvm.md).

## Result contract

- `EmbeddedResult` holds columns (name, Wvlet type name from `DataType.wvExpr`) and rows by position. Values are the runner's external values (`ValueOps.toExternal`): decimals, dates, and timestamps as strings.
- `TableRows.positionalRows` carries rows by position when column names repeat, since its name-keyed maps keep only one value per name. The session fills it only then.
- `EmbeddedResult.toJson` is the document the C and SDK bindings return; change its shape only with a version bump of every binding.

## Gates

- `./sbt "runnerJVM/testOnly wvlet.lang.embedded.*"`: the API cases, the Java usage (`.jvm/src/test/java`, mirrored in the bindings page), and DuckDB conformance over spec/tpch queries on `spec/tpch/data`.
- A change to shared runner code compiles on `runnerJS/compile` and `runnerNative/compile`.

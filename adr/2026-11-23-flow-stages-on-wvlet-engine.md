# ADR: Flow stages run on the wvlet engine through a stage-engine interface

Date: 2026-11-23 · PR #2328

## Context

`FlowExecutor` ran every stage, the activity-result and notification tables, sensors, and run-table drops as SQL text on a JDBC `DBConnector`. `wvlet build` uses the same executor, and so does its upstream check (`select 1 ... limit 0`). The in-process wvlet engine ([ADR](2026-11-21-wvlet-engine-connector.md)) runs typed Wvlet plans, not SQL, so `wvlet build` and `wvlet flow` could not run on it, although it is the default of `wvlet run` ([ADR](2026-11-23-wvlet-engine-default.md)).

## Decision

- Stages go through `FlowStageEngine`, which has these operations:
  - create a table from a relation, run a `Save`, drop a table;
  - write rows;
  - check that a relation has rows (sensors), read a column (`for each` keys), check that a table exists.
- `SqlStageEngine` keeps the previous SQL statements. `WvletStageEngine` calls `WvletEngineSession` with plans. `FlowExecutor` takes any `Connector` and picks the implementation in `FlowStageEngine.of`.
- We didn't wrap the engine as a SQL `DBConnector` that re-parses the generated SQL: `DBConnector` is built on JDBC, and a SQL → plan round trip of GenSQL output is lossy.
- Statements on one engine session run one at a time (a lock on the session), because its catalog and writer don't support concurrent writes. An engine statement can't be cancelled mid-run. A no-op statement handle keeps heartbeats working, and a timeout still fails the attempt.
- Activations and flow notifications fail with `NOT_IMPLEMENTED` on the engine, because `ActivationRequest` carries a `DBConnector` that sinks read with SQL. So do view models (the engine has no views) and staging tables of other connectors.
- `wvlet build` and `wvlet flow` gain `-t/--target` (like `wv`), which picks `Profile.defaultProfileFor(<engine>)`. The defaults stay on DuckDB.

## Consequences

- `wvlet build -t wvlet` and `wvlet flow run F -t wvlet` work without DuckDB. Built tables persist across runs only when the `wvlet` connector has a `warehouse` folder.
- Parallel stages on the engine are serialized, so a stage's timeout also counts time spent waiting for the session.
- Switching the defaults needs activation sinks that read engine tables. Then flows embedded in `wvlet run` (`QueryExecutor`) move to the engine, and the default location of persisted tables is decided.

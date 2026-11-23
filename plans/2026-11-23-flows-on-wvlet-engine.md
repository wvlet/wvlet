# Run `wvlet build` and flows on the wvlet engine

## Goals

- Let `wvlet build` and `wvlet flow` run on the in-process `wvlet` engine, so the CLI's last DuckDB-only commands stop needing DuckDB.
- Then make the engine their default, as `wvlet run` and `wv` already do (#2316).

## Background

- `FlowExecutor` drives a stage entirely through SQL text on a `DBConnector`, which is JDBC-based:
  - query stages run `create or replace table "<run table>" as <GenSQL>`;
  - model stages from `wvlet build` run `GenSQL.generateSaveSQL`;
  - activity results and the `on_failure` run summary become tables via `values (...)` / `insert`;
  - run tables are dropped with `drop table`, also by `FlowRunRetention`;
  - `checkUpstreamsBuilt` probes `select 1 from t limit 0`.
- The engine (`WvletEngineSession`) runs typed plans, not SQL: `execute(Relation)`, `save(Save)`, `ddl(statement)`. So it can't simply be passed in as a `DBConnector`.
- Activation sinks get the stage table through `ActivationRequest.connector: DBConnector` and read it with SQL. That typed field is what blocks activations on the engine. Changing it touches `FileActivationSink`, which Step 2 (another session) is rewriting now, along with `SourceTableStaging` and `QueryExecutor`.
- Other `DBConnector` users in flows: `wait until` sensors (`queryJsonRows`), stage cancel and timeout (`executeCancellable`), `FlowRunRetention` and `flow session clean` (`drop table`), and `backfill` / `scheduler` in `WvletFlowCommand`. Flows embedded in `wvlet run` go through `QueryExecutor`.
- `wvlet build` and `wvlet flow` have no `-t` option today; only `--profile` selects the engine.
- The engine has no views yet. `materialized: 'view'` models can't be built on it.
- Today's default (`generic`, in-memory DuckDB) doesn't persist built tables across runs either. An engine connector with a `warehouse` folder does (Iceberg).

## Design

Lane: Standard. This changes which engine runs user flows and builds, and it is split into two PRs.

**This PR: engine support, defaults unchanged**

- Add a small stage-engine interface inside the runner that covers what the executor needs:
  - materialize a relation into a table;
  - run a `Save`;
  - drop a table;
  - write rows into a table;
  - query rows (for sensors);
  - check that a table exists.
- It has two implementations: SQL on a `DBConnector` (today's code, moved as is) and plans on a `WvletEngineSession`.
- `FlowExecutor` gets its default engine and each `on <connector>` engine through this interface. An in-process `wvlet` connector (no host) resolves to the engine. Any other connector resolves to SQL as today.
- This covers every `wvlet flow` subcommand (`run`, `resume`, `backfill`, `scheduler`, `session clean`), `wvlet build`, and the server's flow API through `FlowRunLauncher`.
- Engine stages can't be stopped mid-statement. A timeout or cancel still marks the stage, and the statement finishes in the background.
- Add `-t` / `--target` to the `build` and `flow` commands, as `wv` got in #2316, so `-t wvlet` and `-t duckdb` work without a profile file.
- `wvlet build -t wvlet` (or a profile with an in-process `wvlet` connector):
  - models compile for `DBType.Wvlet` and save into engine tables (Iceberg when the connector has a `warehouse`);
  - the upstream check asks the engine's catalog instead of sending SQL.
- Engine stages fail with `NOT_IMPLEMENTED` and a clear message for what still needs Step 2 or later work:
  - activations and `on_*` notifications, until `ActivationRequest` can carry the engine;
  - reading another connector's table (staging);
  - view models.
- `QueryExecutor` is left alone, so flows embedded in `wvlet run` stay on the SQL path until the follow-up.
- Tests run the core cases of `FlowExecutorTest`, `WvletFlowCommandTest`, and `WvletBuildCommandTest` on an engine profile next to the DuckDB runs: query stages, routes and merge, resume, activity stages, sensors, and table models. `ModelBuildConformanceTest` is a compiler-only test and stays as is.

**Follow-up PR: switch the defaults**

- This PR waits for Step 2 to merge. Then it lets `ActivationRequest` carry the engine, which enables activations and notifications, and moves `QueryExecutor`'s embedded flows to the engine.
- `wvlet build` and the flow commands default to `Profile.defaultWvletProfile`. `ModelBuildLauncher.plan` defaults to `DBType.Wvlet`. `-t duckdb` keeps working.
- That PR also updates the ADR 2026-11-23-wvlet-engine-default, `syntax/flow.md`, `model-pipelines.md`, `for-ai-agents.md`, and the bundled skill.

## Alternatives and Why Not?

- **Wrap the engine as a SQL `DBConnector` by re-parsing the generated SQL.** `DBConnector` is built on JDBC, and a SQL → plan round trip of GenSQL output is lossy and fragile.
- **Switch the defaults in this PR.** Flows with activations or view models would break until Step 2 lands. A silent DuckDB fallback is ruled out by the engine-default ADR.
- **Default `wvlet build` to a `.wvlet/warehouse` folder.** It would change where data is stored, an always-gate decision. It belongs to the default-switch PR, if anywhere.

## Follow-up decision (from the direction card)

- Persistence default: the user asks for a defined target location. They doubt `.wvlet` is right and suggest `<project>/target` or `<project>/warehouse`. Decide in the default-switch PR.

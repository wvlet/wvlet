# `wvlet build` on npm and Native (CLI unification phase 3)

## Goals

- npm and Native `wvlet` users can build their materialized models with `wvlet build`, with the same options and the same result tables as the JVM CLI (phase 3 of plans/2026-11-26-cli-unification.md).
- A project built with npm `wvlet build` persists the same tables, at the same paths, as the JVM build.
- The JVM `wvlet build` keeps its behaviour unchanged.

## Background

- The compiler already plans a build in shared code: `ModelBuildPlanner` (wvlet-lang) collects the `table` and `view` models, orders them by dependency, and gives each one save statement (`save to <target>` or `create or replace view`).
- The JVM CLI runs that plan as a flow on `FlowExecutor` (about 1,800 lines, JVM-only): stages run in parallel on threads, with retries and timeouts, a run record in `target/flow-runs` (file, SQLite, or Postgres store), JDBC connectors, and staging of tables read across engines. A build does not use the activity item store.
- The default engine of `wvlet build` is the in-process wvlet engine with a `target/warehouse` folder, where tables persist as Iceberg tables; `view` models need `-t duckdb`. The engine and its warehouse are shared code and run on Node and Native.
- npm and Native already run statements through the shared `PlanExecutor` over `SqlConnectorProvider` (wvlet engine, DuckDB, Trino). Every query on Node is synchronous; uni's `Sleep` waits synchronously on every platform.

## Design

Lane: Standard — new user-facing command on npm and Native, and it writes data to a storage path on those platforms.

User flow (npm or Native):

- The user has a folder of `.wv` files with `model m with { materialized: 'table' } = { ... }` and runs `wvlet build` (or `wvlet build -s m+`, `-t duckdb`, `-p <profile>`, `--dry-run`) in it.
- Each selected model is built in dependency order, and a table prints one row per model: model, target, state, and elapsed time, as on the JVM. A failed model exits non-zero; its downstream models are skipped.
- To query the built tables, the user runs `wvlet run` or the REPL with a profile whose `wvlet` connector sets `warehouse: target/warehouse` (or a DuckDB profile with a database `path`), as on the JVM today: `run` and the REPL don't add the build's default warehouse, and `-t duckdb` builds into an in-memory DuckDB that is gone when the command exits.
- To rebuild, run `wvlet build` again; tables are replaced, views are recreated.

Decisions:

- `build` moves to the shared command surface in `wvlet-cli-core` with one option set: `-w`, `-s,--select`, `--dry-run`, `-p,--profile`, `-t`, `--catalog`, `--schema`, and the JVM's `--run-store`. The JVM CLI plugs in its existing flow-executor path through a hook, so its output, run records, and connectors stay unchanged.
  - `-p` is new for the JVM `build`, completing phase 1's "`-p` everywhere"; `wvlet flow` keeps its own options.
  - `--run-store` stays in the shared option set (a command method has one fixed signature), and npm and Native reject it with an error naming the JVM CLI.
- Build planning (compile the work folder, select models, resolve the default profile with its `target/warehouse`, check that unselected upstreams are built, generate the dry-run SQL) moves from the JVM `ModelBuildLauncher` and `WvletFlowOption` into shared wvlet-runner code, used by both paths. Paths go through uni `IO`, and the upstream check probes tables through the shared engine sessions and `SqlConnectorProvider`.
- npm and Native run a new sequential builder in shared code: each model's save statement runs on the caller, one model at a time, through the same engine sessions and connectors as `wvlet run`.
  - The default profile resolves as on the JVM: the in-process wvlet engine with the work folder's warehouse folder (`<work>/target/warehouse`, or `~/.cache/wvlet/target/warehouse` when the folder has no `.wv` file directly in it, per `WorkEnv`). This is the JVM build's existing path, now written by npm and Native too: an always-gate storage path, confirmed on the card.
  - A model's `engine:` picks a profile connector, as on the JVM. A model reading a model built on another engine fails with an error naming the JVM CLI, which stages such tables.
  - `retries`, `retry_delay`, `backoff`, and `max_retry_delay` apply as on the JVM, waiting with uni's synchronous `Sleep`. `timeout` is not enforced, since a Node query can't be interrupted; the build logs a warning when a model sets it. So a model with `timeout` and `retries` doesn't retry a slow attempt as the JVM does.
- npm and Native write no flow run record: there is no `wvlet flow` there to read it, and the default file store is JVM code. `--run-store` on npm and Native fails with an error naming the JVM CLI.
- Docs drop "JVM CLI only" for `build`: the `usage/install.md` table, `usage/cli-reference.md`, `for-ai-agents.md`, the bundled skill, `sdks/cli-node/README.md`, and `syntax/model-pipelines.md` (also adding `-p`), which lists what the npm/Native build lacks (parallel models, timeouts and timeout retries, run records, cross-engine reads, Snowflake and other JDBC connectors).
- Tests: the existing JVM build tests pass unchanged; new shared tests (JVM and Node) build table models on the wvlet engine into `target/warehouse`, read them back with a later session, cover `-s` selection with the upstream check, `--dry-run`, a failing model skipping its downstream, and `--run-store` rejection.

## Alternatives and Why Not?

- Port `FlowExecutor` to shared code: threads, JDBC connectors, three run stores, and cross-engine staging, for parallelism that a synchronous Node runtime can't use.
- Run the JVM build on the new sequential builder too: it would drop parallel stages, timeouts, run records, and Snowflake from the JVM build.
- Write JSON run records on npm/Native too: nothing on those platforms reads them, and it adds a storage path to maintain.
- Keep `build` as a JVM-only command and add a separate npm one: the option sets would drift again, which phase 1 removed.

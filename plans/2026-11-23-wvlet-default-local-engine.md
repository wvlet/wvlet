# wvlet engine as the default local engine

## Goals
- Queries run with no `-t` / `--profile` execute on the in-process `wvlet` engine instead of DuckDB, so the CLI no longer needs DuckDB for everyday local work (first step of dropping DuckDB from the shipped CLI).
- Users who need DuckDB keep it with one flag or profile setting.

## Background
- The `wvlet` connector without a host already runs on the engine (ADR 2026-11-21-wvlet-engine-connector); it passes TPC-H 22/22, TPC-DS 99/99, and all but 6 DuckDB-catalog specs in spec/basic.
- Today the fallback is DuckDB in `WvletCli` (npm/native `run`), the JVM `wvlet run` (`WvletCompiler`), the `wv` REPL, and `wvlet build`. On top of that, `QueryExecutor` auto-switches a pure local-file read to a scratch DuckDB even when the profile engine is something else.
- The engine follows DuckDB function semantics (`DBType.Wvlet` picks DuckDB stdlib variants), so result values do not change for supported queries.
- Parquet export, the activation sink, and source staging still use DuckDB as a utility; they move in step 2.

## Design
- Lane: Standard, pre-approved by the work item and the goal's recorded merge authorization.
- No `-t` and no `--profile` → a `local` profile with one `wvlet` connector (no host) for `wvlet run`, `wv`, and the npm/native `wvlet run`. `wv` gets `-t` so `wv -t duckdb` opts out without a profile.
- `wvlet build` / `wvlet flow` stay on DuckDB: `FlowExecutor` needs a SQL connector, which the engine is not; moving flows is a follow-up.
- `-t duckdb` and profiles with `type: duckdb` behave exactly as before.
- `wvlet compile` / `to_wvlet` with no `-t` keep the DuckDB SQL dialect: they print SQL for some engine and never execute it, so DuckDB SQL stays the most useful default output.
- The local-file auto-switch stays but targets the in-process engine: a query that reads only local files on a Trino/Snowflake profile runs on the engine instead of a scratch DuckDB; a DuckDB profile keeps running it on DuckDB.
- Server UI and `wvlet catalog` keep DuckDB for now (later step).
- Migration in the ADR: `sql"..."` / `execute` statements, `.duckdb` files, DuckDB extensions, and DuckDB-only functions need `-t duckdb` or a `type: duckdb` profile; the engine reports NOT_IMPLEMENTED for SQL statements instead of guessing.
- Tests: CLI tests that relied on the DuckDB default either pin `-t duckdb` or check the engine result; new tests cover the default engine for run and the REPL, and the local-file switch on a Trino profile.

## Alternatives and Why Not?
- Drop the auto-switch: local-file reads on a Trino profile would then be sent to Trino, which cannot see local files.
- Change `compile` default to `-t wvlet`: its SQL is DuckDB-flavoured anyway but labelled with a dialect no external engine runs.

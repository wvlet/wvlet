# ADR: The in-process wvlet engine is the default for running queries

Date: 2026-11-23 · PR #2316

## Context

The CLI ran every query on DuckDB unless `-t` or `--profile` said otherwise, and `QueryExecutor` sent a query reading only local files to a scratch DuckDB even when the profile's engine was Trino or Snowflake. The in-process `wvlet` engine ([ADR](2026-11-21-wvlet-engine-connector.md)) now passes TPC-H 22/22, TPC-DS 99/99, and all but 6 DuckDB-catalog specs in spec/basic. DuckDB's JDBC jar (~81 MB) is the largest part of the CLI distribution, and the native CLI links libduckdb even for commands that never use it. Making the engine the default is the first step to shipping the CLI without DuckDB.

## Decision

- With neither `-t` nor `--profile`, `wvlet run` (JVM, npm, and native) and the `wv` REPL use `Profile.defaultWvletProfile`: one `wvlet` connector without a host. `wv` gains `-t` / `--target` so `wv -t duckdb` opts back in without a profile file.
- `-t duckdb` and profiles with `type: duckdb` behave as before. Nothing is translated or retried: a query that the engine can't run fails with the engine's error (`NOT_IMPLEMENTED` for SQL statements), so users see where they need DuckDB instead of getting a silent fallback.
- `wvlet compile` and `to_wvlet` keep the DuckDB SQL dialect as their default. They print SQL for an external engine and never execute it, so the default output should be SQL that DuckDB, the most common target, runs as is.
- `wvlet build` and `wvlet flow` keep DuckDB for now: `FlowExecutor` runs stages on a SQL connector (`DBConnector`), which the engine isn't. Moving flows onto the engine is a later step of the same goal.
- The local-file switch now targets the engine: on a profile whose engine is not DuckDB (or the generic DuckDB fallback), a query whose only inputs are local files runs on the in-process engine. A DuckDB profile reads local files on DuckDB itself.

## Consequences

- Everyday local queries (`wvlet run "from 'data.parquet' ..."`, `wv`) need no DuckDB.
- Migration for DuckDB-only users: `sql"..."` / `execute` statements, `.duckdb` database files, DuckDB extensions, table functions such as `read_csv(...)`, and DuckDB functions the engine lacks need `-t duckdb` (or `wv -t duckdb`) or a profile with `"type": "duckdb"`. The engine reports such statements as errors that name the unsupported feature.
- Parquet export, the activation sink handoff, and source table staging still use DuckDB as a utility, and the server UI and `wvlet catalog` still default to DuckDB; later steps move them before DuckDB leaves the distribution.

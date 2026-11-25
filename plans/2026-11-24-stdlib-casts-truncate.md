# Stdlib casts: to_int / to_long truncate, numeric to_boolean

## Goals
- `x.to_int` / `x.to_long` on a fractional value truncate toward zero on every engine (2.5 → 2, -2.5 → -2), as the naming ADR records (user decision, 2026-11-23).
- Numeric `x.to_boolean` works on every engine, including the wvlet engine, where it failed.

## Background
- Today the variants are a plain `cast(x as bigint)`: DuckDB rounds doubles half to even and decimals half away from zero; Trino, Snowflake, and BigQuery round half away from zero; Hive truncates. The same query gives different integers per engine.
- The engine's double → bigint cast rounded half up (2.5 → 3, -2.5 → -2), unlike DuckDB (2, -2), and its boolean cast rejected doubles and decimals.

## Design
- Lane: **Express**, because the user already chose the behaviour, the change is small, and it touches no always-gate area.
- double, float, and decimal `to_int` / `to_long`: truncate, then cast (`trunc` on DuckDB, Snowflake, and BigQuery; `truncate` on Trino). Hive keeps its plain cast, which already truncates.
- `to_boolean` on these types: non-zero is true, NaN included, as DuckDB does. BigQuery casts only integers to bool, so its variant compares with 0.
- Engine: double → bigint casts round half to even and reject out-of-range values, as DuckDB does. The boolean cast accepts doubles and decimals. These are fixes to the engine's own DuckDB-compatible casts.
- Tests: an engine-vs-duckdb_jdbc check for the stdlib casts and plain `::bigint` casts; a spec that asserts the truncated values on DuckDB and the engine; and per-dialect SQL assertions.

## Out of scope
- `string.to_int('2.5')` and `any.to_int`: the receiver may not be numeric, so `trunc` can't wrap it. DuckDB gives 3; that stays.

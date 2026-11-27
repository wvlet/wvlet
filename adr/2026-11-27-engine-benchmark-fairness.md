# ADR: Compare the engine with DuckDB at steady state, with the same caches on both sides

Date: 2026-11-27 · PR #2546

## Context

- The SF10 dashboard put the engine at about DuckDB's speed on TPC-DS, and the docs were about to say so. An audit of `EngineTPCHBench` / `EngineTPCDSBench` found that DuckDB 2.0.1 ran with `parquet_metadata_cache = false`, so it re-read every Parquet footer per query while the engine's footer cache is on. It also found that every DuckDB run came before every engine run, and that DuckDB boxed every result value over JDBC while the engine only counted rows.
- Two of the engine's advantages have no DuckDB counterpart over Parquet files: per-file statistics kept across queries (distinct counts, value samples, much like `ANALYZE`), and the TPC keys declared to the engine. At SF10, recomputing the statistics for every query made the engine 3.0x slower on TPC-DS and 1.9x slower on TPC-H, and removing the keys made it 4–7% slower.

## Decision

- Publish steady-state numbers: the same warm-up, then alternating timed runs, with the cross-query caches each engine has turned on for both. DuckDB gets its footer cache and `preserve_insertion_order = false`; the engine's caches stay on.
- The engine reads every result value, as DuckDB's JDBC result does.
- Disclose, rather than remove, what only the engine has (file statistics, declared keys, DuckDB's JDBC transfer of large results), with the measured effect of each in the performance page and the dashboard. The bench has switches to time without them: `WVLET_ENGINE_BENCH_COLD=1`, `WVLET_ENGINE_BENCH_KEYS=none`, and `WVLET_ENGINE_BENCH_DUCKDB_DEFAULTS=1` for the old DuckDB setup.

## Consequences

- From this PR on, DuckDB is about 5–6% faster in the benchmark, so the engine's ratios rise by about that much. The dashboard notes the change.
- A first query over files the engine hasn't seen pays for its statistics, and steady-state numbers don't show that. Each run records both engines' first-run time (`*_first_ms`) so it stays visible.

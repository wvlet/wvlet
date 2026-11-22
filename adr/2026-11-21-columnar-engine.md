# ADR: An in-process columnar engine that executes Wvlet plans

Date: 2026-11-21 · PR #TBD

## Context

Every Wvlet query ran on an external engine through generated SQL (DuckDB via JDBC, the libduckdb C API, or koffi; Trino over REST). Scala.js and Scala Native depend on a native libduckdb, and Parquet/Iceberg access needed DuckDB or parquet-java/Hadoop. uni 2026.3.0 ships pure-Scala Parquet, Parquet dataset, and Iceberg readers on all three platforms, which removes the storage-layer reason for an external engine.

## Decision

- Add `wvlet-engine`, a cross-platform (JVM / JS / Native) vectorized engine that executes the typed, model-expanded `Relation` the runner already holds, instead of SQL text. SQL input works too, since `SqlParser` produces the same plan nodes.
- Column names resolve against the operators' runtime schemas, not compiler types, so files with unknown schemas plan the same way as catalog tables.
- Stdlib calls reach the plan as `sql"..."` templates in DuckDB's dialect; the engine parses them back into expressions (`Binder.expandTemplates`) and implements DuckDB-compatible function semantics.
- Subqueries are decorrelated at planning time: EXISTS / IN become semi and anti joins, correlated scalar subqueries become a grouped aggregation joined to the outer input. Inner joins and WHERE conjuncts are planned together as a join graph, and common conjuncts are factored out of OR branches.
- DuckDB is the oracle: `EngineConformance` runs each spec query on both and compares the rows as multisets.

## Consequences

- TPC-H 22/22 matches DuckDB on `spec/tpch/data`; `spec/basic` starts at 115/162, with the rest listed as known gaps in `EngineBasicSpecTest` (the list only shrinks).
- The engine is not wired into the CLI or runner yet; that's the next step (a `wvlet` connector type).
- No parallelism, column pruning, or Parquet filter pushdown yet, so performance work is still to come (see `plans/2026-11-21-columnar-engine.md`).

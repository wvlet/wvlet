package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.RelationType

/**
  * Cross-platform DuckDB facade. Concrete file/system access lives in the per-platform
  * [[DuckDBCompat]] trait:
  *   - JVM backs the methods with the DuckDB JDBC driver
  *   - Scala.js (Node.js) and Scala Native currently throw `UnsupportedOperationException` — plug
  *     points are in place so a future PR can wire `@duckdb/node-api` (Node) and `libduckdb` C
  *     bindings (Native) without changing call sites.
  *
  * Used by `DuckDBAnalyzer` for non-JSON file schema inference (parquet, csv, …). Could grow to
  * back actual query execution on Node/Native once the platform implementations land.
  */
object DuckDB extends DuckDBCompat

package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.RelationType

/**
  * Cross-platform DuckDB facade. Concrete file/system access lives in the per-platform
  * [[DuckDBCompat]] trait:
  *   - JVM backs the methods with the DuckDB JDBC driver
  *   - Scala.js (Node.js) currently throws `UnsupportedOperationException` — plug point is in place
  *     so a future PR can wire `@duckdb/node-api` without changing call sites
  *   - Scala Native uses libduckdb's C API
  *
  * Used by `DuckDBAnalyzer` for non-JSON file schema inference (parquet, csv, …). Could grow to
  * back actual query execution on Node/Native once the platform implementations land.
  */
object DuckDB extends DuckDBCompat:

  /**
    * Escape a string for safe inlining into a DuckDB SQL string literal (the part between single
    * quotes). DuckDB doesn't parameterize file paths in `FROM` clauses, so we have to inline them;
    * this helper at least neutralizes embedded single quotes by doubling them (`O'Reilly.parquet` →
    * `O''Reilly.parquet`). The caller is still expected to wrap the result in single quotes.
    */
  def escapeSqlString(s: String): String = s.replace("'", "''")

end DuckDB

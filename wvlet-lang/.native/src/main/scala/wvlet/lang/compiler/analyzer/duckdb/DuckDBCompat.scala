package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.RelationType

/**
  * Stub DuckDB backend for Scala Native.
  *
  * A future PR will replace this with `extern` bindings to `libduckdb` (DuckDB's C API). Until
  * then, parquet/csv schema inference on Native surfaces a clear `UnsupportedOperationException`
  * instead of a `???` runtime error. JSON files do not hit this path — `DuckDBAnalyzer` routes them
  * through the cross-platform `JSONAnalyzer`.
  */
trait DuckDBCompat:
  def isAvailable: Boolean = false

  def schemaOf(path: String): RelationType =
    throw new UnsupportedOperationException(
      s"DuckDB-backed schema inference is not yet wired on Scala Native (path: ${path})"
    )

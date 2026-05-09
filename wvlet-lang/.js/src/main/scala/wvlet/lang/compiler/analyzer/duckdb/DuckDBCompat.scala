package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.RelationType

/**
  * Stub DuckDB backend for Scala.js (Node.js).
  *
  * A future PR will replace this with a real implementation backed by `@duckdb/node-api` (the
  * official Node.js DuckDB binding). Until then, parquet/csv schema inference on Node surfaces a
  * clear `UnsupportedOperationException` instead of a `???` runtime error. JSON files do not hit
  * this path — `DuckDBAnalyzer` routes them through the cross-platform `JSONAnalyzer`.
  */
trait DuckDBCompat:
  def isAvailable: Boolean = false

  def schemaOf(path: String): RelationType =
    throw new UnsupportedOperationException(
      s"DuckDB-backed schema inference is not yet wired on Scala.js (path: ${path})"
    )

package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.model.DataType.NamedType

/**
  * Cross-platform query result shape. Values are kept as `Option[String]` so backends don't have to
  * agree on a typed value representation up front — the JDBC, libduckdb, and koffi-FFI backends
  * each call the DuckDB engine's string-coercion (`duckdb_value_varchar` / `ResultSet.getString`)
  * and hand us the same surface. `None` represents SQL `NULL`.
  *
  * Good enough for a CLI runner that prints to stdout / json / a box-drawn table. If we later need
  * typed access (date arithmetic, decimal math) we'd switch to a `Seq[Any]` plus the existing wvlet
  * `DataType` machinery — for now strings keep all three backends consistent.
  */
case class QueryResultRow(values: Seq[Option[String]])

case class QueryResult(columns: Seq[NamedType], rows: Seq[QueryResultRow]):
  def rowCount: Int    = rows.size
  def columnCount: Int = columns.size

package wvlet.lang.compiler.analyzer.duckdb

import org.duckdb.DuckDBConnection
import wvlet.uni.control.Control.withResource
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.connector.QueryResultRow
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType

import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet

trait DuckDBCompat:
  def isAvailable: Boolean = true
  def canExecute: Boolean  = true

  def schemaOf(path: String): RelationType =
    if !File(path).isFile then
      EmptyRelationType
    else
      // File paths cannot be parameterized in DuckDB FROM clauses, so we inline. The
      // File.isFile() check above pins the path to a real file, and `DuckDB.escapeSqlString`
      // doubles single quotes so paths like `O'Reilly.parquet` produce valid SQL.
      val sql = s"select * from '${DuckDB.escapeSqlString(path)}' limit 0"
      withConnection { conn =>
        withResource(conn.createStatement().executeQuery(sql)) { rs =>
          val metadata = rs.getMetaData
          val columns  = (1 to metadata.getColumnCount)
            .map { i =>
              val name     = metadata.getColumnName(i)
              val dataType = metadata.getColumnTypeName(i).toLowerCase
              // TODO support non-primitive type parsing
              NamedType(Name.termName(name), DataType.parse(dataType))
            }
            .toList
          SchemaType(None, Name.typeName(RelationType.newRelationTypeName), columns)
        }
      }

  /**
    * Run `sql` against a fresh in-memory DuckDB and return all rows materialized as strings. Values
    * come back via `rs.getString(col)` — same string-coercion the JS/Native backends get from
    * `duckdb_value_varchar`, so cross-platform output is consistent.
    *
    * Statements without a ResultSet (e.g. `COPY ... TO`) are reported as a single `Count` row,
    * matching the result shape DuckDB's C API gives the JS/Native backends.
    */
  def execute(sql: String): QueryResult = withConnection { conn =>
    withResource(conn.createStatement()) { stmt =>
      // JDBC's executeQuery() rejects statements that produce no ResultSet, so dispatch on
      // execute() and fall back to the update count for COPY/DDL statements.
      if stmt.execute(sql) then
        withResource(stmt.getResultSet)(readResult)
      else
        val count = stmt.getUpdateCount
        if count < 0 then
          QueryResult(Nil, Nil)
        else
          QueryResult(
            List(NamedType(Name.termName("Count"), DataType.LongType)),
            List(QueryResultRow(List(Some(count.toString))))
          )
    }
  }

  private def readResult(rs: ResultSet): QueryResult =
    val metadata = rs.getMetaData
    val colCount = metadata.getColumnCount
    val columns  = (1 to colCount)
      .map { i =>
        val name     = metadata.getColumnName(i)
        val dataType = metadata.getColumnTypeName(i).toLowerCase
        NamedType(Name.termName(name), DataType.parse(dataType))
      }
      .toList

    val rows = List.newBuilder[QueryResultRow]
    while rs.next() do
      val values = (1 to colCount)
        .map { i =>
          val v = rs.getString(i)
          if rs.wasNull() then
            None
          else
            Option(v)
        }
        .toList
      rows += QueryResultRow(values)
    QueryResult(columns, rows.result())

  private def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        withResource(conn)(f)
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

end DuckDBCompat

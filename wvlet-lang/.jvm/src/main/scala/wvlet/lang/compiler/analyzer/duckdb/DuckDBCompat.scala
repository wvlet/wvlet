package wvlet.lang.compiler.analyzer.duckdb

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
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

/**
  * JVM DuckDB backend over the DuckDB JDBC driver. The driver is optional: wvlet-lang does not
  * depend on it, and it is on the classpath only when wvlet-connector-duckdb is (the full CLI, or
  * tests). Without it, [[isAvailable]] and [[canExecute]] report `false`, like Native without
  * libduckdb, and file references are left for the wvlet engine to read.
  */
trait DuckDBCompat:
  private lazy val driverLoaded: Boolean =
    try
      Class.forName(DuckDBCompat.driverClassName)
      true
    catch
      case _: ClassNotFoundException | _: LinkageError =>
        false

  def isAvailable: Boolean = driverLoaded
  def canExecute: Boolean  = driverLoaded

  /** Why DuckDB is unavailable */
  def notAvailableMessage: String =
    "DuckDB is not available: the DuckDB JDBC driver is not on the classpath. Install the " +
      "full wvlet CLI, or copy wvlet-connector-duckdb and duckdb_jdbc into the lib folder of the CLI"

  def schemaOf(path: String): RelationType =
    if !File(path).isFile then
      EmptyRelationType
    else
      // File paths cannot be parameterized in DuckDB FROM clauses, so we inline. The
      // File.isFile() check above pins the path to a real file, and `DuckDB.escapeSqlString`
      // doubles single quotes so paths like `O'Reilly.parquet` produce valid SQL.
      val sql = s"select * from '${DuckDB.escapeSqlString(path)}' limit 0"
      withConnection { conn =>
        // Close the Statement too — wrapping only the ResultSet leaks the statement handle
        // (and its server-side cursor) until the connection itself is closed (#1699)
        withResource(conn.createStatement()) { stmt =>
          withResource(stmt.executeQuery(sql)) { rs =>
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
      executeStatement(stmt, sql)
    }
  }

  /**
    * Run `sql` on a caller-provided JDBC statement and materialize the result. Shared by the
    * fresh-connection [[execute]] above and the runner's stateful `DuckDBConnector.asSqlConnector`
    * view, which supplies a statement bound to its long-lived connection so in-memory tables
    * persist across calls.
    */
  private[lang] def executeStatement(stmt: java.sql.Statement, sql: String): QueryResult =
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

  /**
    * Open a persistent session — a single long-lived JDBC connection that every
    * `session.execute(sql)` runs on, so in-memory tables and other session state survive across
    * calls. `path` opens a file-backed database; `None` opens an in-memory one.
    */
  def newSession(path: Option[String] = None): DuckDBSession =
    val conn = openConnection(path)
    new DuckDBSession:
      private var closed = false

      override def execute(sql: String): QueryResult =
        if closed then
          throw new IllegalStateException("DuckDB session is already closed")
        withResource(conn.createStatement()) { stmt =>
          executeStatement(stmt, sql)
        }

      override def close(): Unit =
        if !closed then
          closed = true
          conn.close()

  private def withConnection[U](f: Connection => U): U = withResource(openConnection(None))(f)

  private def openConnection(path: Option[String]): Connection =
    if !driverLoaded then
      throw StatusCode.CONNECTOR_NOT_INSTALLED.newException(notAvailableMessage)
    DriverManager.getConnection(s"jdbc:duckdb:${path.getOrElse("")}")

end DuckDBCompat

object DuckDBCompat:
  private val driverClassName = "org.duckdb.DuckDBDriver"

package wvlet.lang.compiler.analyzer.duckdb

import org.duckdb.DuckDBConnection
import wvlet.uni.control.Control.withResource
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType

import java.io.File
import java.sql.DriverManager

trait DuckDBCompat:
  def isAvailable: Boolean = true

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

  private def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        withResource(conn)(f)
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

end DuckDBCompat

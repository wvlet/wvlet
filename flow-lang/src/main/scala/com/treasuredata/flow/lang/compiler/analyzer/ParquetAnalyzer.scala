package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType}
import com.treasuredata.flow.lang.model.expr.Name
import com.treasuredata.flow.lang.model.{DataType, RelationType}
import org.duckdb.DuckDBConnection
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource

import java.sql.DriverManager

object ParquetAnalyzer:

  private def withConnection[U](f: DuckDBConnection => U): U =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        try f(conn)
        finally conn.close()
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

  def guessSchema(path: String): RelationType =
    // Use DuckDB to analyze the schema of the Parquet file
    val sql = s"select * from '${path}' limit 0"

    withConnection { conn =>
      withResource(conn.createStatement().executeQuery(sql)) { rs =>
        val metadata = rs.getMetaData
        val columns = (1 to metadata.getColumnCount).map { i =>
          val name     = metadata.getColumnName(i)
          val dataType = metadata.getColumnTypeName(i).toLowerCase
          // TODO support non-primitive type parsing
          NamedType(Name.fromString(name), DataType.getPrimitiveType(dataType))
        }
        SchemaType(None, RelationType.newRelationTypeName, columns, Nil)
      }
    }

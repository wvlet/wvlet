/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.connector

import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.connector.DBConnector
import wvlet.lang.engine.catalog.DataFileWriter
import wvlet.lang.engine.catalog.StringRows
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.Schema
import wvlet.lang.model.DataType

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types
import scala.util.Try

/**
  * Writes a query result of an engine that can't write local files itself (e.g. Trino, Snowflake)
  * to a local file with the wvlet engine's writers (Parquet, CSV, TSV, JSON lines). Rows stream
  * page by page, so memory stays bounded by the source's page size
  */
object LocalFileExport:

  // Rows per batch read from a JDBC result set
  private val jdbcBatchSize = 4096

  /**
    * Run `sql` on `connector` and write all result rows to `path`. `options` are the file writer
    * options (`format`, `compression`, `header`, ...). An empty result still writes a file carrying
    * the result schema (e.g. a header-only CSV). Returns the number of written rows
    */
  def write(connector: DBConnector, sql: String, path: String, options: Map[String, Any])(using
      QueryProgressMonitor
  ): Long =
    var rowCount                                           = 0L
    def counted(batches: Iterator[Batch]): Iterator[Batch] = batches.map { b =>
      rowCount += b.size
      b
    }
    connector.sqlConnector match
      case Some(sc) =>
        // HTTP-based engines (e.g. Trino) return paginated results with text values
        val handle = sc.submit(sql)
        try
          val pages  = handle.batches().buffered
          val schema =
            if pages.hasNext then
              StringRows.schemaOf(pages.head.columns.map(c => c.name.name -> c.dataType))
            else
              Schema(IndexedSeq.empty)
          val batches = pages.map(p => StringRows.toBatch(schema, p.rows.map(_.values)))
          DataFileWriter.write(path, schema, counted(batches), options)
        finally
          handle.close()
      case None =>
        connector.runQuery(sql) { rs =>
          val schema = schemaOf(rs)
          DataFileWriter.write(path, schema, counted(batchesOf(rs, schema)), options)
        }
    rowCount

  end write

  private def schemaOf(rs: ResultSet): Schema =
    val md = rs.getMetaData
    StringRows.schemaOf(
      (1 to md.getColumnCount).map { i =>
        md.getColumnName(i) -> jdbcTypeOf(md, i)
      }
    )

  // Scalar types by their JDBC type code (vendor type names such as Snowflake's NUMBER don't parse
  // as wvlet types); other types by their type name, and text when the name doesn't parse
  private def jdbcTypeOf(md: ResultSetMetaData, i: Int): DataType =
    md.getColumnType(i) match
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
        DataType.IntType
      case Types.BIGINT =>
        DataType.LongType
      case Types.REAL | Types.FLOAT | Types.DOUBLE =>
        DataType.DoubleType
      case Types.DECIMAL | Types.NUMERIC if md.getPrecision(i) > 0 =>
        DataType.DecimalType(
          DataType.IntConstant(md.getPrecision(i)),
          DataType.IntConstant(md.getScale(i).max(0))
        )
      case Types.BOOLEAN | Types.BIT =>
        DataType.BooleanType
      case Types.DATE =>
        DataType.DateType
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR | Types.NVARCHAR =>
        DataType.StringType
      case _ =>
        Try(DataType.parse(md.getColumnTypeName(i))).getOrElse(DataType.AnyType)

  private def batchesOf(rs: ResultSet, schema: Schema): Iterator[Batch] =
    val n = schema.size
    Iterator
      .continually {
        val rows = IndexedSeq.newBuilder[Seq[Option[String]]]
        var k    = 0
        while k < jdbcBatchSize && rs.next() do
          rows += (1 to n).map(i => Option(rs.getString(i)))
          k += 1
        rows.result()
      }
      .takeWhile(_.nonEmpty)
      .map(rows => StringRows.toBatch(schema, rows))

end LocalFileExport

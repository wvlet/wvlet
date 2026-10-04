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

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType
import wvlet.lang.connector.CatalogProvider
import wvlet.lang.connector.Connector
import wvlet.lang.connector.DBConnector
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.uni.log.LogSupport

import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
  * Moves rows between connectors for cross-connector staging: reads a table engine-independently as
  * JSON lines and lands them in a staging table on the target engine (#1861 Phase 2/3)
  */
object SourceTableStaging extends LogSupport:

  /**
    * Stream every row of a connector's table as JSON object strings through `body`, without
    * materializing the table. The iterator is only valid inside `body`
    */
  def streamTableAsJsonRows[U](source: Connector, qualifiedName: List[String])(
      body: Iterator[String] => U
  ): U =
    source match
      case db: DBConnector =>
        // Quote each identifier part: source engines have their own casing/keyword rules
        val qualified = qualifiedName.map(part => s"\"${part}\"").mkString(".")
        db.streamJsonRows(s"select * from ${qualified}")(body)
      case other =>
        val rows = other
          .catalog
          .map(_.scan(qualifiedName.last))
          .getOrElse(
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Connector '${other.name}' (${other.connectorType}) exposes no tables to read"
              )
          )
        body(rows.iterator)

  /**
    * Copy a source connector's table into `stagingTable` on the target engine, streaming rows as
    * JSON lines through a temporary file. Returns the number of staged rows. `schemaFields` builds
    * the empty table when there are no rows (read_json_auto cannot infer a schema from an empty
    * file)
    */
  def stageTable(
      source: Connector,
      qualifiedName: List[String],
      engine: DBConnector,
      engineName: String,
      stagingTable: String,
      schemaFields: Seq[NamedType]
  ): Long =
    streamTableAsJsonRows(source, qualifiedName) { rows =>
      loadJsonRows(engine, engineName, stagingTable, rows, schemaFields)
    }

  /**
    * Land JSON rows in `stagingTable` on the target engine, consuming the row iterator
    * incrementally. Returns the number of loaded rows
    */
  def loadJsonRows(
      engine: DBConnector,
      engineName: String,
      stagingTable: String,
      rows: Iterator[String],
      schemaFields: Seq[NamedType]
  ): Long =
    if engine.dbType != DBType.DuckDB then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Staging into '${engineName}' (${engine.dbType}) is not supported yet; " +
            "use a DuckDB engine as the staging target"
        )
    val file = Files.createTempFile(s"wv_staging_", ".jsonl")
    try
      var rowCount = 0L
      scala
        .util
        .Using
        .resource(Files.newBufferedWriter(file, StandardCharsets.UTF_8)) { writer =>
          rows.foreach { row =>
            writer.write(row)
            writer.newLine()
            rowCount += 1
          }
        }
      loadJsonFile(engine, stagingTable, file, rowCount, schemaFields)
      rowCount
    finally
      Files.deleteIfExists(file)

  end loadJsonRows

  /**
    * Load an already-spooled JSON-lines file into a DuckDB staging table. An empty file has no rows
    * to infer a schema from, so the table is built from the declared fields instead
    */
  def loadJsonFile(
      engine: DBConnector,
      stagingTable: String,
      file: java.nio.file.Path,
      rowCount: Long,
      schemaFields: Seq[NamedType]
  ): Unit =
    // DuckDB accepts forward slashes on every platform; backslashes would need escaping in
    // the SQL literal
    val filePath = file.toAbsolutePath.toString.replace('\\', '/').replace("'", "''")
    if rowCount > 0 then
      engine.execute(
        s"""create or replace table "${stagingTable}" as select * from read_json_auto('${filePath}')"""
      )
    else
      val columns = schemaFields
        .map(f => s""""${f.name.name}" ${duckdbTypeOf(f.dataType)}""")
        .mkString(", ")
      engine.execute(s"""create or replace table "${stagingTable}" (${columns})""")

  /**
    * Load a JSON-lines file into a table whose columns and types are the declared fields, instead
    * of inferring them: JSON rows carry decimals and timestamps as strings, and a declared column
    * missing from a row must still exist (as null)
    */
  def loadDeclaredJsonFile(
      engine: DBConnector,
      table: String,
      file: java.nio.file.Path,
      fields: Seq[NamedType]
  ): Unit =
    val filePath = file.toAbsolutePath.toString.replace('\\', '/').replace("'", "''")
    val columns  = fields
      .map { f =>
        val tpe =
          f.dataType match
            case DataType.StringType =>
              "VARCHAR"
            case other =>
              duckdbTypeOf(other) match
                // Not a primitive: keep the value as JSON rather than flattening it to text
                case "VARCHAR" =>
                  "JSON"
                case primitive =>
                  primitive
        s""""${f.name.name}": '${tpe}'"""
      }
      .mkString(", ")
    engine.execute(
      s"""create or replace table "${table}" as select * from read_json('${filePath}', format = 'newline_delimited', columns = {${columns}})"""
    )

  // Best-effort mapping of resolved wvlet types to DuckDB column types for empty staging
  // tables; anything uncommon degrades to VARCHAR (the staging table is empty anyway)
  def duckdbTypeOf(dataType: DataType): String =
    dataType match
      case DataType.IntType =>
        "INTEGER"
      case DataType.LongType =>
        "BIGINT"
      case DataType.FloatType =>
        "FLOAT"
      case DataType.DoubleType =>
        "DOUBLE"
      case DataType.BooleanType =>
        "BOOLEAN"
      case DataType.DateType =>
        "DATE"
      case _: DataType.TimestampType =>
        "TIMESTAMP"
      case _ =>
        "VARCHAR"

end SourceTableStaging

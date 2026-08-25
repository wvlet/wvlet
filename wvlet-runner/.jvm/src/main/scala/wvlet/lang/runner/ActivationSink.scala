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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.connector.DBConnector
import wvlet.lang.connector.duckdb.DuckDBConnector
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.runner.connector.SourceTableStaging
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver
import wvlet.uni.weaver.codec.PrimitiveWeaver.given

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import scala.collection.immutable.ListMap
import scala.util.Using

/**
  * A delivery request of a materialized stage output to an activation target
  *
  * @param target
  *   The activation target name (e.g. `activate('file', ...)` -> `file`)
  * @param params
  *   Named activation parameters (e.g. `activate('file', path: 'out.csv')`)
  * @param stageName
  *   The activating stage
  * @param table
  *   The run-scoped table holding the materialized stage output
  * @param connector
  *   Connector to the database holding the table
  */
case class ActivationRequest(
    target: String,
    params: Map[String, String],
    stageName: String,
    table: String,
    connector: DBConnector
)

/**
  * A sink connector delivering materialized stage outputs to an external activation target
  * (webhook, file export, etc.). Sinks are matched by name against `activate('<name>', ...)`
  * operators; an exception thrown from `activate` fails the stage attempt and follows its regular
  * retry policy
  */
trait ActivationSink:
  /** The target name this sink serves */
  def name: String

  /** Deliver the materialized stage output to this sink */
  def activate(request: ActivationRequest): Unit

/**
  * A built-in sink exporting the stage output to a local file: `activate('file', path: 'out.csv')`.
  * The format is taken from the `format:` parameter or the path extension (csv, parquet, or json;
  * csv by default). Engines with native `COPY TO` support (DuckDB) write the file directly; other
  * engines (e.g. Trino) stream the stage table's rows over their paginated result and hand the file
  * write off to a transient local DuckDB via `read_json_auto` + `COPY TO` — the same pattern
  * `save to` uses for local files on those engines
  *
  * @param handoffEngine
  *   Builds the local DuckDB used by the handoff path; overridable for tests. The engine is created
  *   per activation and closed when the export completes
  */
class FileActivationSink(handoffEngine: () => DBConnector = () => DuckDBConnector(WorkEnv()))
    extends ActivationSink
    with LogSupport:
  override def name: String = "file"

  override def activate(request: ActivationRequest): Unit =
    val path = request
      .params
      .getOrElse(
        "path",
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"activate('file') of stage ${request.stageName} requires a path: parameter"
          )
      )
    val format       = request.params.get("format").orElse(extensionOf(path)).getOrElse("csv")
    val formatClause =
      format match
        case "csv" =>
          "(format csv, header)"
        case "parquet" =>
          "(format parquet)"
        case "json" =>
          "(format json)"
        case other =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"activate('file') of stage ${request.stageName}: unsupported format '${other}'"
            )
    val escapedPath = path.replaceAll("'", "''")
    info(s"Exporting stage ${request.stageName} output ${request.table} to ${path} (${format})")
    if request.connector.dbType.supportSaveAsFile then
      request
        .connector
        .withSession { conn =>
          Using.resource(conn.createStatement()) { stmt =>
            stmt.execute(
              s"""copy (select * from "${request.table}") to '${escapedPath}' ${formatClause}"""
            )
          }
        }
    else
      exportViaDuckDBHandoff(request, escapedPath, formatClause)

  end activate

  /**
    * Export on engines without native `COPY TO`: stream the stage table page by page as JSON lines
    * into a temporary file, then let a local DuckDB `COPY` it to the target path. Memory stays
    * bounded by the source's result page size. An empty stage output still produces a file carrying
    * the schema (e.g. a header-only CSV) by rebuilding the columns from the streamed result's
    * metadata
    */
  private def exportViaDuckDBHandoff(
      request: ActivationRequest,
      escapedPath: String,
      formatClause: String
  ): Unit =
    given QueryProgressMonitor = QueryProgressMonitor.noOp
    val sqlConnector           = request
      .connector
      .sqlConnector
      .getOrElse(
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"activate('file') of stage ${request.stageName} is not supported on ${request
                .connector
                .dbType}: the engine has neither COPY TO support nor a streaming result path"
          )
      )
    val jsonlFile = Files.createTempFile("wv_activation_", ".jsonl")
    // DuckDB accepts forward slashes on every platform; backslashes would need escaping in
    // the SQL literal
    val jsonlPath = jsonlFile.toAbsolutePath.toString.replace('\\', '/').replaceAll("'", "''")
    try
      val rowCodec                = summon[Weaver[ListMap[String, Any]]]
      var rowCount                = 0L
      var columns: Seq[NamedType] = Nil
      val handle                  = sqlConnector.submit(s"""select * from "${request.table}"""")
      try
        Using.resource(Files.newBufferedWriter(jsonlFile, StandardCharsets.UTF_8)) { w =>
          handle
            .batches()
            .foreach { batch =>
              if columns.isEmpty then
                columns = batch.columns
              val names = batch.columns.map(_.name.name)
              batch
                .rows
                .foreach { row =>
                  w.write(
                    rowCodec.toJson(ListMap.from(names.zip(row.values.map(v => v.orNull: Any))))
                  )
                  w.newLine()
                  rowCount += 1
                }
            }
        }
      finally handle.close()

      val duck = handoffEngine()
      try
        if rowCount > 0 then
          duck.execute(
            s"copy (select * from read_json_auto('${jsonlPath}')) to '${escapedPath}' ${formatClause}"
          )
        else
          // read_json_auto cannot infer a schema from an empty file: rebuild the columns from
          // the streamed result's metadata so the exported file still carries the schema
          val cols = columns
            .map(f => s""""${f.name.name}" ${SourceTableStaging.duckdbTypeOf(f.dataType)}""")
            .mkString(", ")
          duck.execute(s"""create or replace table "__wv_activation_empty" (${cols})""")
          duck.execute(
            s"""copy (select * from "__wv_activation_empty") to '${escapedPath}' ${formatClause}"""
          )
      finally duck.close()
    finally
      Files.deleteIfExists(jsonlFile)
    end try

  end exportViaDuckDBHandoff

  private def extensionOf(path: String): Option[String] =
    val name = path.substring(path.lastIndexOf('/') + 1)
    val dot  = name.lastIndexOf('.')
    if dot > 0 then
      Some(name.substring(dot + 1).toLowerCase)
    else
      None

end FileActivationSink

/**
  * A built-in sink posting the stage output to an HTTP endpoint:
  * `activate('webhook', url: 'https://...')`.
  *
  * Rows are serialized as a JSON array of objects (`format: 'json'`, the default) or as
  * newline-delimited JSON (`format: 'ndjson'`) and sent in a single POST request with up to
  * `max_rows:` rows (1000 by default; a larger result is truncated with a warning). A non-2xx
  * response or a connection failure fails the stage attempt and follows its regular retry policy
  */
class WebhookActivationSink(httpClient: => HttpClient = WebhookActivationSink.defaultHttpClient)
    extends ActivationSink
    with LogSupport:

  override def name: String = "webhook"

  override def activate(request: ActivationRequest): Unit =
    val url = request
      .params
      .getOrElse(
        "url",
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"activate('webhook') of stage ${request.stageName} requires a url: parameter"
          )
      )
    val format = request.params.getOrElse("format", "json")
    if format != "json" && format != "ndjson" then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"activate('webhook') of stage ${request
              .stageName}: unsupported format '${format}'. Use json or ndjson"
        )
    val maxRows = request.params.get("max_rows").map(_.toInt).getOrElse(1000)

    val (rows, truncated) = readRows(request, maxRows)
    if truncated then
      warn(
        s"activate('webhook') of stage ${request
            .stageName}: sending only the first ${maxRows} rows of ${request.table}"
      )
    val (contentType, body) =
      format match
        case "ndjson" =>
          ("application/x-ndjson", rows.mkString("", "\n", "\n"))
        case _ =>
          ("application/json", rows.mkString("[", ",", "]"))

    info(
      s"Posting ${rows.size} row(s) of stage ${request.stageName} output ${request.table} to ${url}"
    )
    val httpRequest = HttpRequest
      .newBuilder(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header("Content-Type", contentType)
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString())
    if response.statusCode() < 200 || response.statusCode() >= 300 then
      throw StatusCode
        .ACTIVATION_FAILED
        .newException(
          s"activate('webhook') of stage ${request
              .stageName}: ${url} responded with status ${response.statusCode()}"
        )

  end activate

  /** Read up to maxRows rows of the stage table as JSON objects, reporting truncation */
  private def readRows(request: ActivationRequest, maxRows: Int): (List[String], Boolean) =
    // Read one extra row to detect truncation; queryJsonRows is engine-independent (JDBC or HTTP)
    val rows = request
      .connector
      .queryJsonRows(s"""select * from "${request.table}" limit ${maxRows + 1}""")
    if rows.size > maxRows then
      (rows.take(maxRows), true)
    else
      (rows, false)

end WebhookActivationSink

object WebhookActivationSink:
  private lazy val defaultHttpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

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
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.connector.DBConnector
import wvlet.lang.runner.connector.LocalFileExport
import wvlet.uni.log.LogSupport

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
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
  * engines (e.g. Trino) stream the stage table's rows into the wvlet engine's file writers — the
  * same path `save to` uses for local files on those engines
  */
class FileActivationSink extends ActivationSink with LogSupport:
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
      exportViaEngineWriter(request, path, format)

  end activate

  /**
    * Export on engines without native `COPY TO`: stream the stage table page by page into the wvlet
    * engine's file writers. Memory stays bounded by the source's result page size, and an empty
    * stage output still produces a file carrying the schema (e.g. a header-only CSV)
    */
  private def exportViaEngineWriter(
      request: ActivationRequest,
      path: String,
      format: String
  ): Unit =
    given QueryProgressMonitor    = QueryProgressMonitor.noOp
    val options: Map[String, Any] =
      format match
        case "csv" =>
          Map("format" -> "csv", "header" -> true)
        case other =>
          Map("format" -> other)
    LocalFileExport.write(request.connector, s"""select * from "${request.table}"""", path, options)

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

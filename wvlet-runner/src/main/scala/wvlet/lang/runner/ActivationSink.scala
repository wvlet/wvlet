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
import wvlet.lang.runner.connector.DBConnector
import wvlet.uni.log.LogSupport

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
  * csv by default) and written with the engine's `COPY TO` statement
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
    request
      .connector
      .withSession { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          stmt.execute(
            s"""copy (select * from "${request.table}") to '${escapedPath}' ${formatClause}"""
          )
        }
      }

  end activate

  private def extensionOf(path: String): Option[String] =
    val name = path.substring(path.lastIndexOf('/') + 1)
    val dot  = name.lastIndexOf('.')
    if dot > 0 then
      Some(name.substring(dot + 1).toLowerCase)
    else
      None

end FileActivationSink

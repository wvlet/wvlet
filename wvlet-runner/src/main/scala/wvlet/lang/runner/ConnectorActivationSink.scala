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
import wvlet.lang.connector.Connector
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.log.LogSupport

/**
  * Delivers stage outputs to a connector tool (#1861 Phase 3): a profile connector exposing
  * MCP-shaped tools becomes an activation target under its instance name, e.g.
  *
  * {{{
  *   activate('slack', tool: 'post_message', channel: '#reports', text: 'daily flow done')
  * }}}
  *
  * When the tool takes a `text` argument and none is given, the first rows of the materialized
  * stage output are attached as JSON lines
  */
class ConnectorActivationSink(connectorName: String, connector: () => Connector)
    extends ActivationSink
    with LogSupport:

  override def name: String = connectorName

  override def activate(request: ActivationRequest): Unit =
    val toolName = request
      .params
      .get("tool")
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"activate('${connectorName}') of stage ${request.stageName} requires a tool: parameter"
          )
      )
    val target   = connector()
    val toolSpec = target
      .tools
      .find(_.name == toolName)
      .getOrElse(
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Connector '${connectorName}' has no tool '${toolName}' " +
              s"(available: ${target.tools.map(_.name).mkString(", ")})"
          )
      )
    val explicitArgs: Seq[(String, JSONString)] = request
      .params
      .removed("tool")
      .toSeq
      .map { case (k, v) =>
        k -> JSONString(v)
      }
    // Attach the stage output as JSON lines when the tool declares a top-level `text`
    // property and none was given
    val acceptsText = toolSpec
      .inputSchema
      .get("properties")
      .collect { case o: JSONObject =>
        o
      }
      .exists(_.get("text").isDefined)
    val args =
      if request.params.contains("text") || !acceptsText then
        explicitArgs
      else
        val rows = request
          .connector
          .queryJsonRows(
            s"""select * from "${request.table}" limit ${ConnectorActivationSink.MaxAttachedRows}"""
          )
        explicitArgs :+ ("text" -> JSONString(rows.mkString("\n")))
    target.invoke(toolName, JSONObject(args))
    logDelivered(request)

  end activate

  private def logDelivered(request: ActivationRequest): Unit = info(
    s"Delivered stage ${request.stageName} output of run to '${connectorName}' via tool"
  )

end ConnectorActivationSink

object ConnectorActivationSink:
  /** Rows of the stage output attached to a tool's text argument */
  val MaxAttachedRows = 20

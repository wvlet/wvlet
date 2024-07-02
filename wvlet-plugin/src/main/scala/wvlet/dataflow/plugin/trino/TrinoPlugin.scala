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
package wvlet.dataflow.plugin.trino

import wvlet.airframe.http.RPCStatus
import wvlet.airframe.rx.Cancelable
import wvlet.airframe.surface.secret
import wvlet.dataflow.spi.{TaskInput, TaskPlugin}

object TrinoPlugin extends TaskPlugin:
  override def pluginName: String = "trino"

  override def run(input: TaskInput): Cancelable =
    require(
      input.taskPlugin == "trino",
      s"Invalid plugin name: ${input.taskPlugin}. Expected trino"
    )

    input.methodName match
      case "runQuery" =>
        // TODO Call method with MethodSurface
        val service = input.taskBody
          .getOrElse("service", new AssertionError("missing service parameter")).toString
        val query  = input.taskBody.getOrElse("query", new AssertionError("missing query")).toString
        val schema = input.taskBody.get("schema").map(_.toString)
        runQuery(service, query, schema)
      case other =>
        throw RPCStatus.NOT_FOUND_U5.newException(s"unknown method: ${other}")

    // TODO: Support cancel
    Cancelable.empty

  def runQuery(service: String, query: String, schema: Option[String] = None): Unit =
    info(s"run query [${service}] schema:${schema}\n${query}")

case class TrinoService(address: String, connector: String, @secret user: String)

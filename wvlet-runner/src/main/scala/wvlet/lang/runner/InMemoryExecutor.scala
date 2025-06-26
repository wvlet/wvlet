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

import wvlet.lang.compiler.Context
import wvlet.lang.model.plan.*
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.control.IO
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File
import scala.collection.immutable.ListMap

object InMemoryExecutor:
  def default = InMemoryExecutor()

class InMemoryExecutor extends LogSupport:
  def execute(plan: LogicalPlan, context: Context): QueryResult =
    plan match
      case p: PackageDef =>
        val results = p
          .statements
          .map: stmt =>
            PlanResult(stmt, execute(stmt, context))
        QueryResultList(results)
      case q: Query =>
        execute(q.body, context)
      case t: TestRelation =>
        warn(s"Test execution is not supported yet: ${t}")
        QueryResult.empty
      case r: FileScan if r.filePath.endsWith(".json") =>
        val json = IO.readAsString(new File(r.filePath))
        trace(json)
        val codec = MessageCodec.of[Seq[ListMap[String, Any]]]
        val data  = codec.fromJson(json)
        TableRows(r.schema, data, data.size)

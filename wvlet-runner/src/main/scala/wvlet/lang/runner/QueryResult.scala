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

import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

sealed trait QueryResult:
  override def toString: String = toPrettyBox()
  def toPrettyBox(maxWidth: Option[Int] = None, maxColWidth: Int = 150): String = QueryResultPrinter
    .print(this, PrettyBoxFormat(maxWidth, maxColWidth))

  def toTSV: String = QueryResultPrinter.print(this, TSVFormat)

object QueryResult:
  object empty extends QueryResult

case class QueryResultList(list: Seq[QueryResult]) extends QueryResult

case class PlanResult(plan: LogicalPlan, result: QueryResult) extends QueryResult

case class TableRows(schema: RelationType, rows: Seq[ListMap[String, Any]], totalRows: Int)
    extends QueryResult:
  def isTruncated: Boolean = rows.size < totalRows

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

import wvlet.airframe.codec.MessageCodec
import wvlet.lang.StatusCode
import wvlet.lang.compiler.SourceLocation
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

sealed trait QueryResult:
  def isEmpty: Boolean          = this eq QueryResult.empty
  override def toString: String = toPrettyBox()
  def toPrettyBox(maxWidth: Option[Int] = None, maxColWidth: Int = 150): String = QueryResultPrinter
    .print(this, PrettyBoxFormat(maxWidth, maxColWidth))

  def toTSV: String                    = QueryResultPrinter.print(this, TSVFormat)
  def getError: Option[Throwable]      = None
  def getWarning: Option[String]       = None
  def hasError: Boolean                = getError.isDefined
  def isWarning: Boolean               = getWarning.isDefined
  def isTest: Boolean                  = false
  def isSuccess: Boolean               = !hasError
  def isSuccessfulQueryResult: Boolean = isSuccess && !isTest && !isWarning

object QueryResult:
  object empty extends QueryResult
  def fromList(lst: List[QueryResult]): QueryResult =
    lst.filter(!_.isEmpty) match
      case Nil =>
        QueryResult.empty
      case r :: Nil =>
        r
      case lst =>
        QueryResultList(lst)

case class QueryResultList(list: Seq[QueryResult]) extends QueryResult:
  override def getError: Option[Throwable] =
    val errors = list.map(_.getError).filter(_.isDefined)
    if errors.isEmpty then
      None
    else
      Some(errors.head.get)

  override def getWarning: Option[String] =
    val warnings = list.map(_.getWarning).filter(_.isDefined).map(_.get)
    if warnings.isEmpty then
      None
    else
      Some(warnings.mkString("\n"))

case class PlanResult(plan: LogicalPlan, result: QueryResult) extends QueryResult

case class TableRows(schema: RelationType, rows: Seq[ListMap[String, Any]], totalRows: Int)
    extends QueryResult:
  def isTruncated: Boolean = rows.size < totalRows
  def toJsonLines: String =
    val codec = MessageCodec.of[ListMap[String, Any]]
    val jsonLines = rows
      .map { row =>
        codec.toJson(row)
      }
      .mkString("\n")
    jsonLines

case class WarningResult(msg: String, loc: SourceLocation) extends QueryResult:
  override def getWarning: Option[String] = Some(msg)

case class ErrorResult(e: Throwable) extends QueryResult:
  override def getError: Option[Throwable] = Some(e)

case class TestSuccess(msg: String, loc: SourceLocation) extends QueryResult:
  override def isTest: Boolean = true

case class TestFailure(msg: String, loc: SourceLocation) extends QueryResult:
  override val getError: Option[Throwable] = Some(StatusCode.TEST_FAILED.newException(msg, loc))
  override def isTest: Boolean             = true

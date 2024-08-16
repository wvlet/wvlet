package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.model.{DataType, RelationType}
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

sealed trait QueryResult:
  override def toString: String = toPrettyBox
  def toPrettyBox: String       = QueryResultPrinter.print(this, PrettyBoxFormat)
  def toTSV: String             = QueryResultPrinter.print(this, TSVFormat)

object QueryResult:
  object empty extends QueryResult

case class QueryResultList(list: Seq[QueryResult]) extends QueryResult

case class PlanResult(plan: LogicalPlan, result: QueryResult) extends QueryResult

case class TableRows(schema: RelationType, rows: Seq[ListMap[String, Any]], totalRows: Int)
    extends QueryResult:
  def isTruncated: Boolean = rows.size < totalRows

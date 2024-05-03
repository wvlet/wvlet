package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.model.RelationType
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import org.msgpack.core.MessagePack
import wvlet.log.LogSupport

trait QueryResult

object QueryResult:
  object empty extends QueryResult

case class QueryResultList(list: Seq[QueryResult]) extends QueryResult

case class PlanResult(plan: LogicalPlan, result: QueryResult) extends QueryResult

case class TableRows(schema: RelationType, rows: Seq[Map[String, Any]]) extends QueryResult

object QueryResultPrinter extends LogSupport:
  def print(result: QueryResult): String =
    result match
      case QueryResultList(list) =>
        list.map(print).mkString("\n\n")
      case PlanResult(plan, result) =>
        s"[plan]:\n${plan.pp}\n[result]\n${print(result)}"
      case TableRows(schema, rows) =>
        val header = schema.fields.map(_.typeName).mkString(", ")
        rows.map(_.toString()).mkString("\n")
      case _ =>
        result.toString

package com.treasuredata.flow.lang.runner

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
  def print(result: QueryResult, limit: Option[Int] = None): String =
    result match
      case QueryResult.empty =>
        "<empty>"
      case QueryResultList(list) =>
        list.map(x => print(x, limit)).mkString("\n\n")
      case PlanResult(plan, result) =>
        s"[plan]:\n${plan.pp}\n[result]\n${print(result, limit)}"
      case TableRows(schema, rows) =>
        val header = schema.fields.map(_.typeName).mkString(", ")
        val resultRows =
          limit match
            case Some(limit) =>
              rows.take(limit)
            case None =>
              rows
        resultRows.map(print).mkString("\n")
      case _ =>
        result.toString

  private def print(row: Any): String =
    row match
      case m: Map[?, ?] =>
        m.map: (k, v) =>
            s"${k}: ${print(v)}"
          .mkString(", ")
      case a: Array[?] =>
        a.map(print).mkString(", ")
      case null =>
        "null"
      case s: String =>
        s""""${s}""""
      case x =>
        x.toString

end QueryResultPrinter

package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.model.RelationType
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import org.msgpack.core.MessagePack
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

sealed trait QueryResult

object QueryResult:
  object empty extends QueryResult

case class QueryResultList(list: Seq[QueryResult]) extends QueryResult

case class PlanResult(plan: LogicalPlan, result: QueryResult) extends QueryResult

case class TableRows(schema: RelationType, rows: Seq[ListMap[String, Any]]) extends QueryResult

object QueryResultPrinter extends LogSupport:
  def print(result: QueryResult, limit: Option[Int] = None): String =
    result match
      case QueryResult.empty =>
        ""
      case QueryResultList(list) =>
        list.map(x => print(x, limit)).mkString("\n\n")
      case PlanResult(plan, result) =>
        s"[plan]:\n${plan.pp}\n[result]\n${print(result, limit)}"
      case t @ TableRows(schema, rows) =>
        printTableRows(t)

  private def printTableRows(tableRows: TableRows): String =
    val tbl: Seq[Seq[String]] =
      val rows = Seq.newBuilder[Seq[String]]
      rows += tableRows.schema.fields.map(_.name.name)
      tableRows
        .rows
        .foreach { row =>
          val sanitizedRow = row.map { (k, v) =>
            Option(v).map(_.toString).getOrElse("")
          }
          rows += sanitizedRow.toSeq
        }
      rows.result()

    val maxColSize: IndexedSeq[Int] =
      tbl
        .map { row =>
          row.map(_.size)
        }
        .reduce { (r1, r2) =>
          r1.zip(r2)
            .map { case (l1, l2) =>
              l1.max(l2)
            }
        }
        .toIndexedSeq

    val header = tbl.head
    val data   = tbl.tail

    val rows = Seq.newBuilder[String]

    rows +=
      maxColSize
        .map { maxSize =>
          "─".padTo(maxSize, "─").mkString
        }
        .mkString("┌─", "─┬─", "─┐")

    rows +=
      header
        .zip(maxColSize)
        .map { case (h, maxSize) =>
          h.padTo(maxSize, " ").mkString
        }
        .mkString("│ ", " │ ", " │")
    rows +=
      maxColSize
        .map { s =>
          "─" * s
        }
        .mkString("├─", "─┼─", "─┤")

    rows ++=
      data.map { row =>
        row
          .zip(maxColSize)
          .map { case (x, maxSize) =>
            x.padTo(maxSize, " ").mkString
          }
          .mkString("│ ", " │ ", " │")
      }

    rows +=
      maxColSize
        .map { maxSize =>
          "─".padTo(maxSize, "─").mkString
        }
        .mkString("└─", "─┴─", "─┘")

    rows.result().mkString("\n")

  end printTableRows

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

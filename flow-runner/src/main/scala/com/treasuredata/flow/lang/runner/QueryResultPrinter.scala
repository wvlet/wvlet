package com.treasuredata.flow.lang.runner

import wvlet.log.LogSupport

trait QueryResultFormat:
  def printTableRows(tableRows: TableRows): String

object TSVFormat extends QueryResultFormat:
  def printTableRows(tableRows: TableRows): String =
    val fieldNames = tableRows.schema.fields.map(_.name.name).toIndexedSeq
    val header     = fieldNames.mkString("\t")
    val data = tableRows
      .rows
      .map { row =>
        fieldNames
          .map { fieldName =>
            row
              .get(fieldName)
              .map {
                case null =>
                  ""
                case other =>
                  other.toString
              }
              .getOrElse("")
          }
          .mkString("\t")
      }
      .mkString("\n")
    s"${header}\n${data}"

end TSVFormat

object QueryResultPrinter extends LogSupport:
  def print(result: QueryResult, format: QueryResultFormat): String =
    result match
      case QueryResult.empty =>
        ""
      case QueryResultList(list) =>
        list.map(x => print(x, format)).mkString("\n\n")
      case PlanResult(plan, result) =>
        print(result, format)
      case t: TableRows =>
        format.printTableRows(t)

end QueryResultPrinter

object PrettyBoxFormat extends QueryResultFormat:
  def printTableRows(tableRows: TableRows): String =
    val alignRight = tableRows.schema.fields.map(_.isNumeric).toIndexedSeq

    val tbl: Seq[Seq[String]] =
      val rows = Seq.newBuilder[Seq[String]]
      rows += tableRows.schema.fields.map(_.name.name)
      var rowCount = 0
      tableRows
        .rows
        .foreach { row =>
          val sanitizedRow = row.map { (k, v) =>
            Option(v).map(_.toString).getOrElse("")
          }
          rowCount += 1
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

    val rows  = Seq.newBuilder[String]
    val width = maxColSize.sum + (maxColSize.size - 1) * 3

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
          .zipWithIndex
          .map { case ((x, maxSize), colIndex) =>
            if alignRight(colIndex) then
              x.reverse.padTo(maxSize, " ").reverse.mkString
            else
              x.padTo(maxSize, " ").mkString
          }
          .mkString("│ ", " │ ", " │")
      }

    // result footer
    rows +=
      maxColSize
        .map { s =>
          "─" * s
        }
        .mkString("├─", "─┴─", "─┤")
    if tableRows.isTruncated then
      rows +=
        f"${tableRows.totalRows}%,d rows (${tableRows.rows.size}%,d shown)"
          .padTo(width, " ")
          .mkString("│ ", "", " │")
    else
      rows += f"${tableRows.totalRows}%,d rows".padTo(width, " ").mkString("│ ", "", " │")

    rows +=
      maxColSize
        .map { maxSize =>
          "─".padTo(maxSize, "─").mkString
        }
        .mkString("└─", "───", "─┘")

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

end PrettyBoxFormat

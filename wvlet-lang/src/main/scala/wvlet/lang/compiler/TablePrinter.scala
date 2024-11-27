package wvlet.lang.compiler

import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.model.DataType
import scala.util.Try

object TablePrinter:
  private val maxWidth: Option[Int] = None
  private val maxColWidth: Int      = 150

  def printTableRows(queryResult: QueryResult): String =
    val dataTypes = queryResult
      .schema
      .map { c =>
        Try(DataType.parse(c.typeName)).getOrElse(DataType.GenericType(Name.typeName(c.typeName)))
      }
    val isNumeric = dataTypes.map(_.isNumeric).toIndexedSeq

    val tbl: List[Seq[String]] =
      val rows = List.newBuilder[Seq[String]]
      // Column names
      rows += queryResult.schema.map(_.name)
      // Column types
      rows +=
        dataTypes.map {
          _.typeDescription
        }

      var rowCount = 0
      queryResult
        .rows
        .foreach { row =>
          val sanitizedRow = row.map { v =>
            Option(v).map(v => printElem(v)).getOrElse("")
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
        .map(_.min(maxColWidth))
        .toIndexedSeq

    assert(tbl.size >= 2)
    val columnLabels = tbl.head
    val columnTypes  = tbl.tail.head

    val data = tbl.tail.tail

    val rows  = Seq.newBuilder[String]
    val width = maxColSize.sum + (maxColSize.size - 1) * 3

    rows +=
      maxColSize
        .map { maxSize =>
          "─".padTo(maxSize, "─").mkString
        }
        .mkString("┌─", "─┬─", "─┐")

    // header
    rows +=
      columnLabels
        .zip(maxColSize)
        .map { case (h, maxSize) =>
          center(h, maxSize)
        }
        .mkString("│ ", " │ ", " │")
    // column types
    rows +=
      columnTypes
        .zip(maxColSize)
        .map { case (h, maxSize) =>
          center(h, maxSize)
        }
        .mkString("│ ", " │ ", " │")

    // header separator
    rows +=
      maxColSize
        .map { s =>
          "─" * s
        }
        .mkString("├─", "─┼─", "─┤")

    // rows
    rows ++=
      data.map { row =>
        row
          .zip(maxColSize)
          .zipWithIndex
          .map { case ((x, maxSize), colIndex) =>
            if isNumeric(colIndex) then
              alignRight(x, maxSize)
            else
              alignLeft(x, maxSize)
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
//    if tableRows.isTruncated then
//      rows +=
//        alignLeft(f"${tableRows.totalRows}%,d rows (${tableRows.rows.size}%,d shown)", width)
//          .mkString("│ ", "", " │")
//    else
    rows += alignLeft(f"${queryResult.totalRows}%,d rows", width).mkString("│ ", "", " │")

    rows +=
      maxColSize
        .map { maxSize =>
          "─".padTo(maxSize, "─").mkString
        }
        .mkString("└─", "───", "─┘")

    val formattedRows = rows
      .result()
      .map { row =>
        trimToWidth(row, maxWidth.getOrElse(row.size))
      }
    formattedRows.mkString("\n")

  end printTableRows

  def printElem(elem: Any): String =
    elem match
      case null =>
        ""
      case s: String =>
        replaceEscapeChars(s)
      case m: Map[?, ?] =>
        val elems = m
          .map { (k, v) =>
            s"${k} => ${printElem(v)}"
          }
          .mkString(", ")
        s"{${elems}}"
      case a: Seq[?] =>
        s"[${a.map(v => printElem(v)).mkString(", ")}]"
      case a: Array[?] =>
        s"[${a.map(v => printElem(v)).mkString(", ")}]"
      case x =>
        replaceEscapeChars(x.toString)

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

  /**
    * Estimate the width of a UTF-16 character
    *
    * @param ch
    * @return
    */
  def wcWidth(ch: Char): Int = 1

  def wcWidth(s: String): Int = s.map(wcWidth).sum

  def trimToWidth(s: String, colSize: Int): String =
    val wclen = wcWidth(s)

    def truncate(s: String, colSize: Int): String =
      var len    = 0
      val result = new StringBuilder(colSize)
      for
        c <- s
        w = wcWidth(c)
        if len + w <= colSize - 1
      do
        result += c
        len += w

      // pad the rest of the column with spaces or dots
      if len < colSize && len < wclen then
        result += '…'

      result.toString

    if wclen <= colSize then
      s
    else
      truncate(s, colSize)
  end trimToWidth

  def center(s: String, colSize: Int): String =
    val ws           = trimToWidth(s, colSize)
    val padding      = (colSize - wcWidth(ws)).max(0)
    val leftPadding  = padding / 2
    val rightPadding = padding - leftPadding
    " " * leftPadding + ws + " " * rightPadding

  def alignRight(s: String, colSize: Int): String =
    val ws      = trimToWidth(s, colSize)
    val padding = (colSize - wcWidth(ws)).max(0)
    " " * padding + ws

  def alignLeft(s: String, colSize: Int): String =
    val ws      = trimToWidth(s, colSize)
    val padding = (colSize - wcWidth(ws)).max(0)
    ws + " " * padding

  def replaceEscapeChars(s: String): String =
    s.map {
        case '\b' =>
          "\\b"
        case '\f' =>
          "\\f"
        case '\n' =>
          "\\n"
        case '\r' =>
          "\\r"
        case '\t' =>
          "\\t"
        case ch =>
          ch
      }
      .mkString

end TablePrinter

package wvlet.lang.compiler.analyzer.duckdb

import wvlet.lang.compiler.connector.QueryResult

/**
  * Cross-platform formatters for [[QueryResult]] — CSV (for piping) and a box-drawn table (for
  * terminal output). Kept deliberately minimal: no per-column type-aware alignment, no truncation,
  * no paging.
  */
object QueryResultPrinter:

  /**
    * RFC-4180-ish CSV. Wraps fields containing commas, quotes, or newlines in double quotes and
    * doubles embedded double quotes. SQL `NULL` is rendered as an empty field.
    */
  def toCsv(result: QueryResult): String =
    val sb     = StringBuilder()
    val header = result.columns.map(c => csvEscape(c.name.name)).mkString(",")
    sb.append(header).append('\n')
    for row <- result.rows do
      val line = row.values.map(_.map(csvEscape).getOrElse("")).mkString(",")
      sb.append(line).append('\n')
    sb.toString

  /**
    * Unicode box-drawn table, e.g.
    * {{{
    *   ┌────┬───────┐
    *   │ id │ name  │
    *   ├────┼───────┤
    *   │ 1  │ alice │
    *   │ 2  │ bob   │
    *   └────┴───────┘
    * }}}
    * Column width = max(header width, max cell width). SQL `NULL` renders as the literal lowercase
    * `null`.
    */
  def toBox(result: QueryResult): String =
    val headers = result.columns.map(_.name.name)
    val widths  = headers
      .zipWithIndex
      .map { case (h, i) =>
        val cellWidths = result.rows.map(_.values(i).map(_.length).getOrElse("null".length))
        (h.length +: cellWidths).max
      }

    val sb = StringBuilder()

    def border(left: String, mid: String, right: String): Unit =
      sb.append(left)
      widths
        .zipWithIndex
        .foreach { case (w, i) =>
          sb.append("─" * (w + 2))
          if i < widths.size - 1 then
            sb.append(mid)
        }
      sb.append(right).append('\n')

    def dataRow(cells: Seq[String]): Unit =
      sb.append('│')
      cells
        .zip(widths)
        .foreach { case (cell, w) =>
          sb.append(' ')
          sb.append(cell)
          sb.append(" " * (w - cell.length))
          sb.append(' ')
          sb.append('│')
        }
      sb.append('\n')

    border("┌", "┬", "┐")
    dataRow(headers)
    border("├", "┼", "┤")
    for row <- result.rows do
      dataRow(row.values.map(_.getOrElse("null")))
    border("└", "┴", "┘")

    sb.toString

  end toBox

  private def csvEscape(s: String): String =
    if s.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r') then
      val doubled = s.replace("\"", "\"\"")
      s"\"${doubled}\""
    else
      s

end QueryResultPrinter

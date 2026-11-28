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
package wvlet.lang.ui.playground

import wvlet.lang.compiler.lsp.StdLibEntry

/** The kind of a notebook cell: a query, or text in markdown */
enum CellKind:
  case Code
  case Markdown

/** The source of a notebook cell */
case class CellSource(kind: CellKind, text: String)

/**
  * A notebook in text form: a `.wv` (or `.sql`) file whose cells are separated by `-- %%` comment
  * lines, the convention of jupytext's percent format. A text cell is a `-- %% [markdown]` block of
  * `--` comment lines. Since markers and text cells are comments, the file compiles as is, and the
  * cells of a notebook keep their line numbers in it.
  *
  * A file without markers (a bundled example, a shared query) reads as a text cell from its leading
  * comment lines, the first one as the title, and a code cell from the rest.
  */
object NotebookText:
  private val marker         = "-- %%"
  private val markdownMarker = "-- %% [markdown]"

  private def isMarker(line: String): Boolean = line.trim.startsWith(marker)

  /** The text of a notebook, and the 1-origin line where each cell's text starts in it */
  case class Layout(text: String, startLines: IndexedSeq[Int])

  /** The cells of a notebook text */
  def parse(text: String): Seq[CellSource] =
    val lines = text.linesIterator.toIndexedSeq
    if lines.exists(isMarker) then
      parseMarked(lines)
    else
      parsePlain(lines)

  private def parseMarked(lines: IndexedSeq[String]): Seq[CellSource] =
    val cells                        = Seq.newBuilder[CellSource]
    var kind                         = CellKind.Code
    var current                      = Vector.empty[String]
    def flush(always: Boolean): Unit =
      val body =
        kind match
          case CellKind.Markdown =>
            current.map(l => l.trim.stripPrefix("--").stripPrefix(" "))
          case CellKind.Code =>
            current
      val t = trimLines(body)
      if always || t.nonEmpty then
        cells += CellSource(kind, t)
    var seenMarker = false
    lines.foreach { line =>
      if isMarker(line) then
        // Text before the first marker is a cell only when it has something in it
        flush(always = seenMarker)
        seenMarker = true
        kind =
          if line.trim.startsWith(markdownMarker) then
            CellKind.Markdown
          else
            CellKind.Code
        current = Vector.empty
      else
        current :+= line
    }
    flush(always = true)
    cells.result()

  end parseMarked

  private def parsePlain(lines: IndexedSeq[String]): Seq[CellSource] =
    val header = lines.takeWhile(_.trim.startsWith("--")).map(_.trim.stripPrefix("--").trim)
    val code   = trimLines(lines.drop(header.size))
    if header.isEmpty then
      Seq(CellSource(CellKind.Code, code))
    else
      val title = header.head
      val body  = header.drop(1)
      val md    =
        if body.isEmpty then
          s"## ${title}"
        else
          s"## ${title}\n\n${body.mkString("\n")}"
      Seq(CellSource(CellKind.Markdown, md), CellSource(CellKind.Code, code))

  // Drop the blank lines around a cell's text
  private def trimLines(lines: Seq[String]): String = lines
    .dropWhile(_.trim.isEmpty)
    .reverse
    .dropWhile(_.trim.isEmpty)
    .reverse
    .mkString("\n")

  /**
    * The text of the cells. A notebook of one code cell is that cell's text, so a plain query stays
    * plain; otherwise every cell starts with a marker line.
    */
  def render(cells: Seq[CellSource]): Layout =
    cells match
      case Seq(CellSource(CellKind.Code, t)) =>
        Layout(t, IndexedSeq(1))
      case _ =>
        val out    = Seq.newBuilder[String]
        val starts = IndexedSeq.newBuilder[Int]
        var line   = 1
        cells.foreach { c =>
          val body =
            c.kind match
              case CellKind.Code =>
                out += marker
                c.text.split("\n", -1).toSeq
              case CellKind.Markdown =>
                out += markdownMarker
                c.text
                  .split("\n", -1)
                  .toSeq
                  .map { l =>
                    if l.isEmpty then
                      "--"
                    else
                      s"-- ${l}"
                  }
          starts += line + 1
          out ++= body
          line += 1 + body.size
        }
        Layout(out.result().mkString("\n"), starts.result())

  /** The notebook of a bundled example */
  def ofExample(e: Example): Seq[CellSource] = parse(e.text)

  /**
    * A query that reads well in a cell: top-level clauses of a one-line example on their own lines
    */
  def formatQuery(q: String): String =
    val clauses = Seq("select ", "agg ", "group by ", "where ", "add ", "limit ")
    val sb      = StringBuilder()
    var depth   = 0
    var quote   = Option.empty[Char]
    var i       = 0
    while i < q.length do
      val c = q.charAt(i)
      quote match
        case Some(qc) =>
          if c == qc then
            quote = None
        case None =>
          c match
            case '\'' | '"' =>
              quote = Some(c)
            case '(' | '[' | '{' =>
              depth += 1
            case ')' | ']' | '}' =>
              depth -= 1
            case _ =>
      val breaks =
        quote.isEmpty && depth == 0 && c == ' ' && i > 0 &&
          clauses.exists(k => q.startsWith(k, i + 1))
      if breaks then
        sb += '\n'
      else
        sb += c
      i += 1
    sb.result()

  end formatQuery

  /**
    * The notebook of a stdlib function: its signature and description, with links to the bundled
    * examples that call it, then one cell per example
    */
  def ofFunction(e: StdLibEntry): Seq[CellSource] =
    val md = Seq.newBuilder[String]
    md += s"## `${e.label}`"
    md += ""
    md += s"**${e.category.title}** · `${e.signature}`"
    e.typeParam.foreach(t => md += s"T is one of: ${t}")
    md += ""
    if e.description.nonEmpty then
      md += e.description
      md += ""
    if e.engines.nonEmpty then
      md += s"Specialized for these engines only: ${e.engines}"
      md += ""
    val used = Reference.examplesUsing(e)
    if used.nonEmpty then
      md += "Used in: " +
        used
          .map(x => s"[${Examples.categoryLabel(x.category)} › ${x.title}](#example=${x.path})")
          .mkString(", ")
      md += ""
    val text     = CellSource(CellKind.Markdown, trimLines(md.result()))
    val examples =
      if e.examples.isEmpty then
        Seq(CellSource(CellKind.Code, s"-- No example yet: try ${e.name} on a column here\n"))
      else
        e.examples.map(x => CellSource(CellKind.Code, formatQuery(Reference.queryOf(x).trim)))
    text +: examples

  end ofFunction

end NotebookText

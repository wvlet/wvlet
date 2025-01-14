package wvlet.lang.compiler.formatter

case class CodeFormatterConfig(
    indentWidth: Int = 2,
    maxLineLength: Int = 100
)

object CodeFormatter:

  sealed trait Doc
  case class Text(s: String) extends Doc
  case object NewLine extends Doc
  // Horizontally concatenated docs
  case class HList(d1: Doc, d2: Doc) extends Doc
  // Vertically concatenated docs
  case class VList(d1: Doc, d2: Doc) extends Doc
  case class Nest(level: Int, d: Doc) extends Doc
  // Grouped doc elements
  case class Group(d: Doc) extends Doc

  // Convenient operators
  inline def text(s: String): Doc = Text(s)
  inline def newLine: Doc = NewLine
  inline def nest(level: Int, d: Doc): Doc = Nest(level, d)
  inline def group(d: Doc): Doc = Group(d)
  inline def ws: Doc = Text(" ")

  // Concat docs horizontally
  def +(d1:Doc, d2:Doc): Doc = d1 match
    case Text("") => d2
    case _ => d2 match
      case Text("") => d1
      case _ => d1 + d2

  // Concat docs vertically
  def /(d1:Doc, d2:Doc): Doc = d1 match
    case Text("") => d2
    case _ => d2 match
      case Text("") => d1
      case _ => d1 / d2

  def flatten(doc: Doc): Doc =
    doc match
      case NewLine => ws
      case d1 + d2 => flatten(d1) + flatten(d2)
      case d1 / d2 => flatten(d1) + ws + flatten(d2)
      case Nest(level, d) => Nest(level, flatten(d))
      case Group(d) => flatten(d)
      case _ => doc

end CodeFormatter

import CodeFormatter.*
class CodeFormatter[Token](config: CodeFormatterConfig):
  def fits(width: Int, level: Int, doc: Doc): Boolean =
    level * config.indentWidth + doc.length <= width

  def best(width: Int, level: Int, doc: Doc): Doc =
    doc match
      case Text(s) => doc
      case NewLine => doc
      case HList(d1, d2) =>
        best(width, level, d1) + best(width, level, d2)
      case VList(d1, d2) =>
        best(width, level, d1) / best(width, level, d2)
      case Nest(l, d) =>
        nest(l, best(width, level + l, d))
      case Group(d) =>
        val oneline = flatten(d)
        if fits(width, level, oneline) then
          oneline
        else
          best(width, level, d)

  def format(doc: Doc): String =
    val formattedDoc = best(config.maxLineLength, 0, doc)
    formattedDoc.render(config.maxLineLength, 0)

  def render(width: Int, nestingLevel: Int, d: Doc): String =
    d match
      case Text(s) => s
      case NewLine => "\n" + " " * nestingLevel
      case HList(d1, d2) =>
        val r1 = render(width, nestingLevel, d1)
        val r2 = render(width, nestingLevel, d2)
        s"${r1}${r2}"
      case VList(d1, d2) =>
        val r1 = render(width, nestingLevel, d1)
        val r2 = render(width, nestingLevel, d2)
        s"${r1}\n${r2w}"
      case Nest(level, d) =>
        render(width, nestingLevel + level, d)
      case Group(d) =>
        val flat = render(width, nestinglevel, flatten(d))
        if (nestingLevel * config.indentWidth + flat.length <= width) then
          flat
        else
          render(width, nestingLevel, d)

  /**
   * Compute the text length if rendered into a single line
   * @param d
   * @return
   */
  def length(d: Doc): Int =
    d match
      case Text(s) => s.length
      case NewLine => 1
      case HList(d1, d2) => length(d1) + length(d2)
      case VList(d1, d2) => length(d1) + 1 + length(d2)
      case Nest(level, d) => length(d)
      case Group(d) => length(d)


end CodeFormatter

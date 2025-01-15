package wvlet.lang.compiler.formatter

import scala.annotation.tailrec

case class CodeFormatterConfig(
        indentWidth: Int = 2,
        maxLineWidth: Int = 100,
        preserveNewLines: Int = 1,
        addTrailingCommaToItemList: Boolean = true
)

/**
  * A code formatter algorithm and data structure for representing code blocks. This algorithm is
  * derived from Philip Wadler's "A Prettier Printer" algorithm.
  */
object CodeFormatter:

  sealed trait Doc:
    /**
      * Compute the text length if rendered into a single line
      *
      * @param d
      * @return
      */
    def length: Int =
      this match
        case Text(s) =>
          s.length
        case NewLine =>
          1
        case OptNewLine =>
          0
        case HList(d1, d2) =>
          d1.length + d2.length
        case VList(d1, d2) =>
          d1.length + 1 + d2.length
        case Nest(level, d) =>
          d.length
        case Group(d) =>
          d.length

    def flatten: Doc =
      this match
        case NewLine =>
          whitespace
        case OptNewLine =>
          empty
        case HList(d1, d2) =>
          d1.flatten + d2.flatten
        case VList(d1, d2) =>
          d1.flatten + whitespace + d2.flatten
        case Nest(level, d) =>
          Nest(level, d.flatten)
        case Group(d) =>
          d.flatten
        case _ =>
          this

    // Concat docs horizontally
    def +(s: String): Doc = this.+(text(s))

    def +(d2: Doc): Doc =
      this match
        case Text("") =>
          d2
        case _ =>
          d2 match
            case Text("") =>
              this
            case _ =>
              this + d2

    def +(d2: Option[Doc]): Doc =
      d2 match
        case Some(d) =>
          this + d
        case None =>
          this

    // Concat docs vertically
    def /(d2: Doc): Doc =
      this match
        case Text("") =>
          d2
        case _ =>
          d2 match
            case Text("") =>
              this
            case _ =>
              this / d2

    def /(d2: Option[Doc]): Doc =
      d2 match
        case Some(d) =>
          this / d
        case None =>
          this

  end Doc

  case class Text(s: String) extends Doc
  case object NewLine        extends Doc

  // Optional line break
  case object OptNewLine extends Doc
  // Horizontally concatenated docs
  case class HList(d1: Doc, d2: Doc) extends Doc
  // Vertically concatenated docs. This break is always preserved
  case class VList(d1: Doc, d2: Doc)  extends Doc
  case class Nest(level: Int, d: Doc) extends Doc
  // Group is a unit for compacting the doc into a single line if possible
  case class Group(d: Doc) extends Doc

  // Convenient operators
  inline def text(s: String): Doc          = Text(s)
  inline def newline: Doc                  = NewLine
  inline def maybeNewline: Doc = OptNewLine
  inline def nest(level: Int, d: Doc): Doc = Nest(level, d)
  inline def group(d: Doc): Doc            = Group(d)
  val whitespace: Doc                              = Text(" ")
  val empty: Doc = Text("")

end CodeFormatter

import CodeFormatter.*

import scala.annotation.tailrec
class CodeFormatter(config: CodeFormatterConfig = CodeFormatterConfig()):
  protected def fits(level: Int, doc: Doc): Boolean =
    level * config.indentWidth + doc.length <= config.maxLineWidth

  protected def best(level: Int, doc: Doc): Doc =
    doc match
      case Text(s) =>
        doc
      case NewLine =>
        doc
      case HList(d1, d2) =>
        best(level, d1) + best(level, d2)
      case VList(d1, d2) =>
        best(level, d1) / best(level, d2)
      case Nest(l, d) =>
        nest(l, best(level + l, d))
      case Group(d) =>
        val oneline = d.flatten
        if fits(level, oneline) then
          oneline
        else
          best(level, d)

  def format(doc: Doc): String =
    val formattedDoc = best(0, doc)
    render(0, formattedDoc)

  def render(nestingLevel: Int, d: Doc): String =
    d match
      case Text(s) =>
        s
      case NewLine =>
        "\n" + " " * nestingLevel
      case HList(d1, d2) =>
        val r1 = render(nestingLevel, d1)
        val r2 = render(nestingLevel, d2)
        s"${r1}${r2}"
      case VList(d1, d2) =>
        val r1 = render(nestingLevel, d1)
        val r2 = render(nestingLevel, d2)
        s"${r1}\n${r2}"
      case Nest(level, d) =>
        render(nestingLevel + level, d)
      case Group(d) =>
        val flat = render(nestingLevel, d.flatten)
        if nestingLevel * config.indentWidth + flat.length <= config.maxLineWidth then
          flat
        else
          render(nestingLevel, d)

  protected def verticalConcat(lst: List[Doc], separator: Doc): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        head + separator / verticalConcat(tail, separator)

  protected def itemList(items: List[Doc]): Doc =
    items match
      case Nil =>
        empty
      case head :: Nil =>
        if config.addTrailingCommaToItemList then
          head + text(",") + newline
        else
          head
      case head :: tail =>
        head + text(",") + newline + itemList(tail)

  /**
    * Concatenate a list of docs with a comma separator
    *
    * @param lst
    * @return
    */
  protected def cs(lst: List[Doc]): Doc =
    concat(lst, text(", "))

  private def toDoc(x: Any): Doc =
    x match
      case d: Doc => d
      case s: String => text(s)
      case Some(x) => toDoc(x)
      case None => empty
      case s: Seq[_] =>
        horizontalConncat(s.map(toDoc).toList)
      case other => empty

  /**
   * Concatenate items with a whitespace separator
   * @param lst
   * @return
   */
  protected def ws(lst: Any*): Doc =
    def loop(x: List[Any]): List[Doc] =
      x match
        case Nil =>
          Nil
        case head :: Nil =>
          toDoc(head) :: Nil
        case head :: tail =>
          toDoc(head) :: loop(tail)

    concat(loop(lst.toList).filterNot(_ == empty), whitespace)

  protected def lines(lst: List[Doc]): Doc =
    concat(lst, newline)

  protected def concat(lst: List[Doc], sep: Doc): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        head + sep + cs(tail)

  protected def horizontalConncat(lst: List[Doc]): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        head + horizontalConncat(tail)

  protected def brace(d: Doc): Doc =
    group(text("{") + nest(1, maybeNewline + d) + maybeNewline + text("}"))

  protected def bracket(d: Doc): Doc =
    group(text("[") + nest(1, maybeNewline + d) + maybeNewline + text("]"))

  protected def paren(d: Doc): Doc =
    group(text("(") + nest(1, maybeNewline + d) + maybeNewline + text(")"))

end CodeFormatter

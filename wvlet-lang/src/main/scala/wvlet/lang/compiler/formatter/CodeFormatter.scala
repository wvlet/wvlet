package wvlet.lang.compiler.formatter

import wvlet.lang.compiler.DBType

import scala.annotation.tailrec

case class CodeFormatterConfig(
    indentWidth: Int = 2,
    maxLineWidth: Int = 80,
    preserveNewLines: Int = 1,
    addTrailingCommaToItemList: Boolean = true,
    fitToLine: Boolean = true,
    sqlDBType: DBType = DBType.DuckDB
)

/**
  * A code formatter algorithm and data structure for representing code blocks. This algorithm is
  * derived from Philip Wadler's "A Prettier Printer" algorithm.
  */
object CodeFormatter:

  sealed trait Doc:
    override def toString: String = render()
    def render(conf: CodeFormatterConfig = CodeFormatterConfig()): String = CodeFormatter(conf)
      .render(0, this)

    def isEmpty: Boolean = this eq empty

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
        case NewLine | WhiteSpaceOrNewline | LineBreak =>
          1
        case OptNewLine =>
          0
        case HList(d1, d2) =>
          d1.length + d2.length
        case VList(d1, d2) =>
          d1.length + 1 + d2.length
        case Nest(d) =>
          d.length
        case Block(d) =>
          d.length + 1
        case Group(d) =>
          d.length

    def flatten: Doc =
      this match
        case NewLine | WhiteSpaceOrNewline =>
          whitespace
        case OptNewLine =>
          empty
        case LineBreak =>
          this
        case HList(d1, d2) =>
          d1.flatten + d2.flatten
        case VList(d1, d2) =>
          d1.flatten + whitespace + d2.flatten
        case Nest(d) =>
          d.flatten
        case Block(d) =>
          whitespace + d.flatten
        case Group(d) =>
          d.flatten
        case _ =>
          this

    // Concat docs horizontally
    def +(s: String): Doc = this.+(text(s))

    // Concat docs horizontally
    def +(d2: Doc): Doc =
      this match
        case Text("") =>
          d2
        case _ =>
          d2 match
            case Text("") =>
              this
            case _ =>
              HList(this, d2)

    // Concat docs horizontally if present
    def +(d2: Option[Doc]): Doc =
      d2 match
        case Some(d) =>
          this + d
        case None =>
          this

    // Concat docs vertically
    def /(s: String): Doc = this./(text(s))

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
              VList(this, d2)

    // Concat docs vertically if present
    def /(d2: Option[Doc]): Doc =
      d2 match
        case Some(d) =>
          this / d
        case None =>
          this

    def toDoc: Doc

    def pp: String = CodeFormatter(CodeFormatterConfig(fitToLine = true)).format(this.toDoc)
  end Doc

  case class Text(s: String) extends Doc:
    override def toDoc: Doc = text(s"'${s}'")

  case object NewLine extends Doc:
    override def toDoc: Doc = text("NL")

  // Optional line break
  case object OptNewLine extends Doc:
    override def toDoc: Doc = text("NL?")

  // A line break that is always preserved
  case object LineBreak extends Doc:
    override def toDoc: Doc = text("LB")

  case object WhiteSpaceOrNewline extends Doc:
    override def toDoc: Doc = text("<break>")

  // Horizontally concatenated docs
  case class HList(d1: Doc, d2: Doc) extends Doc:
    override def toDoc: Doc = group(
      text("HList") + text("(") + maybeNewline + nest(cs(List(d1.toDoc, d2.toDoc)) + text(")"))
    )

  // Vertically concatenated docs. This break is always preserved
  case class VList(d1: Doc, d2: Doc) extends Doc:
    override def toDoc: Doc = group(
      text("VList") + text("(") + maybeNewline + nest(cs(List(d1.toDoc, d2.toDoc)) + text(")"))
    )

  // 1-level indented block
  case class Nest(d: Doc) extends Doc:
    override def toDoc: Doc = group(
      text(s"Nest") + text("(") + maybeNewline + nest(d.toDoc + text(")"))
    )

  // Nested code block wrapped with newlines or preceded with a single whitespace
  case class Block(d: Doc) extends Doc:
    override def toDoc: Doc = group(
      text("Block") + text("(") + maybeNewline + nest(d.toDoc + text(")"))
    )

  // Group is a unit for compacting the doc into a single line if possible
  case class Group(d: Doc) extends Doc:
    override def toDoc: Doc = group(
      text("Group") + text("(") + maybeNewline + nest(d.toDoc + text(")"))
    )

  // Convenient operators
  inline def text(s: String): Doc     = Text(s)
  inline def newline: Doc             = NewLine
  inline def linebreak: Doc           = LineBreak
  inline def maybeNewline: Doc        = OptNewLine
  inline def whitespaceOrNewline: Doc = WhiteSpaceOrNewline
  inline def nest(d: Doc): Doc        = Nest(d)
  inline def group(d: Doc): Doc       = Group(d)
  // Create a new block with possible newlines before and after the block
  inline def block(d: Doc): Doc = Block(d)
  val whitespace: Doc           = Text(" ")
  val empty: Doc                = Text("")

  private def toDoc(x: Any): Doc =
    x match
      case d: Doc =>
        d
      case s: String =>
        text(s)
      case Some(x) =>
        toDoc(x)
      case None =>
        empty
      case s: Seq[?] =>
        concat(s.map(toDoc).toList)
      case other =>
        empty

  /**
    * Comma-separate list of the given Doc list
    *
    * @param lst
    * @return
    */
  def cs(lst: Any*): Doc = concat(to_list(lst), text(",") + whitespaceOrNewline)

  private def to_list(x: Any*): List[Doc] =
    def iter(lst: List[Any]): List[Doc] =
      lst match
        case Nil =>
          Nil
        case head :: tail =>
          head match
            case s: Seq[?] =>
              iter(s.toList) ++ iter(tail)
            case a: Array[?] =>
              iter(a.toList) ++ iter(tail)
            case _ =>
              toDoc(head) :: iter(tail)

    iter(x.toList).filterNot(_ == empty)

  /**
    * Concatenate items with a whitespace separator
    *
    * @param lst
    * @return
    */
  def ws(lst: Any*): Doc = concat(to_list(lst*), whitespace)

  def lines(lst: List[Doc]): Doc = concat(lst, newline)

  def concat(lst: List[Doc], sep: Doc): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        head + sep + concat(tail, sep)

  def concat(lst: List[Doc]): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        head + concat(tail)

  // Create subexpression block wrapped with braces
  def indentedBrace(d: Doc): Doc = text("{") + nest(linebreak + d) + linebreak + text("}")
  def indentedParen(d: Doc): Doc = text("(") + nest(linebreak + d) + linebreak + text(")")

  def brace(d: Doc): Doc   = group(text("{") + lineBlock(d) + text("}"))
  def bracket(d: Doc): Doc = group(text("[") + lineBlock(d) + text("]"))
  def paren(d: Doc): Doc   = group(text("(") + lineBlock(d) + text(")"))

  /**
    * Code block using braces with nested layout or space separated layout
    * {{{
    *   xxxx {
    *     (block)
    *   }
    * }}}
    *
    * or
    *
    * {{{
    *   xxxx { (block) }
    * }}}
    * @param d
    * @return
    */
  def codeBlock(d: Doc): Doc = group(
    text("{") + nest(whitespaceOrNewline + d) + whitespaceOrNewline + text("}")
  )

  /**
    * Code block using parentheses with nested layout or space separated layout
    * {{{
    *   xxxx (
    *     (block)
    *   )
    * }}}
    *
    * or
    *
    * {{{
    *   xxxx ( (block) )
    * }}}
    *
    * @param d
    * @return
    */
  def parenBlock(d: Doc): Doc = group(
    text("(") + nest(whitespaceOrNewline + d) + whitespaceOrNewline + text(")")
  )

  /**
    * Create a new nested block with possible newlines before and after the block
    * @param d
    * @return
    */
  def lineBlock(d: Doc): Doc = nest(maybeNewline + d) + maybeNewline

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
      case NewLine | WhiteSpaceOrNewline | OptNewLine | LineBreak =>
        doc
      case HList(d1, d2) =>
        best(level, d1) + best(level, d2)
      case VList(d1, d2) =>
        best(level, d1) / best(level, d2)
      case Nest(d) =>
        nest(best(level + 1, d))
      case Block(d) =>
        block(best(level + 1, d))
      case Group(d) =>
        lazy val oneline = d.flatten
        if config.fitToLine && fits(level, oneline) then
          oneline
        else
          best(level, d)

  def format(doc: Doc): String =
    val formattedDoc = best(0, doc)
    render(0, formattedDoc)

  def render(nestingLevel: Int, d: Doc): String =
    def indent: String = " " * nestingLevel * config.indentWidth

    d match
      case Text(s) =>
        s
      case NewLine | WhiteSpaceOrNewline | OptNewLine | LineBreak =>
        s"\n${indent}"
      case HList(d1, d2) =>
        val r1 = render(nestingLevel, d1)
        val r2 = render(nestingLevel, d2)
        s"${r1}${r2}"
      case VList(d1, d2) =>
        val r1 = render(nestingLevel, d1)
        val r2 = render(nestingLevel, d2)
        s"${r1}\n${indent}${r2}"
      case Nest(d) =>
        render(nestingLevel + 1, d)
      case Block(d) =>
        render(nestingLevel + 1, newline + d + newline)
      case Group(d) =>
        lazy val flat = render(nestingLevel, d.flatten)
        if config.fitToLine &&
          nestingLevel * config.indentWidth + flat.length <= config.maxLineWidth
        then
          flat
        else
          render(nestingLevel, d)

  end render

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

  protected def append(lst: List[Doc], separator: Doc): Doc =
    lst match
      case Nil =>
        empty
      case head :: Nil =>
        head
      case head :: tail =>
        (head / separator) / append(tail, separator)

end CodeFormatter

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

import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}

/** Inline markdown: text, `code`, **bold**, and [links](href) */
enum MdInline:
  case Text(s: String)
  case Code(s: String)
  case Bold(s: String)
  case Link(label: String, href: String)

/** Block markdown: headings, paragraphs, bullet lists, and fenced code */
enum MdBlock:
  case Heading(level: Int, content: Seq[MdInline])
  case Paragraph(content: Seq[MdInline])
  case Bullets(items: Seq[Seq[MdInline]])
  case CodeBlock(text: String)

/**
  * The markdown of notebook text cells: a small subset, rendered as DOM elements (never as HTML
  * text), so a cell can't inject markup
  */
object Markdown:

  def parse(text: String): Seq[MdBlock] =
    val lines                = text.split("\n", -1).toIndexedSeq
    val blocks               = Seq.newBuilder[MdBlock]
    var i                    = 0
    def isBullet(l: String)  = l.startsWith("- ") || l.startsWith("* ")
    def isHeading(l: String) = l.matches("^#{1,4} .*")
    while i < lines.length do
      val l = lines(i)
      if l.trim.isEmpty then
        i += 1
      else if l.trim.startsWith("```") then
        val body = lines.drop(i + 1).takeWhile(!_.trim.startsWith("```"))
        blocks += MdBlock.CodeBlock(body.mkString("\n"))
        i += body.size + 2
      else if isHeading(l) then
        val level = l.takeWhile(_ == '#').length
        blocks += MdBlock.Heading(level, inlines(l.drop(level).trim))
        i += 1
      else if isBullet(l) then
        val items = lines.drop(i).takeWhile(isBullet)
        blocks += MdBlock.Bullets(items.map(x => inlines(x.drop(2).trim)))
        i += items.size
      else
        val para = lines
          .drop(i)
          .takeWhile(x =>
            x.trim.nonEmpty && !isHeading(x) && !isBullet(x) && !x.trim.startsWith("```")
          )
        blocks += MdBlock.Paragraph(inlines(para.map(_.trim).mkString(" ")))
        i += para.size
    blocks.result()

  end parse

  /** Inline spans of a line: `code` first (its text is literal), then **bold** and links */
  def inlines(s: String): Seq[MdInline] =
    val out           = Seq.newBuilder[MdInline]
    val sb            = StringBuilder()
    def flush(): Unit =
      if sb.nonEmpty then
        out += MdInline.Text(sb.result())
        sb.clear()
    var i = 0
    while i < s.length do
      val c = s.charAt(i)
      if c == '`' && s.indexOf('`', i + 1) > i then
        val end = s.indexOf('`', i + 1)
        flush()
        out += MdInline.Code(s.substring(i + 1, end))
        i = end + 1
      else if s.startsWith("**", i) && s.indexOf("**", i + 2) > i then
        val end = s.indexOf("**", i + 2)
        flush()
        out += MdInline.Bold(s.substring(i + 2, end))
        i = end + 2
      else if c == '[' then
        val close = s.indexOf("](", i)
        val end   =
          if close > i then
            s.indexOf(')', close)
          else
            -1
        if close > i && end > close then
          flush()
          out += MdInline.Link(s.substring(i + 1, close), s.substring(close + 2, end))
          i = end + 1
        else
          sb += c
          i += 1
      else
        sb += c
        i += 1
    flush()
    out.result()

  end inlines

  private def renderInline(xs: Seq[MdInline]): Seq[RxElement] = xs.map {
    case MdInline.Text(s) =>
      span(s)
    case MdInline.Code(s) =>
      code(highlight(s))
    case MdInline.Bold(s) =>
      strong(s)
    case MdInline.Link(label, h) =>
      // In-page links (#example=..., #function=...) open in the playground; web links in a new
      // tab. Other schemes (javascript:, data:) from a shared notebook show as plain text
      if h.startsWith("#") then
        a(href -> h, label)
      else if h.startsWith("https://") || h.startsWith("http://") then
        a(href -> h, target -> "_blank", rel -> "noopener", label)
      else
        span(label)
  }

  /** A token of inline code, with its CSS class (empty for plain text) */
  case class Token(text: String, kind: String)

  private val typeNames = Set(
    "int",
    "long",
    "float",
    "real",
    "double",
    "decimal",
    "boolean",
    "string",
    "date",
    "timestamp",
    "json",
    "array",
    "map",
    "any",
    "numeric",
    "null",
    "sql",
    "A",
    "K",
    "V",
    "T"
  )

  /**
    * Light coloring of inline code, e.g. a signature `string.substring(start: int): string`: a name
    * before `(` or after `.` is a function, a name after `:` (or a type name) is a type, and quoted
    * text and numbers are literals
    */
  def tokens(s: String): Seq[Token] =
    val out                = Seq.newBuilder[Token]
    var i                  = 0
    def prevNonSpace: Char =
      var j = i - 1
      while j >= 0 && s.charAt(j) == ' ' do
        j -= 1
      if j >= 0 then
        s.charAt(j)
      else
        ' '
    while i < s.length do
      val c = s.charAt(i)
      if c == '\'' || c == '"' then
        val end =
          s.indexOf(c, i + 1) match
            case -1 =>
              s.length
            case e =>
              e + 1
        out += Token(s.substring(i, end), "string")
        i = end
      else if c.isDigit then
        val end =
          s.indexWhere(ch => !(ch.isDigit || ch == '.'), i) match
            case -1 =>
              s.length
            case e =>
              e
        out += Token(s.substring(i, end), "number")
        i = end
      else if c.isLetter || c == '_' then
        val end =
          s.indexWhere(ch => !(ch.isLetterOrDigit || ch == '_'), i) match
            case -1 =>
              s.length
            case e =>
              e
        val word = s.substring(i, end)
        val next =
          if end < s.length then
            s.charAt(end)
          else
            ' '
        val kind =
          if prevNonSpace == ':' || (typeNames(word) && next != '(' && prevNonSpace != '.') then
            "type"
          else if next == '(' || prevNonSpace == '.' then
            "fn"
          else
            ""
        out += Token(word, kind)
        i = end
      else
        out += Token(c.toString, "punct")
        i += 1
      end if
    end while
    // Merge neighbors of the same kind, so the DOM stays small
    out
      .result()
      .foldLeft(Vector.empty[Token]) { (acc, t) =>
        acc.lastOption match
          case Some(l) if l.kind == t.kind =>
            acc.init :+ Token(l.text + t.text, t.kind)
          case _ =>
            acc :+ t
      }

  end tokens

  private def highlight(s: String): Seq[RxElement] = tokens(s).map { t =>
    if t.kind.isEmpty then
      span(t.text)
    else
      span(cls -> s"pg-tok-${t.kind}", t.text)
  }

  def render(text: String): RxElement = div(
    cls -> "pg-md",
    parse(text).map {
      case MdBlock.Heading(level, c) =>
        level match
          case 1 =>
            h1(renderInline(c))
          case 2 =>
            h2(renderInline(c))
          case 3 =>
            h3(renderInline(c))
          case _ =>
            h4(renderInline(c))
      case MdBlock.Paragraph(c) =>
        p(renderInline(c))
      case MdBlock.Bullets(items) =>
        ul(items.map(i => li(renderInline(i))))
      case MdBlock.CodeBlock(t) =>
        pre(code(t))
    }
  )

end Markdown

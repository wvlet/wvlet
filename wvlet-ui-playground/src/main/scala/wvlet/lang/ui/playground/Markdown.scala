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
      code(s)
    case MdInline.Bold(s) =>
      strong(s)
    case MdInline.Link(label, h) =>
      // In-page links (#example=..., #function=...) open in the playground; others in a new tab
      if h.startsWith("#") then
        a(href -> h, label)
      else
        a(href -> h, target -> "_blank", rel -> "noopener", label)
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

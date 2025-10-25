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
package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span
import wvlet.lang.compiler.Context
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

/**
  * MarkdownWriter converts MarkdownDocument CST back to markdown text. Supports two modes:
  *   - Span-based extraction (perfect roundtrip from original source)
  *   - Structure-based reconstruction (validates CST structure)
  */
class MarkdownWriter(using ctx: Context) extends LogSupport:

  /**
    * Write markdown from CST by extracting text from spans (perfect roundtrip)
    */
  def writeFromSpans(doc: MarkdownDocument): String =
    val ctx = summon[Context]
    ctx.compilationUnit.text(doc.span)

  /**
    * Write markdown from CST by reconstructing from structure (validates CST)
    */
  def writeFromStructure(doc: MarkdownDocument): String =
    val sb = StringBuilder()
    doc
      .blocks
      .foreach { block =>
        writeBlock(block, sb)
        // Add newline after each block
        sb.append("\n")
      }
    sb.toString

  private def writeBlock(block: MarkdownBlock, sb: StringBuilder): Unit =
    block match
      case heading: MarkdownHeading =>
        writeHeading(heading, sb)
      case codeBlock: MarkdownCodeBlock =>
        writeCodeBlock(codeBlock, sb)
      case para: MarkdownParagraph =>
        writeParagraph(para, sb)
      case list: MarkdownList =>
        writeList(list, sb)
      case blockquote: MarkdownBlockquote =>
        writeBlockquote(blockquote, sb)
      case hr: MarkdownHorizontalRule =>
        sb.append("---")
      case blank: MarkdownBlankLine =>
        sb.append("")
      case text: MarkdownText =>
        sb.append(text.raw)
      case item: MarkdownListItem =>
        writeListItem(item, sb)

  private def writeHeading(heading: MarkdownHeading, sb: StringBuilder): Unit =
    val hashes = "#" * heading.level
    sb.append(hashes)
    sb.append(" ")
    sb.append(heading.text)

  private def writeCodeBlock(codeBlock: MarkdownCodeBlock, sb: StringBuilder): Unit =
    sb.append("```")
    codeBlock
      .language
      .foreach { lang =>
        sb.append(lang)
      }
    sb.append("\n")
    sb.append(codeBlock.code)
    sb.append("\n```")

  private def writeParagraph(para: MarkdownParagraph, sb: StringBuilder): Unit = sb.append(para.raw)

  private def writeList(list: MarkdownList, sb: StringBuilder, indent: Int = 0): Unit = list
    .items
    .zipWithIndex
    .foreach { case (item, idx) =>
      sb.append(" " * indent)
      if list.ordered then
        sb.append(s"${idx + 1}. ")
      else
        sb.append("- ")
      sb.append(item.raw)
      sb.append("\n")
      item
        .blocks
        .foreach {
          case nested: MarkdownList =>
            writeList(nested, sb, indent + 2)
          case block =>
            sb.append(" " * (indent + 2))
            writeBlock(block, sb)
            sb.append("\n")
        }
    }

  private def writeListItem(item: MarkdownListItem, sb: StringBuilder): Unit =
    sb.append("- ")
    sb.append(item.raw)

  private def writeBlockquote(blockquote: MarkdownBlockquote, sb: StringBuilder): Unit =
    sb.append("> ")
    sb.append(blockquote.raw)

end MarkdownWriter

object MarkdownWriter:
  /**
    * Write markdown from CST (span-based extraction for perfect roundtrip)
    */
  def write(doc: MarkdownDocument)(using ctx: Context): String = MarkdownWriter().writeFromSpans(
    doc
  )

  /**
    * Write markdown from CST structure (validates structure correctness)
    */
  def writeStructure(doc: MarkdownDocument)(using ctx: Context): String = MarkdownWriter()
    .writeFromStructure(doc)

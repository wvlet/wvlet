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
package wvlet.lang.model.expr

import wvlet.lang.api.Span
import wvlet.lang.compiler.{CompilationUnit, Context}
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.plan.{LeafPlan, LogicalPlan}

/**
  * Markdown document root node (CST approach - stores only spans, text extracted on-demand)
  */
case class MarkdownDocument(blocks: List[MarkdownBlock], span: Span) extends Expression:
  override def children: Seq[Expression] = blocks

  /**
    * Extract full source text from span
    */
  def sourceText(using ctx: Context): String =
    ctx.compilationUnit.sourceFile.getContent.slice(span.start, span.end).mkString

/**
  * Base trait for markdown block elements
  */
sealed trait MarkdownBlock extends Expression with LeafExpression:
  def span: Span

  /**
    * Extract source text for this block
    */
  def sourceText(using ctx: Context): String =
    ctx.compilationUnit.sourceFile.getContent.slice(span.start, span.end).mkString

/**
  * Heading with level (1-6 corresponding to #, ##, etc.)
  */
case class MarkdownHeading(level: Int, span: Span) extends MarkdownBlock:
  /**
    * Extract heading text without # markers
    */
  def text(using ctx: Context): String =
    val full = sourceText
    // Remove leading # characters and trim
    full.dropWhile(_ == '#').trim

/**
  * Paragraph of text (may contain inline formatting in future)
  */
case class MarkdownParagraph(span: Span) extends MarkdownBlock

/**
  * Code block with optional language hint
  */
case class MarkdownCodeBlock(language: Option[String], span: Span) extends MarkdownBlock:
  /**
    * Extract code without fence markers
    */
  def code(using ctx: Context): String =
    val full = sourceText
    // Remove opening ``` and closing ```
    val lines = full.split("\n")
    if lines.length > 2 then
      lines.slice(1, lines.length - 1).mkString("\n")
    else
      ""

/**
  * List (ordered or unordered)
  */
case class MarkdownList(ordered: Boolean, items: List[MarkdownListItem], span: Span)
    extends MarkdownBlock:
  override def children: Seq[Expression] = items

/**
  * List item
  */
case class MarkdownListItem(span: Span) extends MarkdownBlock

/**
  * Blockquote
  */
case class MarkdownBlockquote(span: Span) extends MarkdownBlock

/**
  * Horizontal rule (---, ***, ___)
  */
case class MarkdownHorizontalRule(span: Span) extends MarkdownBlock

/**
  * Blank line (for spacing preservation)
  */
case class MarkdownBlankLine(span: Span) extends MarkdownBlock

/**
  * Text block (fallback for unparsed content)
  */
case class MarkdownText(span: Span) extends MarkdownBlock

/**
  * Wrapper plan for markdown documents (since MarkdownDocument extends Expression, not LogicalPlan)
  */
case class MarkdownPlan(doc: MarkdownDocument, span: Span) extends LogicalPlan with LeafPlan:
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

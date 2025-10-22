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
package wvlet.lang.model.plan

import wvlet.lang.api.{LinePosition, Span}
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.expr.Expression
import wvlet.lang.api.Span.NoSpan

/**
  * AST nodes for Markdown documents Represents the structure of parsed Markdown following
  * CommonMark specification
  */

/**
  * Root node representing a complete Markdown document
  */
case class MarkdownDocument(
    blocks: List[MarkdownBlock],
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends LogicalPlan:
  override def children: List[LogicalPlan]     = blocks
  override def relationType: RelationType      = DataType.EmptyRelationType
  override def inputRelationType: RelationType = DataType.EmptyRelationType

/**
  * Base trait for block-level Markdown elements
  */
sealed trait MarkdownBlock extends LogicalPlan:
  override def relationType: RelationType      = DataType.EmptyRelationType
  override def inputRelationType: RelationType = DataType.EmptyRelationType

/**
  * Heading (# through ######)
  */
case class Heading(
    level: Int,
    text: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = Nil

/**
  * Code block with optional language hint
  */
case class CodeBlock(
    language: Option[String],
    code: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = Nil

/**
  * Blockquote (> quoted text)
  */
case class Blockquote(
    content: List[MarkdownBlock],
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = content

/**
  * List (ordered or unordered)
  */
case class ListBlock(
    items: List[ListItem],
    ordered: Boolean = false,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = items

/**
  * List item
  */
case class ListItem(
    content: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = Nil

/**
  * Horizontal rule (---, ***, ___)
  */
case class HorizontalRule(nodeLocation: LinePosition = LinePosition.NoPosition, span: Span = NoSpan)
    extends MarkdownBlock:
  override def children: List[LogicalPlan] = Nil

/**
  * Paragraph containing inline content
  */
case class Paragraph(
    content: MarkdownExpression,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownBlock:
  override def children: List[LogicalPlan] = Nil

/**
  * Base trait for inline Markdown elements (extends Expression, not LogicalPlan) This allows proper
  * nesting: MarkdownBold(MarkdownLink(...)), MarkdownItalic(MarkdownBold(...)), etc.
  */
sealed trait MarkdownExpression extends Expression:
  override def children: Seq[Expression] = Seq.empty
  override def dataType: DataType        = DataType.StringType

/**
  * Convenience alias for backward compatibility
  */
type InlineContent = MarkdownExpression

/**
  * Plain text
  */
case class MarkdownText(
    text: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression

/**
  * Bold text (**text** or __text__) Can contain nested inline elements
  */
case class MarkdownBold(
    content: MarkdownExpression,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression:
  override def children: Seq[Expression] = Seq(content)

/**
  * Italic text (*text* or _text_) Can contain nested inline elements
  */
case class MarkdownItalic(
    content: MarkdownExpression,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression:
  override def children: Seq[Expression] = Seq(content)

/**
  * Inline code span (`code`)
  */
case class MarkdownCodeSpan(
    code: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression

/**
  * Link [text](url) Text can contain inline formatting
  */
case class MarkdownLink(
    text: MarkdownExpression,
    url: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression:
  override def children: Seq[Expression] = Seq(text)

/**
  * Image ![alt](url)
  */
case class MarkdownImage(
    altText: String,
    url: String,
    nodeLocation: LinePosition = LinePosition.NoPosition,
    span: Span = NoSpan
) extends MarkdownExpression

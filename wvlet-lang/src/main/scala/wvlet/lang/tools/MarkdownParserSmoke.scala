package wvlet.lang.tools

import wvlet.lang.compiler.{CompilationUnit, MemoryFile, SourceFile}
import wvlet.lang.compiler.parser.MarkdownParser
import wvlet.lang.model.plan.{
  MarkdownBold,
  MarkdownDocument,
  MarkdownItalic,
  MarkdownSequence,
  Paragraph
}

object MarkdownParserSmoke:
  @main
  def run(): Unit =
    val input  = "This paragraph has **bold** and *italic*."
    val file   = MemoryFile("smoke.md", input)
    val source = SourceFile(file)
    val unit   = CompilationUnit(source)
    val parser = MarkdownParser(unit)
    val doc    = parser.parse().asInstanceOf[MarkdownDocument]

    val paragraphs = doc
      .blocks
      .collect { case p: Paragraph =>
        p
      }
    assert(paragraphs.size == 1, s"expected 1 paragraph, got ${paragraphs.size}")
    paragraphs.head.content match
      case seq: MarkdownSequence =>
        val hasBold   = seq.parts.exists(_.isInstanceOf[MarkdownBold])
        val hasItalic = seq.parts.exists(_.isInstanceOf[MarkdownItalic])
        assert(hasBold, "expected a MarkdownBold inline element in the paragraph")
        assert(hasItalic, "expected a MarkdownItalic inline element in the paragraph")
      case other =>
        sys.error(s"expected MarkdownSequence, got ${other.getClass.getSimpleName}")

    println(
      "OK: Markdown paragraph accumulates inline elements into one Paragraph with sequence content."
    )

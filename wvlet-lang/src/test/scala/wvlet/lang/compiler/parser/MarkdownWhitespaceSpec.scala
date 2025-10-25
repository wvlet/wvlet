package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.model.expr.MarkdownParagraph

class MarkdownWhitespaceSpec extends AirSpec:
  test("preserve leading whitespace and blank-line separation") {
    val markdown =
      """    first paragraph
        |  
        |
        |Second paragraph
        |""".stripMargin
    val unit   = CompilationUnit(SourceFile.fromString("whitespace.md", markdown))
    val parser = MarkdownParser(unit)
    val doc    = parser.parse()
    val paragraphs = doc
      .blocks
      .collect { case p: MarkdownParagraph =>
        p
      }
    paragraphs.length shouldBe 2

    val first = paragraphs.head.raw
    first shouldBe "    first paragraph"

    val second = paragraphs(1).raw
    second.startsWith("Second paragraph") shouldBe true
    second.trim shouldBe "Second paragraph"
  }

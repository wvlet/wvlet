package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.SourceFile
import wvlet.lang.model.expr.MarkdownList
import wvlet.lang.model.expr.MarkdownListItem

class MarkdownListSpec extends AirSpec:
  test("parse nested unordered lists") {
    val markdown =
      """- Parent
        |  - Child 1
        |  - Child 2
        |- Sibling
        |""".stripMargin

    val unit   = CompilationUnit(SourceFile.fromString("nested.md", markdown))
    val parser = MarkdownParser(unit)
    val doc    = parser.parse()

    doc.blocks.headOption shouldBe defined
    val list = doc.blocks.head.asInstanceOf[MarkdownList]
    list.items.length shouldBe 2
    list.items.map(_.raw.trim) shouldBe Seq("Parent", "Sibling")

    val firstItem = list.items.head
    val nested    = firstItem
      .blocks
      .collect { case l: MarkdownList =>
        l
      }
    nested.length shouldBe 1
    nested.head.items.map(_.raw.trim) shouldBe Seq("Child 1", "Child 2")
  }

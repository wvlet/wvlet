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
package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, Context, MemoryFile, SourceFile}
import wvlet.lang.model.plan.*

class MarkdownParserTest extends AirSpec:

  private def parse(input: String): MarkdownDocument =
    val file       = MemoryFile("test.md", input)
    val sourceFile = SourceFile(file)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    parser.parse() match
      case doc: MarkdownDocument =>
        doc
      case other =>
        throw new Exception(s"Expected MarkdownDocument but got ${other.getClass.getName}")

  test("parse heading"):
    val doc = parse("# Heading 1\n## Heading 2\n### Heading 3")
    val headings = doc
      .blocks
      .collect { case h: Heading =>
        h
      }
    headings.size shouldBe 3
    headings(0).level shouldBe 1
    headings(0).text shouldContain "Heading 1"
    headings(1).level shouldBe 2
    headings(2).level shouldBe 3

  test("parse code block"):
    val input =
      """```scala
                  |val x = 1
                  |val y = 2
                  |```""".stripMargin
    val doc = parse(input)
    val codeBlocks = doc
      .blocks
      .collect { case cb: CodeBlock =>
        cb
      }
    codeBlocks.size shouldBe 1
    codeBlocks(0).language shouldBe Some("scala")
    codeBlocks(0).code shouldContain "val x = 1"

  test("parse code block without language"):
    val input =
      """```
                  |code here
                  |```""".stripMargin
    val doc = parse(input)
    val codeBlocks = doc
      .blocks
      .collect { case cb: CodeBlock =>
        cb
      }
    codeBlocks.size shouldBe 1
    codeBlocks(0).language shouldBe None

  test("parse paragraph with inline content"):
    val doc = parse("This is plain text")
    val paragraphs = doc
      .blocks
      .collect { case p: Paragraph =>
        p
      }
    paragraphs.size shouldBe 1
    paragraphs(0).content shouldMatch { case _: MarkdownExpression =>
    }

  test("parse list"):
    val input =
      """- Item 1
                  |- Item 2
                  |- Item 3""".stripMargin
    val doc = parse(input)
    val lists = doc
      .blocks
      .collect { case l: ListBlock =>
        l
      }
    lists.size shouldBe 1
    lists(0).items.size shouldBe 3

  test("parse blockquote"):
    val doc = parse("> This is a quote")
    val quotes = doc
      .blocks
      .collect { case bq: Blockquote =>
        bq
      }
    quotes.size shouldBe 1

  test("parse horizontal rule"):
    val doc = parse("---\n")
    val hrs = doc
      .blocks
      .collect { case hr: HorizontalRule =>
        hr
      }
    hrs.size shouldBe 1

  test("parse mixed content"):
    val input =
      """# Title
                  |
                  |This is a paragraph.
                  |
                  |```python
                  |print("hello")
                  |```
                  |
                  |- List item
                  |""".stripMargin
    val doc = parse(input)
    doc.blocks.nonEmpty shouldBe true
    doc.blocks.exists(_.isInstanceOf[Heading]) shouldBe true
    doc.blocks.exists(_.isInstanceOf[CodeBlock]) shouldBe true
    doc.blocks.exists(_.isInstanceOf[ListBlock]) shouldBe true

  test("parse empty document"):
    val doc = parse("")
    doc.blocks.isEmpty shouldBe true

  test("parse document with only whitespace"):
    val doc = parse("   \n\n  \n")
    doc.blocks.isEmpty shouldBe true

end MarkdownParserTest

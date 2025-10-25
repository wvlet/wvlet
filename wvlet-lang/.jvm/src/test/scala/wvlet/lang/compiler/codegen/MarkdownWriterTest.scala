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

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.{CompilationUnit, Context, LocalFile, SourceFile}
import wvlet.lang.compiler.parser.MarkdownParser

/**
  * Roundtrip tests for MarkdownWriter using spec/markdown files
  */
class MarkdownWriterTest extends AirSpec:

  /**
    * Test roundtrip for a markdown file from spec/markdown
    */
  private def testRoundtrip(fileName: String): Unit =
    val file = LocalFile(s"spec/markdown/${fileName}")
    if !file.exists then
      fail(s"Test file not found: ${fileName}")

    val originalText = file.contentString
    val sourceFile   = SourceFile.fromFile(file)
    val unit         = CompilationUnit(sourceFile)
    val parser       = MarkdownParser(unit)
    val doc          = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    // Span-based extraction should be perfect roundtrip
    val result = writer.writeFromSpans(doc)

    // Verify roundtrip
    result shouldBe originalText

  test("roundtrip hello.md") {
    testRoundtrip("hello.md")
  }

  test("roundtrip heading.md") {
    testRoundtrip("heading.md")
  }

  test("roundtrip paragraph.md") {
    testRoundtrip("paragraph.md")
  }

  test("roundtrip code-block.md") {
    testRoundtrip("code-block.md")
  }

  test("roundtrip list.md") {
    testRoundtrip("list.md")
  }

  test("roundtrip code-with-title.md") {
    testRoundtrip("code-with-title.md")
  }

  test("validate span accuracy") {
    val file         = LocalFile("spec/markdown/hello.md")
    val originalText = file.contentString
    val sourceFile   = SourceFile.fromFile(file)
    val unit         = CompilationUnit(sourceFile)
    val parser       = MarkdownParser(unit)
    val doc          = parser.parse()

    // Verify span covers the entire document
    doc.span.start shouldBe 0
    doc.span.end shouldBe originalText.length

    // Verify we can extract text from spans
    val text = sourceFile.getContent.slice(doc.span.start, doc.span.end).mkString
    text shouldBe originalText
  }

end MarkdownWriterTest

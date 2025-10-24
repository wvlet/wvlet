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
import wvlet.lang.compiler.{CompilationUnit, Context, SourceFile}
import wvlet.lang.compiler.parser.{MarkdownParser, ParserPhase}
import wvlet.lang.model.expr.{MarkdownDocument, MarkdownPlan}

/**
  * Roundtrip tests for MarkdownWriter
  */
class MarkdownWriterTest extends AirSpec:

  test("roundtrip simple heading") {
    val markdown   = "# Hello World\n"
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    // Span-based extraction should be perfect roundtrip
    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip heading with multiple levels") {
    val markdown   = """# Level 1
## Level 2
### Level 3
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip paragraph") {
    val markdown   = """This is a paragraph.

This is another paragraph.
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip code block") {
    val markdown   = """```scala
def hello() = println("Hello")
```
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip code block with language hint") {
    val markdown   = """```sql
SELECT * FROM users
```
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip mixed content") {
    val markdown   = """# Title

This is a paragraph.

```scala
val x = 1
```

## Section

Another paragraph.
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("roundtrip with blank lines") {
    val markdown   = """# Heading

Paragraph 1.


Paragraph 2 with blank line above.
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromSpans(doc)
    result shouldBe markdown
  }

  test("structure-based write for heading") {
    val markdown   = "# Hello World\n"
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    given Context = Context.NoContext
    val writer    = MarkdownWriter()

    val result = writer.writeFromStructure(doc)
    // Structure-based may differ in exact formatting but should contain content
    result shouldContain "# Hello World"
  }

  test("validate span accuracy") {
    val markdown   = """# Title
Paragraph
"""
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val unit       = CompilationUnit(sourceFile)
    val parser     = MarkdownParser(unit)
    val doc        = parser.parse()

    // Verify span covers the entire document
    doc.span.start shouldBe 0
    doc.span.end shouldBe markdown.length

    // Verify we can extract text from spans
    val text = sourceFile.getContent.slice(doc.span.start, doc.span.end).mkString
    text shouldBe markdown
  }

end MarkdownWriterTest

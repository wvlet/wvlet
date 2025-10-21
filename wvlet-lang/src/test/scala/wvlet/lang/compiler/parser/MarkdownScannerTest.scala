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
import wvlet.lang.compiler.{MemoryFile, SourceFile}

class MarkdownScannerTest extends AirSpec:

  private def scan(input: String): List[TokenData[MarkdownToken]] =
    val file       = MemoryFile("test.md", input)
    val sourceFile = SourceFile(file)
    val scanner    = MarkdownScanner(sourceFile, ScannerConfig(skipWhiteSpace = false))

    val tokens = List.newBuilder[TokenData[MarkdownToken]]
    var done   = false
    while !done do
      val token = scanner.nextToken()
      tokens += token
      if token.token == MarkdownToken.EOF then
        done = true

    tokens.result()

  test("scan heading tokens"):
    val tokens        = scan("# Heading 1\n## Heading 2")
    val headingTokens = tokens.filter(_.token == MarkdownToken.HEADING)
    headingTokens.size shouldBe 2
    headingTokens(0).str shouldContain "Heading 1"
    headingTokens(1).str shouldContain "Heading 2"

  test("scan code block"):
    val input =
      """```scala
                  |val x = 1
                  |```""".stripMargin
    val tokens      = scan(input)
    val fenceTokens = tokens.filter(_.token == MarkdownToken.FENCE_MARKER)
    fenceTokens.size shouldBe 2

  test("scan inline code span"):
    val tokens     = scan("This is `code` text")
    val codeTokens = tokens.filter(_.token == MarkdownToken.CODE_SPAN)
    codeTokens.size shouldBe 1

  test("scan bold text"):
    val tokens     = scan("This is **bold** text")
    val boldTokens = tokens.filter(_.token == MarkdownToken.BOLD)
    boldTokens.size shouldBe 1

  test("scan italic text"):
    val tokens       = scan("This is *italic* text")
    val italicTokens = tokens.filter(_.token == MarkdownToken.ITALIC)
    italicTokens.size shouldBe 1

  test("scan list items"):
    val input =
      """- Item 1
                  |- Item 2
                  |+ Item 3""".stripMargin
    val tokens     = scan(input)
    val listTokens = tokens.filter(_.token == MarkdownToken.LIST_ITEM)
    listTokens.size shouldBe 3

  test("scan blockquote"):
    val tokens      = scan("> This is a quote")
    val quoteTokens = tokens.filter(_.token == MarkdownToken.BLOCKQUOTE)
    quoteTokens.size shouldBe 1

  test("scan horizontal rule"):
    val tokens   = scan("---\n")
    val hrTokens = tokens.filter(_.token == MarkdownToken.HORIZONTAL_RULE)
    hrTokens.size shouldBe 1

  test("scan link"):
    val tokens     = scan("[text](url)")
    val linkTokens = tokens.filter(_.token == MarkdownToken.LINK)
    linkTokens.size shouldBe 1

  test("scan image"):
    val tokens      = scan("![alt](image.png)")
    val imageTokens = tokens.filter(_.token == MarkdownToken.IMAGE)
    imageTokens.size shouldBe 1

  test("scan plain text"):
    val tokens     = scan("Just plain text")
    val textTokens = tokens.filter(_.token == MarkdownToken.TEXT)
    textTokens.nonEmpty shouldBe true

end MarkdownScannerTest

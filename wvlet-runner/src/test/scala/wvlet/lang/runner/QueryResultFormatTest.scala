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
package wvlet.lang.runner

import wvlet.airspec.AirSpec

class QueryResultFormatTest extends AirSpec:
  test("unicode width") {
    for ch <- 'a' to 'Z' do
      QueryResultFormat.wcWidth(ch) shouldBe 1
    QueryResultFormat.wcWidth('東') shouldBe 2
  }

  test("truncate long strings") {
    QueryResultFormat.trimToWidth("hello", 3) shouldBe "he…"
    QueryResultFormat.trimToWidth("hello", 10) shouldBe "hello"
  }

  test("fit to width unicode strings") {
    QueryResultFormat.wcWidth("東京都") shouldBe 6
    QueryResultFormat.trimToWidth("東京都", 5) shouldBe "東京…"
    QueryResultFormat.trimToWidth("東京都", 10) shouldBe "東京都"
    QueryResultFormat.trimToWidth("東京都", 6) shouldBe "東京都"
  }

  test("strip ANSI color codes") {
    QueryResultFormat.stripAnsiCodes("hello") shouldBe "hello"
    QueryResultFormat.stripAnsiCodes("\u001b[31mred text\u001b[0m") shouldBe "red text"
    QueryResultFormat.stripAnsiCodes("\u001b[1;32mbold green\u001b[0m") shouldBe "bold green"
    QueryResultFormat.stripAnsiCodes("\u001b[31m\u001b[1mred bold\u001b[0m\u001b[0m") shouldBe
      "red bold"
  }

  test("width calculation with ANSI color codes") {
    QueryResultFormat.wcWidth("hello") shouldBe 5
    QueryResultFormat.wcWidth("\u001b[31mhello\u001b[0m") shouldBe 5
    QueryResultFormat.wcWidth("\u001b[1;32mbold\u001b[0m") shouldBe 4
    QueryResultFormat.wcWidth("東京\u001b[31m都\u001b[0m") shouldBe 6
  }

  test("trimToWidth with ANSI color codes") {
    // Test that ANSI codes are preserved during truncation
    val result = QueryResultFormat.trimToWidth("\u001b[31mhello world\u001b[0m", 8)
    result shouldBe "\u001b[31mhello w…\u001b[0m"

    // The ANSI sequence should be preserved but the visual content truncated
    val stripped = QueryResultFormat.stripAnsiCodes(result)
    stripped shouldBe "hello w…"

    QueryResultFormat.trimToWidth("\u001b[31mhello\u001b[0m", 10) shouldBe
      "\u001b[31mhello\u001b[0m"
  }

  test("alignment functions with ANSI color codes") {
    // Test that alignment functions properly handle ANSI codes
    val coloredText = "\u001b[32mactive\u001b[0m"
    val normalText  = "inactive"

    // Both should align to the same visual column width
    val aligned1 = QueryResultFormat.alignLeft(coloredText, 10)
    val aligned2 = QueryResultFormat.alignLeft(normalText, 10)

    // Visual width should be the same after alignment
    QueryResultFormat.wcWidth(aligned1) shouldBe QueryResultFormat.wcWidth(aligned2)
    QueryResultFormat.wcWidth(aligned1) shouldBe 10

    // Test right alignment
    val rightAligned1 = QueryResultFormat.alignRight(coloredText, 10)
    val rightAligned2 = QueryResultFormat.alignRight(normalText, 10)

    QueryResultFormat.wcWidth(rightAligned1) shouldBe QueryResultFormat.wcWidth(rightAligned2)
    QueryResultFormat.wcWidth(rightAligned1) shouldBe 10

    // Test centering
    val centered1 = QueryResultFormat.center(coloredText, 10)
    val centered2 = QueryResultFormat.center(normalText, 10)

    QueryResultFormat.wcWidth(centered1) shouldBe QueryResultFormat.wcWidth(centered2)
    QueryResultFormat.wcWidth(centered1) shouldBe 10
  }

end QueryResultFormatTest

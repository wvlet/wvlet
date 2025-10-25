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
import wvlet.lang.compiler.SourceFile

class MarkdownScannerTest extends AirSpec:

  test("should not emit zero-length TEXT tokens when special char starts a text run") {
    // '#' in the middle of a line should be treated as TEXT, not cause zero-length token
    val markdown   = "a#b\n"
    val sourceFile = SourceFile.fromString("test.md", markdown)
    val scanner    = MarkdownScanner(sourceFile)

    var seenEOF  = false
    var iter     = 0
    val maxIters = markdown.length + 8
    while !seenEOF && iter < maxIters do
      val t = scanner.nextToken()
      if t.token == MarkdownToken.EOF then
        seenEOF = true
      else
        // All non-EOF tokens must advance input with positive span length
        assert(t.span.end - t.span.start > 0)
      iter += 1

    seenEOF shouldBe true
  }

end MarkdownScannerTest

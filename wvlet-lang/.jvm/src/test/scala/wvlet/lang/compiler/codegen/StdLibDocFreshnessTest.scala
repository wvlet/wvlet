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

import wvlet.lang.compiler.SourceIO
import wvlet.uni.test.UniTest

/**
  * Guard against drift between the standard-library sources and the generated reference page
  * (website/docs/syntax/stdlib-reference.md). When stdlib definitions are added or changed, this
  * test fails with the command that regenerates the page.
  */
class StdLibDocFreshnessTest extends UniTest:

  test("stdlib reference doc matches the stdlib sources") {
    val generated = StdLibDocGenerator.generateMarkdown()
    val bundled   = SourceIO.readAsString(StdLibDocGenerator.targetPath)
    if bundled != generated then
      val bundledLines   = bundled.linesIterator.toIndexedSeq
      val generatedLines = generated.linesIterator.toIndexedSeq
      val firstDiff      = bundledLines
        .zipAll(generatedLines, "<missing line>", "<missing line>")
        .indexWhere(_ != _)
      val diffReport =
        if firstDiff >= 0 then
          val expected = generatedLines.lift(firstDiff).getOrElse("<missing line>")
          val actual   = bundledLines.lift(firstDiff).getOrElse("<missing line>")
          s"First difference at line ${firstDiff +
              1}:\n  checked-in: ${actual}\n  generated : ${expected}"
        else
          ""
      fail(
        s"""${StdLibDocGenerator.targetPath} is out of date (checked-in: ${bundledLines
            .size} lines, regenerated: ${generatedLines.size} lines).
           |${diffReport}
           |Regenerate it with:
           |  ${StdLibDocGenerator.regenCommand}""".stripMargin
      )
  }

end StdLibDocFreshnessTest

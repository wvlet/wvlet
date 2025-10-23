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
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

/**
  * Spec-driven tests for Markdown parser
  *
  * This test suite automatically discovers and tests all .md files in the spec/markdown directory.
  * Each .md file is parsed and validated to ensure the parser can handle various Markdown
  * constructs.
  */
trait MarkdownSpec(specPath: String) extends AirSpec:
  for
    unit <- CompilationUnit.fromPath(specPath)
    if unit.sourceFile.isMarkdown
  do
    val specName = unit.sourceFile.relativeFilePath.replaceAll("/", ":")
    test(s"parse ${specName}") {
      // Parse the markdown file
      val plan = ParserPhase.parseOnly(unit)

      // Verify it parsed as a MarkdownDocument
      plan match
        case doc: MarkdownDocument =>
          // Basic validation - should have parsed some content
          if unit.sourceFile.getContent.nonEmpty then
            doc.blocks.nonEmpty shouldBe true
            debug(s"Parsed ${doc.blocks.size} blocks")
            debug(s"Block types: ${doc.blocks.map(_.getClass.getSimpleName).mkString(", ")}")
        case other =>
          fail(s"Expected MarkdownDocument but got ${other.getClass.getName}")
    }

end MarkdownSpec

/**
  * Test all markdown files in spec/markdown directory
  */
class MarkdownSpecBasic extends MarkdownSpec("spec/markdown")

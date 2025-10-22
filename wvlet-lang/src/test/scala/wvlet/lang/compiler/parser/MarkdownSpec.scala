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
import wvlet.lang.compiler.{
  CompilationUnit,
  Compiler,
  CompilerOptions,
  LocalFile,
  SourceFile,
  WorkEnv
}
import wvlet.lang.model.plan.*

import java.io.File

/**
  * Spec-driven tests for Markdown parser
  *
  * This test suite automatically discovers and tests all .md files in the spec/markdown directory.
  * Each .md file is parsed and validated to ensure the parser can handle various Markdown
  * constructs.
  */
trait MarkdownSpec(specPath: String) extends AirSpec:

  private val workEnv = WorkEnv(path = specPath, logLevel = logger.getLogLevel)

  private val compiler = Compiler(
    CompilerOptions(
      phases = Compiler.parseOnlyPhases,
      sourceFolders = List(specPath),
      workEnv = workEnv
    )
  )

  // Get all .md files in the spec directory
  private val markdownFiles = discoverMarkdownFiles(specPath)

  // Create a test for each markdown file
  for file <- markdownFiles do
    val testName = file.getPath.replaceFirst(s"^${specPath}/", "").replaceAll("/", ":")
    test(testName) {
      val sourceFile = SourceFile(LocalFile(file.getPath))
      val unit       = CompilationUnit(sourceFile)

      // Parse the markdown file
      val plan = ParserPhase.parseOnly(unit)

      // Verify it parsed as a MarkdownDocument
      plan match
        case doc: MarkdownDocument =>
          // Basic validation - should have parsed some content
          if sourceFile.getContent.nonEmpty then
            doc.blocks.nonEmpty shouldBe true
            debug(s"Parsed ${doc.blocks.size} blocks from ${file.getName}")
            debug(s"Block types: ${doc.blocks.map(_.getClass.getSimpleName).mkString(", ")}")
        case other =>
          fail(s"Expected MarkdownDocument but got ${other.getClass.getName}")
    }

  /**
    * Discover all .md files in the given directory recursively
    */
  private def discoverMarkdownFiles(path: String): List[File] =
    val dir = new File(path)
    if !dir.exists() then
      warn(s"Directory not found: $path")
      List.empty
    else
      dir
        .listFiles()
        .toList
        .flatMap { f =>
          if f.isDirectory then
            discoverMarkdownFiles(f.getPath)
          else if f.getName.endsWith(".md") then
            List(f)
          else
            List.empty
        }

end MarkdownSpec

/**
  * Test all markdown files in spec/markdown directory
  */
class MarkdownSpecBasic extends MarkdownSpec("spec/markdown")

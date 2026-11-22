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

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.runner.connector.ConnectorCatalogs
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/**
  * A `wvlet` code fence in a Markdown file that AI agents read.
  *
  * @param file
  *   the Markdown file, relative to the repository root
  * @param line
  *   1-origin line number of the opening fence
  * @param mode
  *   how to check the snippet: `parse` (default), `run`, or `skip`
  */
case class DocSnippet(file: String, line: Int, mode: String, code: String):
  def testName: String = s"${file.replaceAll("/", ":")}:${line}"

object DocSnippet:
  private val fenceOpen = """^\s*```wvlet\b\s*(.*)$""".r

  /**
    * Extract `wvlet` code fences. The info string after `wvlet` selects the check: nothing for a
    * syntax check, `run` to compile and execute the snippet on DuckDB with its `test` assertions,
    * and `skip` for fragments that are not complete statements.
    */
  def extract(file: String, text: String): List[DocSnippet] =
    val lines    = text.linesIterator.toIndexedSeq
    val snippets = List.newBuilder[DocSnippet]
    var i        = 0
    while i < lines.size do
      lines(i) match
        case fenceOpen(info) =>
          val mode  = info.trim.split("\\s+").find(Set("run", "skip").contains).getOrElse("parse")
          val start = i
          i += 1
          val body = Seq.newBuilder[String]
          while i < lines.size && !lines(i).trim.startsWith("```") do
            body += lines(i)
            i += 1
          snippets += DocSnippet(file, start + 1, mode, body.result().mkString("\n"))
        case _ =>
      i += 1
    snippets.result()

  /**
    * Markdown files written for AI agents: the bundled skill and the agent guide page
    */
  def agentDocs: List[File] =
    val skillDir   = File("plugins/wvlet/skills")
    val skillFiles =
      if skillDir.exists() then
        Files
          .walk(skillDir.toPath)
          .iterator()
          .asScala
          .map(_.toFile)
          .filter(f => f.isFile && f.getName.endsWith(".md"))
          .toList
          .sortBy(_.getPath)
      else
        Nil
    skillFiles :+ File("website/docs/for-ai-agents.md")

end DocSnippet

/**
  * Check that every Wvlet example AI agents read compiles, so the bundled skill cannot drift from
  * the language. Fences marked `wvlet run` are executed on DuckDB and their `test` assertions are
  * evaluated.
  */
class SkillSnippetTest extends UniTest:
  private val workEnv = WorkEnv(path = "target/skill-snippets", logLevel = logger.getLogLevel)
  private val profile = Profile.defaultDuckDBProfile
  private val engine  = profile.defaultEngine
  private val dbConnectorProvider = ConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)

  override def afterAll: Unit =
    queryExecutor.close()
    dbConnectorProvider.close()

  private val snippets: List[DocSnippet] = DocSnippet
    .agentDocs
    .flatMap { f =>
      DocSnippet.extract(f.getPath, Files.readString(f.toPath))
    }

  test("find snippets in the bundled skill") {
    snippets.exists(_.file.startsWith("plugins/wvlet/skills")) shouldBe true
    snippets.exists(_.mode == "run") shouldBe true
  }

  private def newCompiler(): Compiler =
    val compiler = Compiler(
      CompilerOptions(sourceFolders = Nil, workEnv = workEnv, dbType = engine.dbType)
    )
    compiler.setDefaultCatalog(
      ConnectorCatalogs.catalogOf(
        queryExecutor.getConnector(engine),
        engine,
        WvletEngineSession.defaultCatalogName(engine).getOrElse("memory"),
        "main"
      )
    )
    compiler

  private def check(s: DocSnippet): Unit =
    s.mode match
      case "run" =>
        val unit     = CompilationUnit.fromWvletString(s.code)
        val compiler = newCompiler()
        val result   = compiler.compileSingleUnit(unit)
        result.reportAllErrors
        val queryResult = queryExecutor.executeSingle(unit, result.context.withDebugRun(true))
        debug(queryResult.toPrettyBox(maxWidth = Some(120)))
        queryResult.getError.foreach(e => throw e)
      case _ =>
        ParserPhase.parseOnly(CompilationUnit.fromWvletString(s.code))

  for s <- snippets do
    test(s.testName) {
      if s.mode == "skip" then
        ignore(s"skipped snippet at ${s.file}:${s.line}")
      try
        check(s)
      catch
        case NonFatal(e) =>
          fail(s"${s.file}:${s.line}: ${e.getMessage}\n${s.code}")
    }

end SkillSnippetTest

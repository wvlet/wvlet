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

import wvlet.uni.test.UniTest
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.runner.connector.DBConnectorProvider

import java.io.File

/**
  * Execution-side parity gate for the new Typer (issue #1764): compile every spec under spec/basic
  * with both the default TypeResolver and the new Typer using a DuckDB catalog (covering specs that
  * cannot compile standalone in wvlet-lang tests), and compare the generated SQL.
  *
  * The bounds below pin the current parity level: improvements pass automatically; regressions fail
  * the build. Tighten as the gap burns down.
  */
class TyperExecutionParityCheck extends UniTest:

  private val specPath            = "spec/basic"
  private val workEnv             = WorkEnv(path = specPath, logLevel = logger.getLogLevel)
  private val profile             = Profile.defaultDuckDBProfile
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)

  override def afterAll: Unit =
    queryExecutor.close()
    dbConnectorProvider.close()

  private def newCompiler(useNewTyper: Boolean): Compiler =
    val options  = CompilerOptions(sourceFolders = List(specPath), workEnv = workEnv)
    val compiler =
      if useNewTyper then
        Compiler.withNewTyper(options)
      else
        Compiler(options)
    compiler.setDefaultCatalog(queryExecutor.getDBConnector(profile).getCatalog("memory", "main"))
    compiler

  private def generateSQL(compiler: Compiler, path: String): Either[String, String] =
    try
      val unit   = CompilationUnit.fromFile(path)
      val result = compiler.compileSingleUnit(unit)
      Right(GenSQL.generateSQL(unit)(using result.context))
    catch
      case e: Throwable =>
        Left(s"${e.getClass.getSimpleName}: ${e.getMessage}")

  // Specs whose generated SQL contains compile-time-generated values (e.g. ulid_string), which
  // can never match across two independent compiler runs
  private val nonDeterministicSpecs = Set("ulid.wv", "val.wv")

  test("new typer must not regress SQL parity over spec/basic with a DuckDB catalog") {
    val wvFiles = Option(File(specPath).listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => f.isFile && f.getName.endsWith(".wv") && !nonDeterministicSpecs(f.getName))
      .sortBy(_.getName)

    var same       = 0
    var diff       = 0
    var newFailed  = 0
    var oldFailed  = 0
    val diffFiles  = List.newBuilder[String]
    val crashFiles = List.newBuilder[String]

    wvFiles.foreach { f =>
      val oldSQL = generateSQL(newCompiler(useNewTyper = false), f.getPath)
      val newSQL = generateSQL(newCompiler(useNewTyper = true), f.getPath)
      (oldSQL, newSQL) match
        case (Right(o), Right(n)) if o == n =>
          same += 1
        case (Right(o), Right(n)) =>
          diff += 1
          diffFiles += f.getName
        case (Right(_), Left(err)) =>
          newFailed += 1
          crashFiles += s"${f.getName}: ${err.linesIterator.nextOption().getOrElse("")}"
        case (Left(_), _) =>
          oldFailed += 1
    }

    info(
      s"typer execution parity: total=${wvFiles
          .length} same=${same} diff=${diff} newTyperFailed=${newFailed} oldFailed=${oldFailed}"
    )
    diffFiles.result().foreach(f => debug(s"[DIFF] ${f}"))
    crashFiles.result().foreach(f => debug(s"[CRASH] ${f}"))

    // Current parity level (2026-07-02): ~84/102 measurable specs produce identical SQL (with a
    // one-spec allowance for shared-symbol-state flakiness across compiler runs in one JVM).
    // Tighten as more TypeResolver behavior is ported
    if same < 83 then
      fail(s"Typer SQL parity regressed: same=${same} (expected >= 83)")
    if newFailed > 0 then
      fail(s"Typer compilation crashes increased: newFailed=${newFailed} (expected 0)")
  }

end TyperExecutionParityCheck

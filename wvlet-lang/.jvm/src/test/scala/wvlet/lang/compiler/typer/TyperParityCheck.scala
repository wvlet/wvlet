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
package wvlet.lang.compiler.typer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.compiler.codegen.GenSQL

import java.io.File

/**
  * Parity gate for the new Typer (issue #1764): compile every spec under spec/basic with both the
  * default TypeResolver and the new Typer, and compare the generated SQL.
  *
  * The bounds below pin the current parity level. When porting more TypeResolver behavior to Typer,
  * improvements are accepted automatically; regressions fail this test. Update the bounds downward
  * as the gap burns down.
  */
class TyperParityCheck extends UniTest:

  private def generateSQL(compiler: Compiler, path: String): Either[String, String] =
    try
      val unit   = CompilationUnit.fromFile(path)
      val result = compiler.compileSingleUnit(unit)
      Right(GenSQL.generateSQL(unit)(using result.context))
    catch
      case e: Throwable =>
        Left(s"${e.getClass.getSimpleName}: ${e.getMessage}")

  test("new typer must not regress SQL parity over spec/basic") {
    val specDir = File("spec/basic")
    // ulid.wv and val.wv generate compile-time ULIDs, so their SQL can never match across runs
    val nonDeterministicSpecs = Set("ulid.wv", "val.wv")
    val wvFiles               = Option(specDir.listFiles())
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
      val oldCompiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
      val newCompiler = Compiler.withNewTyper(CompilerOptions(workEnv = WorkEnv(".")))
      val oldSQL      = generateSQL(oldCompiler, f.getPath)
      val newSQL      = generateSQL(newCompiler, f.getPath)
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
          // Specs requiring a catalog (DB connection) cannot be compiled standalone; they are
          // covered by the runner-side spec tests
          oldFailed += 1
    }

    info(
      s"typer parity: total=${wvFiles
          .length} same=${same} diff=${diff} newTyperFailed=${newFailed} needsCatalog=${oldFailed}"
    )
    diffFiles.result().foreach(f => debug(s"[DIFF] ${f}"))
    crashFiles.result().foreach(f => debug(s"[CRASH] ${f}"))

    // Current parity level (2026-07-02). Tighten as more TypeResolver behavior is ported:
    // remaining diffs are grouping-key indexes (_1) and aggregation-function inlining
    if same < 48 then
      fail(s"Typer SQL parity regressed: same=${same} (expected >= 48)")
    if newFailed > 0 then
      fail(s"Typer compilation crashes increased: newFailed=${newFailed} (expected 0)")
  }

end TyperParityCheck

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
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}

/**
  * Tests for type-mismatch diagnostics collected by the Typer
  */
class TyperDiagnosticsTest extends UniTest:

  private def compileErrors(wv: String): List[TyperError] =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(wv)
    compiler.compileSingleUnit(unit)
    unit.typerErrors

  test("report a mismatch between resolved incompatible operand types") {
    val errors = compileErrors("select 1 - 'a' as v")
    errors.exists {
      case t: TypeMismatch =>
        true
      case _ =>
        false
    } shouldBe true
  }

  test("report an incomparable comparison between resolved types") {
    val errors = compileErrors("select 1 < true as v")
    errors.exists {
      case t: TypeMismatch =>
        true
      case _ =>
        false
    } shouldBe true
  }

  test("allow string concatenation to absorb non-string operands") {
    compileErrors("select 'v=' + 42 as v") shouldBe Nil
  }

  test("not report mismatches for unresolved operands") {
    // unknown_col cannot resolve, so the arithmetic and comparison must stay silent
    compileErrors("from [[1]] as t(id) | where unknown_col + 1 > 2") shouldBe Nil
  }

  test("fail the compilation on type errors when failOnTypeErrors is set") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")).withFailOnTypeErrors(true))
    val unit     = CompilationUnit.fromWvletString("select 1 - 'a' as v")
    val e        = intercept[WvletLangException] {
      compiler.compileSingleUnit(unit)
    }
    e.statusCode shouldBe StatusCode.TYPE_ERROR
  }

end TyperDiagnosticsTest

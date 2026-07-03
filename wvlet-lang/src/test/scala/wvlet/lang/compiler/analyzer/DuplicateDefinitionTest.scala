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
package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}

/**
  * Tests for detecting top-level definitions with the same name in multiple compilation units,
  * which resolve nondeterministically through the shared global namespace (issue #93)
  */
class DuplicateDefinitionTest extends UniTest:

  test("warn once when a model name is defined in multiple files") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit1    = CompilationUnit.fromWvletString("model dup_m = { from [[1]] as t1(id) }")
    val unit2    = CompilationUnit.fromWvletString("model dup_m = { from [[2]] as t2(id) }")
    val ref      = CompilationUnit.fromWvletString("from dup_m")
    val result   = compiler.compileMultipleUnits(List(unit1, unit2), contextUnit = ref)
    val dups     = result.context.global.duplicateDefinitions
    dups.exists(_._1.name == "dup_m") shouldBe true
  }

  test("not warn for a name defined in a single file") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString("model single_m = { from [[1]] as t1(id) }")
    val ref      = CompilationUnit.fromWvletString("from single_m")
    val result   = compiler.compileMultipleUnits(List(unit), contextUnit = ref)
    result.context.global.duplicateDefinitions.exists(_._1.name == "single_m") shouldBe false
  }

end DuplicateDefinitionTest

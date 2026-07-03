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

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}

import java.io.File

/**
  * Regression test for nested model expansion in a multi-unit compilation (the REPL scenario with a
  * working folder). The referenced model's unit may not be fully typed when SQL generation runs, so
  * nested model references can appear only after the body is resolved; a missed nested reference
  * previously sent the SQL printer into unbounded recursion (StackOverflowError, flaky on CI)
  */
class MultiUnitModelExpansionTest extends UniTest:

  test("expand nested model references compiled together with their defining units") {
    val dir      = File("spec/basic/model")
    val files    = dir.listFiles().filter(_.getName.endsWith(".wv")).map(_.getPath).toList.sorted
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(dir.getPath), workEnv = WorkEnv(dir.getPath))
    )
    val units = files.map(p => CompilationUnit.fromFile(p))
    val ref   = CompilationUnit.fromWvletString("from person_filter(2)")
    val res   = compiler.compileMultipleUnits(units, contextUnit = ref)
    val sql   = GenSQL.generateSQL(ref)(using res.context)
    sql shouldContain "person.json"
    sql shouldContain "age_group"
    // The nested models must be fully expanded away
    sql shouldNotContain "person_filter"
    sql shouldNotContain "person_with_age_group"
  }

end MultiUnitModelExpansionTest

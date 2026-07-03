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
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.DataType

/**
  * Model references across compilation units must resolve to the fully typed model schema
  * regardless of the order in which the units are typed. The referencing unit is deliberately
  * listed before the defining unit, so typing it forces the lazy completion (on-demand typing) of
  * the model in the not-yet-typed defining unit.
  */
class CrossUnitModelTypingTest extends UniTest:

  test("resolve a model defined in a unit that is typed after the referencing unit") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))

    // Typed first, references base_model defined in the unit typed after it
    val referencingUnit = CompilationUnit.fromWvletString("""model wrapper_model = {
        |  from base_model
        |  where id > 0
        |}
        |""".stripMargin)
    // Typed last; the uname column type (string) is only known after typing the body
    val definingUnit = CompilationUnit.fromWvletString("""model base_model = {
        |  from [[1, 'a'], [2, 'b']] as t(id, name)
        |  select id, upper(name) as uname
        |}
        |""".stripMargin)
    val queryUnit = CompilationUnit.fromWvletString("from wrapper_model\n")

    val result = compiler.compileMultipleUnits(List(referencingUnit, definingUnit), queryUnit)

    // The wrapper model saw the resolved schema of base_model, including the computed column
    val wrapperType = result
      .context
      .findTermSymbolByName("wrapper_model")
      .map(_.dataType)
      .getOrElse(DataType.UnknownType)
    wrapperType.isResolved shouldBe true
    val unameField = wrapperType
      .asInstanceOf[wvlet.lang.model.RelationType]
      .fields
      .find(_.name.name == "uname")
    unameField.map(_.dataType) shouldBe Some(DataType.StringType)

    // The full query expands through both models into plain SQL
    val sql = GenSQL.generateSQL(queryUnit)(using result.context)
    sql shouldContain "upper"
    sql shouldNotContain "wrapper_model"
    sql shouldNotContain "base_model"
  }

end CrossUnitModelTypingTest

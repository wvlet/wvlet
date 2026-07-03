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
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType

/**
  * Type definitions are completed lazily (SymbolCompleter), so a parent type defined in another
  * compilation unit resolves once all units are labeled, regardless of unit order. The extending
  * unit is deliberately listed before the defining unit.
  */
class CrossUnitTypeDefTest extends UniTest:

  test("resolve a parent type defined in a unit labeled after the extending unit") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))

    val extendingUnit = CompilationUnit.fromWvletString("""type ext_child extends ext_parent = {
        |  child_field: string
        |}
        |""".stripMargin)
    val definingUnit = CompilationUnit.fromWvletString("""type ext_parent = {
        |  parent_code: string
        |}
        |""".stripMargin)
    val queryUnit = CompilationUnit.fromWvletString("from [[1]] as t(id) select id\n")

    val result = compiler.compileMultipleUnits(List(extendingUnit, definingUnit), queryUnit)

    val childSym = result.context.findSymbolByName(Name.typeName("ext_child"))
    childSym.isDefined shouldBe true
    childSym
      .map(_.dataType)
      .collect { case s: DataType.SchemaType =>
        s
      } match
      case Some(schema) =>
        // The parent must be the resolved schema of ext_parent from the other unit,
        // not an unknown stub
        schema.parent.map(_.isResolved) shouldBe Some(true)
        schema
          .parent
          .collect { case p: DataType.SchemaType =>
            p.typeName.name
          } shouldBe Some("ext_parent")
      case other =>
        fail(s"Expected a SchemaType for ext_child, but got: ${other}")
  }

end CrossUnitTypeDefTest

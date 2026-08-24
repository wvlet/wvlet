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
import wvlet.lang.compiler.CompileResult
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.typer.TableShapeDeclaredAsType
import wvlet.lang.compiler.typer.TyperError

/**
  * The `type` vs `table` doctrine (#1998): `table` declares stored relations; `type` remains for
  * method interfaces and aliases. A relation-shaped `type` that resolves a table reference gets a
  * deprecation warning steering to the `table` spelling, while `table` declarations and method-only
  * `type` definitions never warn
  */
class TableTypeDeprecationTest extends UniTest:

  private def compile(defs: String, query: String): (CompileResult, CompilationUnit) =
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val defsUnit  = CompilationUnit.fromWvletString(defs)
    val queryUnit = CompilationUnit.fromWvletString(query)
    val result    = compiler.compileMultipleUnits(List(defsUnit), queryUnit)
    (result, queryUnit)

  private def deprecationWarnings(unit: CompilationUnit): List[TableShapeDeclaredAsType] = unit
    .typerErrors
    .collect { case w: TableShapeDeclaredAsType =>
      w
    }

  test("warn when a bound relation-shaped type resolves a table reference") {
    val defs =
      """type orders in mydb.sales = {
        |  order_id: bigint
        |  status: string
        |}
        |""".stripMargin
    val (_, queryUnit) = compile(defs, "from sales.orders\n")
    val warnings       = deprecationWarnings(queryUnit)
    warnings.size shouldBe 1
    warnings.head.severity shouldBe TyperError.Severity.Warning
    warnings.head.message shouldContain "table orders in mydb.sales = {...}"
  }

  test("warn when an unbound relation-shaped type resolves a bare reference") {
    val defs =
      """type events = {
        |  id: bigint
        |  label: string
        |}
        |""".stripMargin
    val (_, queryUnit) = compile(defs, "from events\n")
    val warnings       = deprecationWarnings(queryUnit)
    warnings.size shouldBe 1
    warnings.head.message shouldContain "table events = {...}"
  }

  test("not warn for table declarations") {
    val defs =
      """table orders in mydb.sales = {
        |  order_id: bigint
        |  status: string
        |}
        |
        |table events = {
        |  id: bigint
        |  label: string
        |}
        |""".stripMargin
    val (_, queryUnit) = compile(defs, "from sales.orders\nfrom events\n")
    deprecationWarnings(queryUnit) shouldBe Nil
  }

  test("resolve references and writes of a bound table declaration to the same location") {
    val defs =
      """table orders in mydb.sales = {
        |  order_id: bigint
        |  status: string
        |}
        |""".stripMargin
    val (result, queryUnit) = compile(defs, "from sales.orders\n")
    val sql                 = GenSQL.generateSQL(queryUnit)(using result.context)
    sql shouldContain "mydb.sales.orders"

    val (createResult, createUnit) = compile(defs, "create table sales.orders\n")
    val createSQL                  = GenSQL.generateSQL(createUnit)(using createResult.context)
    createSQL shouldContain "create table mydb.sales.orders"
    createSQL shouldContain "order_id"
  }

end TableTypeDeprecationTest

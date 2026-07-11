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
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.plan.TypeDef

/**
  * Schema-bound table types (`type <name> in [<catalog>.]<schema> = {...}`, #1881) let queries
  * against database tables type-check without a live catalog connection, and compile into scans of
  * the bound table location
  */
class TypeTableBindingTest extends UniTest:

  private val ordersType =
    """type orders in mydb.sales = {
      |  order_id: bigint
      |  status: string
      |}
      |""".stripMargin

  private def compileQuery(typeDefs: String, query: String): (String, wvlet.lang.compiler.Context) =
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val typeUnit  = CompilationUnit.fromWvletString(typeDefs)
    val queryUnit = CompilationUnit.fromWvletString(query)
    val result    = compiler.compileMultipleUnits(List(typeUnit), queryUnit)
    val sql       = GenSQL.generateSQL(queryUnit)(using result.context)
    (sql, result.context)

  test("parse a catalog.schema binding in the type definition context") {
    val (_, context) = compileQuery(ordersType, "from sales.orders\n")
    val sym          = context.findSymbolByName(Name.typeName("orders"))
    sym.isDefined shouldBe true
    sym.get.tree match
      case t: TypeDef =>
        t.defContexts.map(_.contextType.fullName) shouldBe List("mydb.sales")
      case other =>
        fail(s"Expected a TypeDef tree, but got: ${other}")
  }

  test("resolve a schema-qualified reference through the bound type") {
    val (sql, _) = compileQuery(ordersType, "from sales.orders select order_id\n")
    sql shouldContain "mydb.sales.orders"
    sql shouldContain "order_id"
  }

  test("resolve a catalog-qualified reference through the bound type") {
    val (sql, _) = compileQuery(ordersType, "from mydb.sales.orders\n")
    sql shouldContain "mydb.sales.orders"
  }

  test("leave references to other schemas unresolved by the bound type") {
    val (sql, _) = compileQuery(ordersType, "from archive.orders\n")
    sql shouldContain "archive.orders"
    sql shouldNotContain "mydb.sales.orders"
  }

  test("skip bare references when the binding points outside the default schema") {
    val (sql, _) = compileQuery(ordersType, "from orders\n")
    sql shouldNotContain "sales"
  }

  test("resolve bare references when the binding matches the default schema") {
    val typeDef =
      """type orders in main = {
        |  order_id: bigint
        |}
        |""".stripMargin
    val (sql, _) = compileQuery(typeDef, "from orders\n")
    sql shouldContain "main.orders"
  }

  test("keep plain table types matching only bare references") {
    val typeDef =
      """type events = {
        |  event_id: bigint
        |}
        |""".stripMargin
    val (bareSql, _) = compileQuery(typeDef, "from events\n")
    bareSql shouldContain "events"

    // A qualified reference must not resolve through an unbound type by its leaf name
    val (qualifiedSql, _) = compileQuery(typeDef, "from staging.events\n")
    qualifiedSql shouldContain "staging.events"
  }

  test("keep dialect contexts as non-binding scopes") {
    val typeDef =
      """type metrics in duckdb = {
        |  value: double
        |}
        |""".stripMargin
    val (sql, _) = compileQuery(typeDef, "from metrics\n")
    sql shouldContain "metrics"
    sql shouldNotContain "duckdb.metrics"
  }

end TypeTableBindingTest

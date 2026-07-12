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
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.typer.DuplicateTypeDefinition
import wvlet.lang.compiler.typer.TyperError
import wvlet.lang.model.plan.TypeDef

/**
  * Schema-bound table types (`type <name> in <catalog>.<schema> = {...}`, #1881) let queries
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

  private def compile(typeDefs: String, query: String): CompileResult =
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val typeUnit  = CompilationUnit.fromWvletString(typeDefs)
    val queryUnit = CompilationUnit.fromWvletString(query)
    compiler.compileMultipleUnits(List(typeUnit), queryUnit)

  private def compileToSQL(typeDefs: String, query: String): String =
    val queryUnit = CompilationUnit.fromWvletString(query)
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val typeUnit  = CompilationUnit.fromWvletString(typeDefs)
    val result    = compiler.compileMultipleUnits(List(typeUnit), queryUnit)
    GenSQL.generateSQL(queryUnit)(using result.context)

  test("parse a catalog.schema binding in the type definition context") {
    val result = compile(ordersType, "from sales.orders\n")
    result.context.findSymbolByName(Name.typeName("orders")).map(_.tree) match
      case Some(t: TypeDef) =>
        t.defContexts.map(_.contextType.fullName) shouldBe List("mydb.sales")
      case other =>
        fail(s"Expected a TypeDef tree, but got: ${other}")
  }

  test("reject bindings with more than two name parts") {
    val result = compile(
      "type orders in mycat.sales.extra = {\n  order_id: bigint\n}\n",
      "from orders\n"
    )
    result.hasFailures shouldBe true
    result.failureReport.values.exists(_.getMessage.contains("<catalog>.<schema>")) shouldBe true
  }

  test("resolve a schema-qualified reference through the bound type") {
    val sql = compileToSQL(ordersType, "from sales.orders select order_id\n")
    sql shouldContain "mydb.sales.orders"
    sql shouldContain "order_id"
  }

  test("resolve a catalog-qualified reference through the bound type") {
    val sql = compileToSQL(ordersType, "from mydb.sales.orders\n")
    sql shouldContain "mydb.sales.orders"
  }

  test("match catalog and schema names case-insensitively") {
    val sql = compileToSQL(ordersType, "from MyDB.SALES.orders\n")
    sql shouldContain "mydb.sales.orders"
  }

  test("leave references to other schemas unresolved by the bound type") {
    val sql = compileToSQL(ordersType, "from archive.orders\n")
    sql shouldContain "archive.orders"
    sql shouldNotContain "mydb.sales.orders"
  }

  test("skip bare references when the binding points outside the default schema") {
    val sql = compileToSQL(ordersType, "from orders\n")
    sql shouldNotContain "sales"
  }

  test("resolve bare references when the binding matches the default catalog and schema") {
    // The compiler's default catalog/schema is memory/main when not configured
    val typeDef =
      """type orders in memory.main = {
        |  order_id: bigint
        |}
        |""".stripMargin
    val sql = compileToSQL(typeDef, "from orders\n")
    sql shouldContain "memory.main.orders"
  }

  test("keep plain table types matching only bare references") {
    val typeDef =
      """type events = {
        |  event_id: bigint
        |}
        |""".stripMargin
    val bareSql = compileToSQL(typeDef, "from events\n")
    bareSql shouldContain "events"

    // A qualified reference must not resolve through an unbound type by its leaf name
    val qualifiedSql = compileToSQL(typeDef, "from staging.events\n")
    qualifiedSql shouldContain "staging.events"
  }

  private val marketingOrdersType =
    """type orders in mydb.marketing = {
      |  order_id: bigint
      |  channel: string
      |}
      |""".stripMargin

  private def compileUnits(typeDefsList: List[String], query: String): List[CompilationUnit] =
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val typeUnits = typeDefsList.map(CompilationUnit.fromWvletString)
    val queryUnit = CompilationUnit.fromWvletString(query)
    compiler.compileMultipleUnits(typeUnits, queryUnit)
    typeUnits

  private def duplicateWarnings(units: List[CompilationUnit]): List[DuplicateTypeDefinition] = units
    .flatMap(_.typerErrors)
    .collect { case d: DuplicateTypeDefinition =>
      d
    }

  test("warn when the same type name is bound to different schemas in different files") {
    val typeUnits = compileUnits(List(ordersType, marketingOrdersType), "from sales.orders\n")
    val warnings  = duplicateWarnings(typeUnits)
    warnings.size shouldBe 1
    val w = warnings.head
    w.severity shouldBe TyperError.Severity.Warning
    w.message shouldContain "Duplicate type definition 'orders'"
    w.message shouldContain "mydb.sales"
    w.message shouldContain "mydb.marketing"
    // The warning surfaces on the unit labeled after the first definition
    duplicateWarnings(List(typeUnits(1))).size shouldBe 1
    // Name lookup uses the file-name-sorted head; in-memory units are named by
    // creation-ordered ULIDs, so the first unit wins
    w.winnerFileName shouldBe typeUnits.head.sourceFile.fileName
  }

  test("not warn when the same type name has an identical binding across files") {
    val upperCased = ordersType.replace("mydb.sales", "MYDB.SALES")
    val typeUnits  = compileUnits(List(ordersType, upperCased), "from sales.orders\n")
    duplicateWarnings(typeUnits) shouldBe Nil
  }

  test("not warn for dialect extensions or duplicated single-part contexts") {
    // A base type extended for a dialect in the same file (like stdlib's `type string` +
    // `type string in duckdb`) is intentional and never a table-binding conflict
    val dialectExtension =
      """type mystr = {
        |  v: string
        |}
        |type mystr in duckdb = {
        |  v: string
        |}
        |""".stripMargin
    duplicateWarnings(compileUnits(List(dialectExtension), "from [[1]] as t(id)\n")) shouldBe Nil

    // Single-part contexts are dialect scopes, not table bindings, in separate files too
    val duckdbMetrics  = "type metrics in duckdb = {\n  value: double\n}\n"
    val trinoMetrics   = "type metrics in trino = {\n  value: double\n}\n"
    val singlePartOnly = compileUnits(List(duckdbMetrics, trinoMetrics), "from [[1]] as t(id)\n")
    duplicateWarnings(singlePartOnly) shouldBe Nil
  }

  test("warn when a schema-bound type shares its name with an unbound type") {
    val unbound   = "type orders = {\n  order_id: bigint\n}\n"
    val typeUnits = compileUnits(List(unbound, ordersType), "from sales.orders\n")
    val warnings  = duplicateWarnings(typeUnits)
    warnings.size shouldBe 1
    warnings.head.message shouldContain "no table binding"
    warnings.head.message shouldContain "mydb.sales"
  }

  test("warn when one file binds the same type name to different schemas") {
    val sameFile  = ordersType + marketingOrdersType
    val typeUnits = compileUnits(List(sameFile), "from sales.orders\n")
    val warnings  = duplicateWarnings(typeUnits)
    warnings.size shouldBe 1
    warnings.head.message shouldContain "mydb.sales"
    warnings.head.message shouldContain "mydb.marketing"
  }

  test("not fail strict compilation on duplicate type definition warnings") {
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv(".")).withFailOnTypeErrors(true))
    val typeUnits = List(ordersType, marketingOrdersType).map(CompilationUnit.fromWvletString)
    val queryUnit = CompilationUnit.fromWvletString("from sales.orders\n")
    // Duplicate-definition diagnostics are warnings and must not fail even in strict mode
    val result = compiler.compileMultipleUnits(typeUnits, queryUnit)
    result.hasFailures shouldBe false
    duplicateWarnings(typeUnits).size shouldBe 1
  }

  test("keep single-part contexts as non-binding dialect scopes") {
    // Dialect contexts are an open set (e.g. `type td_trino extends trino` in the stdlib),
    // so any single-part context keeps the previous unbound-type behavior
    val typeDef =
      """type metrics in duckdb = {
        |  value: double
        |}
        |type points in custom_dialect = {
        |  x: double
        |}
        |""".stripMargin
    val sql = compileToSQL(typeDef, "from metrics\n")
    sql shouldContain "metrics"
    sql shouldNotContain "duckdb.metrics"

    val customSql = compileToSQL(typeDef, "from points\n")
    customSql shouldContain "points"
    customSql shouldNotContain "custom_dialect"
  }

end TypeTableBindingTest

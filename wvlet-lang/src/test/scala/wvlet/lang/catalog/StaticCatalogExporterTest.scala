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
package wvlet.lang.catalog

import wvlet.uni.test.UniTest
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Catalog.TableColumn
import wvlet.lang.catalog.Catalog.TableDef
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.DataType

class StaticCatalogExporterTest extends UniTest:

  private def tableDef(schema: String, name: String, columns: (String, String)*): TableDef =
    TableDef(
      TableName(Some("mydb"), Some(schema), name),
      columns.map((n, t) => TableColumn(n, DataType.parse(t))).toList
    )

  private val orders = tableDef(
    "sales",
    "orders",
    "order_id" -> "bigint",
    "status"   -> "string",
    "price"    -> "decimal(10,2)"
  )

  private val customers = tableDef("sales", "customers", "customer_id" -> "bigint")

  test("generate schema-bound type definitions in deterministic order") {
    // Tables are sorted by name regardless of the input order
    val source = StaticCatalogExporter.generateSchemaSource(
      "mydb",
      "sales",
      List(orders, customers)
    )
    source shouldContain "type customers in mydb.sales = {"
    source shouldContain "type orders in mydb.sales = {"
    source shouldContain "  order_id: long"
    source shouldContain "  price: decimal[10,2]"
    source shouldContain StaticCatalogExporter.generatedFileHeader
    source.indexOf("type customers") < source.indexOf("type orders") shouldBe true

    val regenerated = StaticCatalogExporter.generateSchemaSource(
      "mydb",
      "sales",
      List(customers, orders)
    )
    regenerated shouldBe source
  }

  test("backquote names that are not plain identifiers") {
    val weird  = tableDef("sales", "weird-table", "select" -> "string", "1col" -> "int")
    val source = StaticCatalogExporter.generateSchemaSource("mydb", "sales", List(weird))
    source shouldContain "type `weird-table` in mydb.sales = {"
    source shouldContain "`select`: string"
    source shouldContain "`1col`: int"
  }

  test("backquote columns named with non-reserved keywords") {
    // The type-body field grammar accepts only plain identifiers, so a bare keyword column
    // like `count` would make the generated file unparseable
    val stats  = tableDef("sales", "stats", "count" -> "long", "end" -> "string")
    val source = StaticCatalogExporter.generateSchemaSource("mydb", "sales", List(stats))
    source shouldContain "`count`: long"
    source shouldContain "`end`: string"

    val compiler  = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    val typeUnit  = CompilationUnit.fromWvletString(source)
    val queryUnit = CompilationUnit.fromWvletString("from sales.stats\n")
    val result    = compiler.compileMultipleUnits(List(typeUnit), queryUnit)
    result.hasFailures shouldBe false
  }

  test("skip tables with backquotes or control characters in names") {
    val bad    = tableDef("sales", "bad", "a`b" -> "int")
    val source = StaticCatalogExporter.generateSchemaSource("mydb", "sales", List(bad, orders))
    source shouldNotContain "a`b"
    source shouldContain "type orders"
  }

  test("skip tables without columns") {
    val empty  = TableDef(TableName(Some("mydb"), Some("sales"), "empty_table"), Nil)
    val source = StaticCatalogExporter.generateSchemaSource("mydb", "sales", List(empty, orders))
    source shouldNotContain "empty_table"
    source shouldContain "type orders"
  }

  test("generated source compiles and resolves qualified references offline") {
    val source    = StaticCatalogExporter.generateSchemaSource("mydb", "sales", List(orders))
    val compiler  = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    val typeUnit  = CompilationUnit.fromWvletString(source)
    val queryUnit = CompilationUnit.fromWvletString("from sales.orders select order_id, price\n")
    val result    = compiler.compileMultipleUnits(List(typeUnit), queryUnit)
    result.hasFailures shouldBe false
    val sql = GenSQL.generateSQL(queryUnit)(using result.context)
    sql shouldContain "mydb.sales.orders"
    sql shouldContain "order_id"
  }

  private def fn(name: String, ret: String, args: String*): SQLFunction = SQLFunction(
    name,
    SQLFunction.FunctionType.SCALAR,
    args.map(DataType.parse).toSeq,
    DataType.parse(ret)
  )

  test("generate engine function definitions in deterministic order") {
    val functions = List(
      fn("regexp_extract", "string", "string", "string"),
      fn("date_trunc", "timestamp", "string", "timestamp"),
      fn("pi", "double")
    )
    val source = StaticCatalogExporter.generateFunctionsSource("duckdb", functions)
    source shouldContain StaticCatalogExporter.generatedFileHeader
    source shouldContain "def date_trunc(a1: string, a2: timestamp) in duckdb: timestamp = native"
    source shouldContain "def regexp_extract(a1: string, a2: string) in duckdb: string = native"
    source shouldContain "def pi in duckdb: double = native"
    source.indexOf("def date_trunc") < source.indexOf("def pi") shouldBe true
    source.indexOf("def pi") < source.indexOf("def regexp_extract") shouldBe true

    // Regenerating from a different input order produces identical output
    StaticCatalogExporter.generateFunctionsSource("duckdb", functions.reverse) shouldBe source
  }

  test("skip function names that could collide with the Wvlet syntax or builtin typing") {
    val functions = List(
      // Keywords cannot be backquoted usefully as call syntax
      fn("end", "string", "string"),
      // Builtin functions already have typing rules
      fn("count", "long"),
      fn("upper", "string", "string"),
      // Standard library defines this function
      fn("ulid_string", "string"),
      // Operators are not plain identifiers
      fn("+", "long", "long", "long"),
      fn("regexp_extract", "string", "string", "string")
    )
    val source = StaticCatalogExporter.generateFunctionsSource("duckdb", functions)
    source shouldContain "def regexp_extract"
    source shouldNotContain "def end"
    source shouldNotContain "def count"
    source shouldNotContain "def upper"
    source shouldNotContain "def ulid_string"
    source shouldNotContain "def +"
  }

  test("skip functions with a non-plain-call syntax") {
    val source = StaticCatalogExporter.generateFunctionsSource(
      "duckdb",
      List(
        SQLFunction(
          "read_csv",
          SQLFunction.FunctionType.TABLE,
          Seq(DataType.StringType),
          DataType.AnyType
        ),
        SQLFunction("database_list", SQLFunction.FunctionType.PRAGMA, Nil, DataType.AnyType),
        SQLFunction("add_macro", SQLFunction.FunctionType.MACRO, Nil, DataType.AnyType)
      )
    )
    source shouldBe ""
  }

  test("collapse overloads to a single def with any arguments") {
    val source = StaticCatalogExporter.generateFunctionsSource(
      "duckdb",
      List(
        fn("date_trunc", "timestamp", "string", "timestamp"),
        fn("date_trunc", "timestamp", "string", "date")
      )
    )
    source shouldContain "def date_trunc(a1: any, a2: any) in duckdb: timestamp = native"
  }

  test("keep the return type of an overloaded function only when all overloads agree") {
    val source = StaticCatalogExporter.generateFunctionsSource(
      "duckdb",
      List(fn("array_slice", "string", "string", "int"), fn("array_slice", "int", "int", "int"))
    )
    source shouldContain "def array_slice(a1: any, a2: any) in duckdb: any = native"
  }

  test("fall back to any for types that do not fit the def grammar") {
    val source = StaticCatalogExporter.generateFunctionsSource(
      "duckdb",
      List(
        SQLFunction(
          "weird_fn",
          SQLFunction.FunctionType.SCALAR,
          Seq(DataType.UnknownType),
          DataType.StringType
        )
      )
    )
    source shouldContain "def weird_fn(a1: any) in duckdb: string = native"
  }

  test("generated function definitions compile and pass calls through to SQL") {
    val source = StaticCatalogExporter.generateFunctionsSource(
      "duckdb",
      List(fn("regexp_extract", "string", "string", "string"))
    )
    val compiler  = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    val fnUnit    = CompilationUnit.fromWvletString(source)
    val queryUnit = CompilationUnit.fromWvletString(
      "from [['a1']] as t(x)\nselect regexp_extract(x, '[0-9]+') as digits\n"
    )
    val result = compiler.compileMultipleUnits(List(fnUnit), queryUnit)
    result.hasFailures shouldBe false
    val sql = GenSQL.generateSQL(queryUnit)(using result.context)
    sql shouldContain "regexp_extract(x, '[0-9]+')"
  }

  test("reject catalog or schema names that escape the target folder") {
    intercept[WvletLangException] {
      StaticCatalogExporter.exportSchemas(
        "../evil",
        List("sales"),
        _ => List(orders),
        "target/static-catalog-test"
      )
    }
  }

  test("reject catalog names that the source scan would skip") {
    // catalog/target would silently never load back: the folder scan ignores target/
    intercept[WvletLangException] {
      StaticCatalogExporter.exportSchemas(
        "target",
        List("sales"),
        _ => List(orders),
        "target/static-catalog-test"
      )
    }
  }

end StaticCatalogExporterTest

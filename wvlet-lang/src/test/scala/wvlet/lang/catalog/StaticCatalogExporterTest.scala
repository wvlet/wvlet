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

  test("reject catalog or schema names that escape the target folder") {
    intercept[IllegalArgumentException] {
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
    intercept[IllegalArgumentException] {
      StaticCatalogExporter.exportSchemas(
        "target",
        List("sales"),
        _ => List(orders),
        "target/static-catalog-test"
      )
    }
  }

end StaticCatalogExporterTest

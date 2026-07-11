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
package wvlet.lang.runner

import wvlet.uni.test.UniTest
import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.connector.duckdb.DuckDBConnector

import java.nio.file.Files
import java.nio.file.Path

/**
  * End-to-end check of `wv catalog import`: export table schemas of a live DuckDB database as Wvlet
  * type definitions, then compile a query offline against the generated files
  */
class StaticCatalogExportTest extends UniTest:

  test("export DuckDB table schemas as Wvlet types and compile a query offline") {
    val workEnv = WorkEnv()
    val duckdb  = DuckDBConnector(workEnv)
    Files.createDirectories(Path.of("target"))
    // Simulate a project folder holding queries and the generated catalog/ folder
    val projectDir = Files.createTempDirectory(Path.of("target"), "static-catalog").toString
    val targetDir  = s"${projectDir}/catalog"
    try
      duckdb.executeUpdate("create schema if not exists sales")
      duckdb.executeUpdate("""create table sales.orders (
          |  order_id bigint,
          |  status varchar,
          |  price decimal(10,2),
          |  created_at timestamp,
          |  tags varchar[]
          |)""".stripMargin)
      duckdb.executeUpdate("create table sales.customers (customer_id bigint, name varchar)")

      // Scan the database directly, as `wv catalog import` does
      def exportSales(): List[String] = StaticCatalogExporter.exportSchemas(
        "memory",
        List("sales"),
        schema => duckdb.listTableDefs("memory", schema),
        targetDir
      )
      val written = exportSales()
      written shouldBe List(s"${targetDir}/memory/sales.wv")

      val source = SourceIO.readAsString(written.head)
      source shouldContain "type customers in memory.sales = {"
      source shouldContain "type orders in memory.sales = {"
      source shouldContain "order_id: long"
      source shouldContain "price: decimal[10,2]"
      source shouldContain "tags: array[string]"

      // Re-running the export produces identical output (deterministic sync)
      exportSales()
      SourceIO.readAsString(written.head) shouldBe source

      // A query in the project folder compiles offline, without any catalog connection:
      // the compiler discovers catalog/<name>/<schema>.wv through the well-known folder scan
      val compiler = Compiler(
        CompilerOptions(sourceFolders = List(projectDir), workEnv = WorkEnv(projectDir))
      )
      val queryUnit = CompilationUnit.fromWvletString(
        "from sales.orders where status = 'done' select order_id, price\n"
      )
      val result = compiler.compileSingleUnit(queryUnit)
      result.hasFailures shouldBe false
      val sql = GenSQL.generateSQL(queryUnit)(using result.context)
      sql shouldContain "memory.sales.orders"
    finally
      duckdb.close()
    end try
  }

end StaticCatalogExportTest

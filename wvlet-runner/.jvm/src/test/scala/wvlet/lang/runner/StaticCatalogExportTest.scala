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

      // Scan the database directly, as `wvlet catalog import` does
      def exportSales(): List[String] = StaticCatalogExporter.exportSchemas(
        "memory",
        List("sales"),
        schema => duckdb.listTableDefs("memory", schema),
        targetDir,
        pruneStale = true
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

      // A re-export prunes generated files of schemas dropped from the database, but
      // keeps hand-written files
      val staleFile = s"${targetDir}/memory/dropped_schema.wv"
      SourceIO.writeString(
        staleFile,
        s"${StaticCatalogExporter
            .generatedFileHeader}\ntype gone in memory.dropped_schema = {\n  id: int\n}\n"
      )
      val handWrittenFile = s"${targetDir}/memory/my_types.wv"
      SourceIO.writeString(handWrittenFile, "type my_type = {\n  id: int\n}\n")
      exportSales()
      SourceIO.existsFile(staleFile) shouldBe false
      SourceIO.existsFile(handWrittenFile) shouldBe true

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

  test("export DuckDB functions as Wvlet defs and compile a call offline") {
    val workEnv = WorkEnv()
    val duckdb  = DuckDBConnector(workEnv)
    Files.createDirectories(Path.of("target"))
    val projectDir = Files.createTempDirectory(Path.of("target"), "static-functions").toString
    val targetDir  = s"${projectDir}/catalog"
    try
      def exportFunctions(): Option[String] = StaticCatalogExporter.exportFunctions(
        "memory",
        "duckdb",
        duckdb.listFunctions("memory"),
        targetDir
      )
      val written = exportFunctions()
      written shouldBe Some(s"${targetDir}/memory/functions.wv")

      val source = SourceIO.readAsString(written.get)
      // Engine functions without builtin typing rules are exported with the dialect tag
      source shouldContain "def regexp_extract("
      source shouldContain " in duckdb"
      // Builtins, keywords, and table functions are skipped (count_if etc. still export)
      source shouldNotContain "def count("
      source shouldNotContain "def upper("
      source shouldNotContain "def read_csv("

      // Re-running the export produces identical output (deterministic sync)
      exportFunctions()
      SourceIO.readAsString(written.get) shouldBe source

      // A call to an imported function compiles offline into a plain SQL call, typed with
      // the declared return type
      val compiler = Compiler(
        CompilerOptions(sourceFolders = List(projectDir), workEnv = WorkEnv(projectDir))
      )
      val queryUnit = CompilationUnit.fromWvletString(
        "from [['abc123']] as t(x)\nselect regexp_extract(x, '[0-9]+') as digits\n"
      )
      val result = compiler.compileSingleUnit(queryUnit)
      result.hasFailures shouldBe false
      val sql = GenSQL.generateSQL(queryUnit)(using result.context)
      sql shouldContain "regexp_extract(x, '[0-9]+')"

      // A full-catalog re-import keeps functions.wv only when it was re-generated: schema
      // pruning removes it when the import ran with --no-functions
      StaticCatalogExporter.exportSchemas(
        "memory",
        Nil,
        _ => Nil,
        targetDir,
        pruneStale = true,
        keepPaths = written.toList
      )
      SourceIO.existsFile(written.get) shouldBe true
      StaticCatalogExporter.exportSchemas("memory", Nil, _ => Nil, targetDir, pruneStale = true)
      SourceIO.existsFile(written.get) shouldBe false
    finally
      duckdb.close()
    end try
  }

end StaticCatalogExportTest

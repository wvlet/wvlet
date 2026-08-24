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
import wvlet.lang.catalog.SchemaDriftDetector
import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.duckdb.DuckDBConnector

import java.nio.file.Files
import java.nio.file.Path

/**
  * End-to-end check of `wvlet catalog diff` (#1994): diff table declarations of a project against a
  * live DuckDB catalog and generate reshape migration blocks
  */
class SchemaDriftCheckerTest extends UniTest:

  private def withDuckDB(testBody: (DuckDBConnector, String) => Unit): Unit =
    val duckdb = DuckDBConnector(WorkEnv())
    Files.createDirectories(Path.of("target"))
    val projectDir = Files.createTempDirectory(Path.of("target"), "schema-drift").toString
    try testBody(duckdb, projectDir)
    finally duckdb.close()

  private def check(duckdb: DuckDBConnector, projectDir: String): SchemaDriftReport =
    SchemaDriftChecker.check(
      sourceFolders = List(projectDir),
      workEnv = WorkEnv(projectDir),
      connector = duckdb,
      defaultCatalog = "memory",
      defaultSchema = "main",
      dbType = DBType.DuckDB
    )

  test("detect drift and generate a reshape migration block") {
    withDuckDB { (duckdb, projectDir) =>
      duckdb.executeUpdate("create schema if not exists sales")
      duckdb.executeUpdate(
        "create table sales.users (user_id bigint, name varchar, status bigint, legacy_flag boolean)"
      )
      SourceIO.writeString(
        s"${projectDir}/tables.wv",
        """table users in memory.sales = {
          |  user_id: int
          |  name: string
          |  status: string
          |  created_at: timestamp
          |}
          |""".stripMargin
      )
      val report = check(duckdb, projectDir)
      report.checkedTables shouldBe 1
      report.hasDrift shouldBe true
      report.drifted.size shouldBe 1

      val drift = report.drifted.head
      drift.addColumns.map(_._1) shouldBe List("created_at")
      drift.excludeColumns shouldBe List("legacy_flag")
      drift.typeChanges.map(_.column) shouldBe List("status")

      val rendered = SchemaDriftDetector.render(drift)
      rendered shouldContain "table memory.sales.users has drifted from its declaration"
      rendered shouldContain "reshape memory.sales.users {"
      rendered shouldContain "add created_at: timestamp"
      rendered shouldContain "exclude legacy_flag"
      rendered shouldContain "cast status as string -- the catalog has long"
      rendered shouldContain "rename <old> as <new>"

      // The generated block is ready to paste: the reshape statement (including the comment
      // lines) must parse as-is
      val migration = rendered
        .linesIterator
        .toList
        .dropWhile(line => !line.trim.startsWith("reshape"))
        .mkString("", "\n", "\n")
      ParserPhase.parseOnly(CompilationUnit.fromWvletString(migration))
    }
  }

  test("declared tables absent from the catalog are missing, not drifted") {
    withDuckDB { (duckdb, projectDir) =>
      SourceIO.writeString(
        s"${projectDir}/tables.wv",
        """table not_created_yet = {
          |  id: int
          |}
          |""".stripMargin
      )
      val report = check(duckdb, projectDir)
      report.hasDrift shouldBe false
      report.checkedTables shouldBe 0
      report.missingTables shouldBe List("memory.main.not_created_yet")
    }
  }

  test("check unbound declarations against the default catalog and schema") {
    withDuckDB { (duckdb, projectDir) =>
      duckdb.executeUpdate("create table events (id bigint, extra varchar)")
      SourceIO.writeString(
        s"${projectDir}/tables.wv",
        """table events = {
          |  id: int
          |}
          |""".stripMargin
      )
      val report = check(duckdb, projectDir)
      report.drifted.size shouldBe 1
      val rendered = SchemaDriftDetector.render(report.drifted.head)
      // Bound to the context catalog/schema: the bare name is enough
      rendered shouldContain "reshape events {"
      rendered shouldContain "exclude extra"
      rendered shouldNotContain "add"
    }
  }

  test("follow table like chains when diffing") {
    withDuckDB { (duckdb, projectDir) =>
      duckdb.executeUpdate("create table users_archive (user_id bigint)")
      SourceIO.writeString(
        s"${projectDir}/tables.wv",
        """table users = {
          |  user_id: int
          |  name: string
          |}
          |
          |table users_archive like users
          |""".stripMargin
      )
      val report = check(duckdb, projectDir)
      // users is missing; users_archive inherits the declared columns of users and drifts
      report.missingTables shouldBe List("memory.main.users")
      report.drifted.map(_.tableName) shouldBe List("users_archive")
      report.drifted.head.addColumns.map(_._1) shouldBe List("name")
    }
  }

  test("a freshly imported catalog diffs clean") {
    withDuckDB { (duckdb, projectDir) =>
      duckdb.executeUpdate("create schema if not exists sales")
      duckdb.executeUpdate("""create table sales.orders (
          |  order_id bigint,
          |  status varchar,
          |  price decimal(10,2),
          |  created_at timestamp,
          |  tags varchar[]
          |)""".stripMargin)
      duckdb.executeUpdate("create table sales.customers (customer_id bigint, name varchar)")

      // Import the catalog as `wvlet catalog import` does, then diff: the round-trip must be clean
      StaticCatalogExporter.exportSchemas(
        "memory",
        List("sales"),
        schema => duckdb.listTableDefs("memory", schema),
        s"${projectDir}/catalog"
      )
      val report = check(duckdb, projectDir)
      report.checkedTables shouldBe 2
      report.hasDrift shouldBe false

      // The check reads the catalog fresh: schema changes after the import are drift
      duckdb.executeUpdate("alter table sales.customers add column vip boolean")
      val drifted = check(duckdb, projectDir)
      drifted.hasDrift shouldBe true
      drifted.drifted.map(_.tableName) shouldBe List("memory.sales.customers")
      drifted.drifted.head.excludeColumns shouldBe List("vip")
    }
  }

end SchemaDriftCheckerTest

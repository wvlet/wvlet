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
package wvlet.lang.runner.engine

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import scala.util.Using
import scala.util.control.NonFatal

/**
  * Warehouse tables the engine writes are plain Iceberg tables: DuckDB's iceberg extension reads
  * every write back (overwrite, append, copy-on-write delete and keyed append, schema changes)
  */
class WvletEngineWarehouseDuckDBTest extends UniTest:
  private val warehouse =
    Files
      .createTempDirectory(Files.createDirectories(Path.of("target")), "engine-warehouse-duckdb")
      .toAbsolutePath
      .toString

  private def run(query: String): Unit =
    val profile = Profile(
      "test",
      List(
        ConnectorConfig(
          name = "wvlet",
          `type` = "wvlet",
          default = true,
          properties = Map("warehouse" -> warehouse)
        )
      )
    )
    val workEnv  = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("."), workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit   = CompilationUnit.fromWvletString(query)
    val result = compiler.compileSingleUnit(unit)
    result.reportAllErrors
    val ctx = result
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try PlanExecutor(provider, profile, workEnv)
        .execute(ExecutionPlanner.plan(unit, ctx), ctx)
        .getError
        .foreach(e => throw e)
    finally provider.close()

  end run

  // The rows DuckDB reads from a warehouse table, or None when the extension can't be loaded
  // (no network to download it)
  private def duckdb(table: String, orderBy: String = "1"): Option[Seq[Seq[Any]]] =
    Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        val loaded =
          try
            stmt.execute("install iceberg")
            stmt.execute("load iceberg")
            true
          catch
            case NonFatal(e) =>
              warn(s"DuckDB's iceberg extension is not available: ${e.getMessage}")
              false
        Option.when(loaded) {
          val rs = stmt.executeQuery(
            s"select * from iceberg_scan('${warehouse}/${table}') order by ${orderBy}"
          )
          val n    = rs.getMetaData.getColumnCount
          val rows = Seq.newBuilder[Seq[Any]]
          while rs.next() do
            rows += (1 to n).map(i => rs.getObject(i))
          rows.result()
        }
      }
    }

  private def check(table: String, expected: Seq[Seq[Any]]): Unit =
    duckdb(table) match
      case Some(rows) =>
        rows.map(_.map(normalize)) shouldBe expected
      case None =>
        ignore("DuckDB's iceberg extension is not available")

  private def normalize(v: Any): Any =
    v match
      case i: java.lang.Integer =>
        i.longValue
      case other =>
        other

  test("DuckDB reads saved and appended tables") {
    run("""from [[1, 'a'], [2, 'b']] as t(id, name)
          |save to items
          |
          |from [[3, 'c']] as t(id, name)
          |append to items""".stripMargin)
    check("main/items", Seq(Seq(1L, "a"), Seq(2L, "b"), Seq(3L, "c")))
    run("""from [[9, 'z']] as t(id, name)
          |save to items""".stripMargin)
    check("main/items", Seq(Seq(9L, "z")))
  }

  test("DuckDB reads copy-on-write deletes and keyed appends") {
    run("""from [[1, 'a'], [2, 'b']] as t(id, name)
          |save to cow
          |
          |from [[3, 'c']] as t(id, name)
          |append to cow
          |
          |from cow
          |where id = 2
          |delete
          |
          |from [[3, 'C'], [4, 'd']] as t(id, name)
          |append to cow on id""".stripMargin)
    check("main/cow", Seq(Seq(1L, "a"), Seq(3L, "C"), Seq(4L, "d")))
  }

  test("DuckDB reads altered tables") {
    run("""create schema shop
          |from [[1, 'pen'], [2, 'book']] as t(id, name)
          |save to shop.goods
          |
          |reshape shop.goods {
          |  add price: double
          |  rename name as item
          |}
          |
          |from [[3, 'cup', 1.5]] as t(id, item, price)
          |append to shop.goods""".stripMargin)
    check("shop/goods", Seq(Seq(1L, "pen", null), Seq(2L, "book", null), Seq(3L, "cup", 1.5)))
  }

  test("DuckDB reads renamed tables") {
    run("""from [[1, 'a']] as t(id, name)
          |save to before_rename
          |
          |from [[2, 'b']] as t(id, name)
          |append to before_rename
          |
          |rename table before_rename to after_rename""".stripMargin)
    check("main/after_rename", Seq(Seq(1L, "a"), Seq(2L, "b")))
  }

end WvletEngineWarehouseDuckDBTest

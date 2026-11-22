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

import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

/**
  * Tables of a `wvlet` connector with a `warehouse` folder persist as Iceberg tables. Each `run`
  * opens a new session, so a test reads back what an earlier session wrote. Runs on JVM, Node.js,
  * and Native.
  */
class PlanExecutorWvletWarehouseTest extends UniTest:
  private def newWarehouse(): String =
    s"target/engine-warehouse/${System.nanoTime()}-${scala.util.Random.nextInt(1000000)}"

  private def profileOf(warehouse: String, props: Map[String, Any] = Map.empty): Profile = Profile(
    "test",
    List(
      ConnectorConfig(
        name = "wvlet",
        `type` = "wvlet",
        default = true,
        properties = props + ("warehouse" -> warehouse)
      )
    )
  )

  private def run(warehouse: String, query: String, props: Map[String, Any] = Map.empty): Unit =
    val profile  = profileOf(warehouse, props)
    val workEnv  = WorkEnv(".", logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("."), workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit          = CompilationUnit.fromWvletString(query)
    val compileResult = compiler.compileSingleUnit(unit)
    compileResult.reportAllErrors
    val ctx = compileResult
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try
      val executor = PlanExecutor(provider, profile, workEnv)
      val result   = executor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
      result.getError.foreach(e => throw e)
    finally
      provider.close()

  end run

  test("keep saved tables across sessions") {
    val w = newWarehouse()
    run(
      w,
      """from [[1, 'a'], [2, 'b']] as t(id, name)
        |save to items""".stripMargin
    )
    SourceIO.existsFile(s"${w}/main/items/metadata/version-hint.text") shouldBe true
    run(
      w,
      """from items
        |order by id
        |test _.rows should be [[1, 'a'], [2, 'b']]
        |
        |from [[3, 'c']] as t(id, name)
        |append to items""".stripMargin
    )
    run(
      w,
      """from items
        |order by id
        |test _.rows should be [[1, 'a'], [2, 'b'], [3, 'c']]
        |
        |from [[9, 'z']] as t(id, name)
        |save to items""".stripMargin
    )
    run(w, "from items\ntest _.rows should be [[9, 'z']]")
  }

  test("delete and update rows with copy-on-write") {
    val w = newWarehouse()
    run(
      w,
      """from [[1, 'a'], [2, 'b']] as t(id, name)
        |save to cow
        |
        |from [[3, 'c']] as t(id, name)
        |append to cow
        |
        |from cow
        |where id = 1
        |delete
        |
        |from [[3, 'C'], [4, 'd']] as t(id, name)
        |append to cow on id""".stripMargin
    )
    run(
      w,
      """from cow
        |order by id
        |test _.rows should be [[2, 'b'], [3, 'C'], [4, 'd']]
        |
        |from cow
        |where id > 100
        |delete
        |
        |truncate cow
        |from cow
        |test _.size should be 0""".stripMargin
    )
  }

  test("keep declared defaults and constraints across sessions") {
    val w = newWarehouse()
    run(
      w,
      """table decl = {
        |  id: int primary key
        |  name: string not null
        |  active: boolean = true
        |}
        |create table decl
        |append to decl (id, name) {
        |  from [[1, 'a']] as t(id, name)
        |}""".stripMargin
    )
    run(
      w,
      """append to decl (id, name) {
        |  from [[2, 'b']] as t(id, name)
        |}
        |from decl
        |order by id
        |test _.rows should be [[1, 'a', true], [2, 'b', true]]""".stripMargin
    )
    val dup = intercept[WvletLangException] {
      run(w, "append to decl (id, name) {\n  from [[1, 'x']] as t(id, name)\n}")
    }
    dup.getMessage shouldContain "unique"
    val nn = intercept[WvletLangException] {
      run(w, "append to decl (id, name) {\n  from [[5, null]] as t(id, name)\n}")
    }
    nn.getMessage shouldContain "NOT NULL"
    run(w, "drop table decl")
    SourceIO.existsFile(s"${w}/main/decl") shouldBe false
  }

  test("drop declared constraints when saving over a warehouse table by path") {
    val w = newWarehouse()
    run(
      w,
      """table decl = {
        |  id: int primary key
        |  name: string not null
        |}
        |create table decl""".stripMargin
    )
    run(
      w,
      s"""from [[1, 'a']] as t(id, name)
         |save to '${w}/main/decl' with format: 'iceberg'""".stripMargin
    )
    // The replaced table has no NOT NULL or primary key constraint left
    run(
      w,
      """append to decl (id, name) {
        |  from [[1, null]] as t(id, name)
        |}
        |from decl
        |test _.size should be 2""".stripMargin
    )
  }

  test("alter warehouse tables") {
    val w = newWarehouse()
    run(
      w,
      """from [[1, 'pen'], [2, 'book']] as t(id, name)
        |save to goods
        |
        |reshape goods {
        |  add price: double
        |  rename name as item
        |}""".stripMargin
    )
    run(
      w,
      """from goods
        |order by id
        |test _.columns should be ['id', 'item', 'price']
        |test _.rows should be [[1, 'pen', null], [2, 'book', null]]
        |
        |from [[3, 'cup', 1.5]] as t(id, item, price)
        |append to goods
        |
        |reshape goods {
        |  exclude item
        |}""".stripMargin
    )
    run(
      w,
      """from goods
        |order by id
        |test _.rows should be [[1, null], [2, null], [3, 1.5]]""".stripMargin
    )
  }

  test("find reshaped columns ignoring case") {
    val w = newWarehouse()
    run(
      w,
      """from [[1, 2]] as t(a, b)
        |save to cases
        |reshape cases {
        |  exclude A
        |  cast B as long
        |}""".stripMargin
    )
    run(w, "from cases\ntest _.columns should be ['b']\ntest _.rows should be [[2]]")
  }

  test("keep schemas as folders") {
    val w = newWarehouse()
    run(
      w,
      """create schema sales
        |from [[1]] as t(id)
        |save to sales.orders""".stripMargin
    )
    SourceIO.isDirectory(s"${w}/sales/orders") shouldBe true
    run(w, "from sales.orders\ntest _.rows should be [[1]]")
    val e = intercept[WvletLangException] {
      run(w, "drop schema sales")
    }
    e.getMessage shouldContain "cascade"
    run(w, "drop schema sales with cascade: true")
    SourceIO.existsFile(s"${w}/sales") shouldBe false
    val missing = intercept[WvletLangException] {
      run(w, "from [[1]] as t(id)\nsave to nowhere.t")
    }
    missing.getMessage shouldContain "Schema not found"
  }

  test("refuse a warehouse table that a tables entry also names") {
    val w = newWarehouse()
    run(w, "from [[1]] as t(id)\nsave to dup")
    val e = intercept[WvletLangException] {
      run(w, "from dup", props = Map("tables" -> Map("dup" -> s"${w}/main/dup")))
    }
    e.getMessage shouldContain "both in the warehouse"
  }

end PlanExecutorWvletWarehouseTest

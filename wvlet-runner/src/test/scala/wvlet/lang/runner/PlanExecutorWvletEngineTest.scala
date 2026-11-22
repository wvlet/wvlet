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
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

/**
  * [[PlanExecutor]] on the in-process wvlet engine (connector type `wvlet`): runs without DuckDB on
  * JVM, Node.js, and Native alike
  */
class PlanExecutorWvletEngineTest extends UniTest:
  private val engineConfig = ConnectorConfig(name = "wvlet", `type` = "wvlet", default = true)
  private val profile      = Profile("test", List(engineConfig))

  private def run(query: String, profile: Profile = profile): QueryResult =
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
      executor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
    finally
      provider.close()

  private def tables(r: QueryResult): List[TableRows] =
    r match
      case l: QueryResultList =>
        l.list.toList.flatMap(tables)
      case t: TableRows =>
        List(t)
      case _ =>
        Nil

  test("tell the in-process engine from a remote wvlet server") {
    WvletEngineSession.isInProcess(engineConfig) shouldBe true
    WvletEngineSession.isInProcess(engineConfig.withHost("localhost")) shouldBe false
    WvletEngineSession.isInProcess(engineConfig.withType("duckdb")) shouldBe false
  }

  test("execute a query on the engine") {
    val result = run("""from [[1, 'a'], [2, 'b'], [3, 'c']] as t(id, name)
                       |where id >= 2
                       |select id, name""".stripMargin)
    result.getError shouldBe None
    val t = tables(result).last
    t.totalRows shouldBe 2
    t.schema.fields.map(_.name.name) shouldBe List("id", "name")
    t.rows.map(_("name")) shouldBe List("b", "c")
  }

  test("evaluate test statements on engine results") {
    val result = run("""from [[1], [2], [3]] as t(a)
                       |agg _.count as n
                       |test _.rows should be [[3]]""".stripMargin)
    result.getError shouldBe None
  }

  test("read tables saved earlier in the session") {
    val result = run("""from [[1, 10], [2, 20]] as t(id, v)
                       |save to saved_t
                       |
                       |from saved_t
                       |agg _.sum(v) as total
                       |test _.rows should be [[30]]""".stripMargin)
    result.getError shouldBe None
  }

  test("append rows to a saved table, casting to its column types") {
    val result = run("""from [[1, 'a']] as t(id, name)
                       |save to append_t
                       |
                       |from [[2.0, 'b']] as t(id, name)
                       |append to append_t
                       |
                       |from append_t
                       |test _.rows should be [[1, 'a'], [2, 'b']]""".stripMargin)
    result.getError shouldBe None
  }

  test("append to the listed columns, filling the others with NULL") {
    val result = run("""from [[1, 'a', 10]] as t(id, name, score)
                       |save to append_cols
                       |
                       |append to append_cols (score, id) {
                       |  from [[20, 2]] as t(s, i)
                       |}
                       |
                       |from append_cols
                       |where name is null
                       |test _.rows should be [[2, null, 20]]""".stripMargin)
    result.getError shouldBe None
  }

  test("update matching rows in place with a keyed append") {
    val result = run(
      """from [[1, 'alice'], [2, 'bob']] as t(id, name)
                       |save to upsert_t
                       |
                       |append to upsert_t on id {
                       |  from [[2, 'bobby'], [3, 'carol']] as t(id, name)
                       |}
                       |
                       |from upsert_t
                       |test _.rows should be [[1, 'alice'], [2, 'bobby'], [3, 'carol']]"""
        .stripMargin
    )
    result.getError shouldBe None
  }

  test("match keyed append columns by name, keeping unmapped columns") {
    val result = run(
      """from [[1, 'alice', 10], [2, 'bob', 20]] as t(id, name, score)
                       |save to upsert_named
                       |
                       |append to upsert_named on id {
                       |  from [[25, 2], [30, 3]] as t(score, id)
                       |}
                       |
                       |from upsert_named
                       |test _.rows should be [[1, 'alice', 10], [2, 'bob', 25], [3, null, 30]]"""
        .stripMargin
    )
    result.getError shouldBe None
  }

  test("insert keyed rows with NULL or duplicate unmatched keys") {
    val result = run(
      """from [[null, 'x']] as t(id, name)
                       |save to upsert_nulls
                       |
                       |append to upsert_nulls on id {
                       |  from [[null, 'y'], [5, 'a'], [5, 'b']] as t(id, name)
                       |}
                       |
                       |from upsert_nulls
                       |test _.rows should be [[null, 'x'], [null, 'y'], [5, 'a'], [5, 'b']]"""
        .stripMargin
    )
    result.getError shouldBe None
  }

  test("match append column lists case-insensitively") {
    val result = run("""from [[1, 'a']] as t(id, name)
                       |save to append_case
                       |
                       |append to append_case (ID) {
                       |  from [[2]] as t(x)
                       |}
                       |
                       |from append_case
                       |test _.size should be 2""".stripMargin)
    result.getError shouldBe None
  }

  test("delete only rows whose condition is true") {
    val result = run("""from [[1, 10], [2, null], [3, 30]] as t(id, v)
                       |save to delete_t
                       |
                       |from delete_t
                       |where v > 15
                       |delete
                       |
                       |from delete_t
                       |order by id
                       |test _.rows should be [[1, 10], [2, null]]""".stripMargin)
    result.getError shouldBe None
  }

  test("save to and append to a Parquet file") {
    val path   = s"target/engine-xp-write/${System.currentTimeMillis()}.parquet"
    val result = run(s"""from [[1, 'a']] as t(id, name)
                        |save to '${path}'
                        |
                        |from [[2, 'b']] as t(id, name)
                        |append to '${path}'
                        |
                        |from '${path}'
                        |order by id
                        |test _.rows should be [[1, 'a'], [2, 'b']]""".stripMargin)
    result.getError shouldBe None
  }

  test("create and drop declared tables") {
    val result = run("""table ddl_t = {
                       |  id: int
                       |  name: string
                       |  active: boolean = true
                       |}
                       |create table ddl_t
                       |create table ddl_t if not exists
                       |
                       |append to ddl_t (id, name) {
                       |  from [[1, 'a']] as t(id, name)
                       |}
                       |
                       |from ddl_t
                       |test _.rows should be [[1, 'a', true]]
                       |
                       |truncate ddl_t
                       |from ddl_t
                       |test _.size should be 0
                       |
                       |drop table ddl_t
                       |drop table ddl_t if exists""".stripMargin)
    result.getError shouldBe None
  }

  test("reject creating a table that already exists") {
    val e = intercept[WvletLangException] {
      run("""table ddl_dup = {
            |  id: int
            |}
            |create table ddl_dup
            |create table ddl_dup""".stripMargin)
    }
    e.getMessage shouldContain "already exists"
  }

  test("enforce NOT NULL and unique constraints of declared tables") {
    val notNull = intercept[WvletLangException] {
      run("""table ddl_nn = {
            |  id: int not null
            |}
            |create table ddl_nn
            |append to ddl_nn {
            |  from [[null]] as t(id)
            |}""".stripMargin)
    }
    notNull.getMessage shouldContain "NOT NULL"
    val unique = intercept[WvletLangException] {
      run("""table ddl_pk = {
            |  id: int primary key
            |}
            |create table ddl_pk
            |append to ddl_pk {
            |  from [[1], [1]] as t(id)
            |}""".stripMargin)
    }
    unique.getMessage shouldContain "unique"
  }

  test("drop schemas only with cascade when they hold tables") {
    val e = intercept[WvletLangException] {
      run("""create schema ddl_s
            |from [[1]] as t(id)
            |save to ddl_s.items
            |drop schema ddl_s""".stripMargin)
    }
    e.getMessage shouldContain "cascade"
    val result = run("""create schema ddl_s2
                       |from [[1]] as t(id)
                       |save to ddl_s2.items
                       |
                       |from [[2]] as t(id)
                       |save to items
                       |
                       |from ddl_s2.items
                       |test _.rows should be [[1]]
                       |
                       |drop schema ddl_s2 with cascade: true
                       |from items
                       |test _.rows should be [[2]]""".stripMargin)
    result.getError shouldBe None
  }

  test("never resolve a qualified table to a same-named table of another schema") {
    val e = intercept[WvletLangException] {
      run("""create schema ddl_s3
            |from [[1]] as t(id)
            |save to ddl_s3.items3
            |
            |from [[2]] as t(id)
            |save to items3
            |
            |drop schema wvlet.ddl_s3 with cascade: true
            |
            |from ddl_s3.items3""".stripMargin)
    }
    e.getMessage shouldContain "not found"
  }

  test("reject declarations the engine can't honor") {
    val pk = intercept[WvletLangException] {
      run("""table ddl_pk2 = {
            |  a: int primary key
            |  b: int primary key
            |}
            |create table ddl_pk2""".stripMargin)
    }
    pk.getMessage shouldContain "more than one primary key"
    val main = intercept[WvletLangException] {
      run("drop schema main with cascade: true")
    }
    main.getMessage shouldContain "default schema"
  }

  test("format integers as strings without a fraction") {
    val result = run("""from [[1, 2.5]] as t(id, price)
                       |select
                       |  id.to_string as s,
                       |  s"#${id}" as label,
                       |  id + '' as joined,
                       |  price.to_string as p
                       |test _.rows should be [['1', '#1', '1', '2.5']]
                       |
                       |from [[1], [2]] as t(id)
                       |agg id.string_agg(',') as ids
                       |test _.rows should be [['1,2']]""".stripMargin)
    result.getError shouldBe None
  }

  test("reshape and rename in-memory tables") {
    val result = run("""table alt_t = {
                       |  id: int
                       |  name: string = 'none'
                       |}
                       |create table alt_t
                       |append to alt_t {
                       |  from [[1, 'pen'], [2, 'book']] as t(id, name)
                       |}
                       |
                       |reshape alt_t {
                       |  add price: double
                       |  rename name as item
                       |  cast id as long
                       |}
                       |
                       |from alt_t
                       |order by id
                       |test _.columns should be ['id', 'item', 'price']
                       |test _.rows should be [[1, 'pen', null], [2, 'book', null]]
                       |
                       |-- The default follows the renamed column
                       |append to alt_t (id) {
                       |  from [[3]] as t(id)
                       |}
                       |from alt_t
                       |where id = 3
                       |test _.rows should be [[3, 'none', null]]
                       |
                       |reshape alt_t {
                       |  exclude price
                       |}
                       |rename table alt_t to alt_u
                       |
                       |from alt_u
                       |test _.columns should be ['id', 'item']
                       |test _.size should be 3""".stripMargin)
    result.getError shouldBe None
  }

  test("reject altering missing columns and file tables") {
    val e = intercept[WvletLangException] {
      run("""from [[1]] as t(id)
            |save to alt_m
            |reshape alt_m {
            |  exclude missing
            |}""".stripMargin)
    }
    e.getMessage shouldContain "missing is not found"
  }

  test("create an Iceberg table and append a snapshot") {
    val path   = s"target/engine-xp-iceberg/${System.currentTimeMillis()}"
    val result = run(s"""from [[1, 'JP'], [2, 'US']] as t(id, country)
                        |save to '${path}' with format: 'iceberg', partition_by: 'country'
                        |
                        |from [[3, 'JP']] as t(id, country)
                        |append to '${path}'
                        |
                        |from '${path}'
                        |order by id
                        |test _.rows should be [[1, 'JP'], [2, 'US'], [3, 'JP']]""".stripMargin)
    result.getError shouldBe None
  }

  test("replace an Iceberg table snapshot with save to") {
    val path   = s"target/engine-xp-iceberg-replace/${System.currentTimeMillis()}"
    val result = run(s"""from [[1, 'JP'], [2, 'US']] as t(id, country)
                        |save to '${path}' with format: 'iceberg', partition_by: 'country'
                        |
                        |-- Replaces every row, keeping the partition spec
                        |from [[3, 'JP']] as t(id, country)
                        |save to '${path}' with format: 'iceberg'
                        |
                        |from '${path}'
                        |test _.rows should be [[3, 'JP']]
                        |
                        |-- The schema follows the new result
                        |from [[4, 'pen', 1.5]] as t(id, item, price)
                        |save to '${path}' with format: 'iceberg'
                        |
                        |from '${path}'
                        |test _.columns should be ['id', 'item', 'price']
                        |test _.rows should be [[4, 'pen', 1.5]]""".stripMargin)
    result.getError shouldBe None
    // Older snapshots stay readable
    val first = run(s"""from '${path}/metadata/v1.metadata.json'
                       |order by id
                       |test _.rows should be [[1, 'JP'], [2, 'US']]""".stripMargin)
    first.getError shouldBe None
  }

  test("reject saving an Iceberg table over a folder with other files") {
    val path = s"target/engine-xp-iceberg-nonempty/${System.currentTimeMillis()}"
    run(s"""from [[1]] as t(id)
           |save to '${path}/data.parquet'""".stripMargin).getError shouldBe None
    val e = intercept[WvletLangException] {
      run(s"""from [[2]] as t(id)
             |save to '${path}' with format: 'iceberg'""".stripMargin)
    }
    e.getMessage shouldContain "not an empty folder"
  }

  test("reject SQL statements the engine can't run") {
    val e = intercept[WvletLangException] {
      run("execute sql\"select 1\"")
    }
    e.getMessage shouldContain "not supported by the wvlet engine"
  }

  test("switch to the engine with use <connector>") {
    val p = Profile(
      "test",
      List(
        ConnectorConfig(name = "duckdb", `type` = "duckdb", default = true),
        engineConfig.withDefault(false)
      )
    )
    val result = run(
      """use wvlet
        |from [[1], [2]] as t(a)
        |test _.size should be 2""".stripMargin,
      p
    )
    result.getError shouldBe None
  }

end PlanExecutorWvletEngineTest

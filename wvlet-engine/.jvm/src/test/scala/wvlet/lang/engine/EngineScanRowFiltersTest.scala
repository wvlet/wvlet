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
package wvlet.lang.engine

import wvlet.lang.compiler.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `scan_row_filters`: the conjuncts a single-file Parquet scan evaluates exactly move into its row
  * filter, and results stay those of the engine's own filter
  */
class EngineScanRowFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.ScanRowFilters))

  /**
    * 1000 rows in row groups of 100 (so that scans split into morsels), with integer, date,
    * timestamp (micros and nanos), boolean, string, decimal, and double columns, some NULLs
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("scan_row_filters.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    (range % 7)::integer as k,
             |    date '2020-01-01' + (range % 400)::integer as d,
             |    timestamp '2020-01-01 00:00:00' + to_microseconds(range * 1000001) as ts,
             |    (timestamp_ns '2020-01-01 00:00:00' + to_microseconds(range) + interval 1 millisecond)::timestamp_ns as tns,
             |    range % 3 = 0 as flag,
             |    case when range % 11 = 0 then null else 'm' || (range % 5) end as mode,
             |    (range % 50)::decimal(12, 2) as qty,
             |    (range % 13) * 0.5 as price
             |  from range(1000)
             |) to '${path}' (format parquet, row_group_size 100)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run[A](e: WvletEngine, query: String)(body: Relation => Context ?=> A): A =
    val unit           = CompilationUnit.fromWvletString(query.replace("$t", s"'${file}'"))
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    body(q)

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def explain(e: WvletEngine, query: String): String =
    val plan = run(e, query)(q => e.explain(q))
    debug(plan)
    plan

  private def rows(e: WvletEngine, query: String): Seq[String] =
    run(e, query)(q => e.execute(q).rows.map(_.mkString(",")).toSeq.sorted)

  private def sameRows(query: String): Seq[String] =
    val r = rows(on, query)
    r shouldBe rows(off, query)
    r

  test("move exact conjuncts into the scan and keep the others in the filter") {
    val q =
      """from $t
              |where k = 3 and d >= '2020-03-01'::date and qty < 20 and mode = 'm1'
              |select id""".stripMargin
    val plan = explain(on, q)
    plan shouldContain "row_filter="
    // The decimal comparison stays in the engine's filter
    plan shouldContain "Filter"
    plan shouldContain "qty"
    // Most selective first: the date range (about 0.85 of the rows) after k = 3 and mode = 'm1'
    val rowFilter = plan.linesIterator.find(_.contains("row_filter=")).get
    rowFilter.indexOf("Compare(d,") > rowFilter.indexOf("Compare(k,") shouldBe true
    sameRows(q).nonEmpty shouldBe true
  }

  test("drop the filter when every conjunct moves") {
    val q    = "from $t where k = 3 and flag select id"
    val plan = explain(on, q)
    plan shouldContain "row_filter="
    plan shouldNotContain "Filter"
    explain(off, q) shouldNotContain "row_filter="
    sameRows(q).size shouldBe 1000 / 21 + 1
  }

  test("return the same rows with the toggle on and off") {
    Seq(
      "from $t where id < 250 or id >= 990 select id",
      "from $t where id in (1, 2, 500, 999) select id, mode",
      "from $t where mode is null select id",
      "from $t where mode is not null and k != 2 select id",
      "from $t where mode = 'm2' or k = 6 select id",
      "from $t where d between '2020-02-01'::date and '2020-02-10'::date select id, d",
      "from $t where ts >= '2020-01-01 00:05:00'::timestamp select id",
      "from $t where flag = false and price > 3 select id",
      // column-to-column: stays in the engine
      "from $t where id < k * 100 select id",
      "from $t where k = 1 group by mode select mode, n = _.count"
    ).foreach(q => sameRows(q))
  }

  test("keep comparisons on nanosecond timestamps in the engine") {
    // The engine reads nanos floored to micros, so uni comparing the nanos would disagree
    val q    = "from $t where tns <= '2020-01-01 00:00:00.001003'::timestamp select id"
    val plan = explain(on, q)
    plan shouldNotContain "row_filter="
    sameRows(q).size shouldBe 4
  }

end EngineScanRowFiltersTest

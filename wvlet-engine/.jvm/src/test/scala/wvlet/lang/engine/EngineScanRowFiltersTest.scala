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
import wvlet.lang.engine.catalog.ColumnPartRange
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.RuntimeFilter
import wvlet.lang.engine.exec.ScanOp
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.LongVec
import wvlet.lang.engine.vector.PType
import wvlet.lang.model.plan.*
import wvlet.uni.cloud.parquet.ParquetFilter
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
  private val noJoin  = on.withOptimizer(options.withDisabled(Optimization.ScanJoinRowFilters))
  private val loose   = on.withOptimizer(options.withDisabled(Optimization.SelectiveScanRowFilters))
  private val noValues = on.withOptimizer(options.withDisabled(Optimization.StringValueSelectivity))
  private val noImplied = on.withOptimizer(
    options.withDisabled(Optimization.ColumnImpliedRowFilters)
  )

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
             |    (range % 13) * 0.5 as price,
             |    case when range % 50 = 7 then 'rare' else 'common' end as tag
             |  from range(1000)
             |) to '${path}' (format parquet, row_group_size 100)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  /**
    * 20480 rows in 10 row groups of 2048 (DuckDB writes one row group below 2048 rows), with ids
    * clustered by row group and dates spread over every row group, for runtime join filters
    */
  private lazy val joinFile: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("scan_join_row_filters.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    date '2020-01-01' + (range % 400)::integer as d,
             |    case when range % 11 = 0 then null else 'm' || (range % 5) end as mode,
             |    (range % 50)::decimal(12, 2) as qty
             |  from range(20480)
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run[A](e: WvletEngine, query: String)(body: Relation => Context ?=> A): A =
    val unit = CompilationUnit.fromWvletString(
      query.replace("$t", s"'${file}'").replace("$j", s"'${joinFile}'")
    )
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

  test("count the rows of a row filter by a column it reads anyway") {
    val q    = "from $t where d >= '2020-12-15'::date select n = _.count"
    val plan = explain(on, q)
    plan shouldContain "row_filter="
    // Not the first column (id), which the scan would then decode for every matching row
    plan shouldContain "Scan[scan_row_filters](columns=d filter="
    // Day offsets 349 until 400, in 2 of the 2.5 cycles of 400 rows
    sameRows(q) shouldBe Seq("102")
  }

  test("keep exact conjuncts in the engine when they are estimated to keep many rows") {
    // k in [1, 5] keeps 5 of its 7 values, more than MaxRowFilterSelectivity
    val q = "from $t where k between 1 and 5 select id"
    explain(on, q) shouldNotContain "row_filter="
    explain(on, q) shouldContain "Filter"
    explain(loose, q) shouldContain "row_filter="
    val r = rows(on, q)
    r.size shouldBe 1000 * 5 / 7 + 1
    r shouldBe rows(loose, q)
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

  test("test selective runtime join filters in the row filter of every morsel") {
    // The builds keep 21 of the 20480 ids, or 9 of the 400 dates, spread over the 10 row groups, so
    // their ranges skip no row group and their per-row filters go into the probe scan's row filter
    val queries = Seq(
      """from $j as a
        |join { from $j where id % 997 = 0 select id as bid } on a.id = bid
        |select a.id, a.mode, a.qty""".stripMargin,
      """from $j as a
        |join { from $j where id % 997 = 0 and id < 9000 select d as bd } on a.d = bd
        |select a.id, a.mode, a.qty""".stripMargin
    )
    // The row filters the scans read with, after running the plan (builds set the join filters)
    def scanRowFilters(e: WvletEngine, q: String): Seq[String] =
      run(e, q) { r =>
        val op = e.physicalPlan(r)
        op.drain()
        op.close()
        def scans(o: Operator): Seq[ScanOp] =
          o match
            case s: ScanOp =>
              Seq(s)
            case other =>
              other.children.flatMap(scans) ++ other.sharedInputs.flatMap(scans)
        scans(op).flatMap(_.spec.rowFilter.map(_.toString))
      }
    queries.foreach { q =>
      debug(scanRowFilters(on, q))
      scanRowFilters(on, q).exists(_.contains("In(")) shouldBe true
      scanRowFilters(noJoin, q).exists(_.contains("In(")) shouldBe false
      val r = rows(on, q)
      r.nonEmpty shouldBe true
      r shouldBe rows(noJoin, q)
      r shouldBe rows(off, q)
    }
  }

  test("test runtime join filters keeping fewer rows before the scan's own row filter") {
    // 21 build keys spread over the 10 row groups keep 0.1% of the ids, and `d < 2020-11-01` 305
    // of the 400 dates (76%)
    val src  = ParquetSource("t", joinFile)
    val ids  = (0L until 20480L).filter(_ % 997 == 0).toArray
    val days = java.time.LocalDate.of(2020, 11, 1)
    def scan(passRate: Double, first: Boolean): ScanOp =
      val rf = RuntimeFilter(0, "id", (_, _) => None, rows = true)
      rf.set(LongVec(PType.Int64, ids, null), ids.indices.toArray, ids.length)
      ScanOp(src, src.schema, None, None, Seq(rf), rowFilter = Some(ParquetFilter.col("d") < days))
        .withRowFilterPassRate(passRate)
        .withJoinFiltersFirst(first)
    def rowFilter(s: ScanOp): String     = s.spec.rowFilter.map(_.toString).getOrElse("")
    def scannedIds(s: ScanOp): Seq[Long] =
      val out = Seq.newBuilder[Long]
      var b   = s.next()
      while b != null do
        val v = b.column(0)
        (0 until b.size).foreach(i => out += v.get(i).asInstanceOf[Long])
        b = s.next()
      s.close()
      out.result()
    val joinFirst = rowFilter(scan(0.76, first = true))
    debug(joinFirst)
    joinFirst shouldContain "In("
    joinFirst.indexOf("In(") < joinFirst.indexOf("Compare(d,") shouldBe true
    // The scan's own filter stays first when it keeps fewer rows, or with the toggle off
    Seq(scan(0.0001, first = true), scan(0.76, first = false)).foreach { s =>
      val f = rowFilter(s)
      f.indexOf("Compare(d,") < f.indexOf("In(") shouldBe true
    }
    val expected = ids.filter(i => i % 400 < 305).toSeq
    scannedIds(scan(0.76, first = true)) shouldBe expected
    scannedIds(scan(0.76, first = false)) shouldBe expected
  }

  test("read the range of a column in each row group") {
    val src   = ParquetSource("t", joinFile)
    val parts = src.partRanges("id").getOrElse(fail("no row group ranges"))
    parts shouldBe (0 until 10).map(g => ColumnPartRange(g * 2048L, g * 2048L + 2047, 2048L))
    // Dates as epoch days
    val d0 = java.time.LocalDate.of(2020, 1, 1).toEpochDay
    src.partRanges("d").get.head shouldBe ColumnPartRange(d0, d0 + 399, 2048L)
    src.partRanges("mode") shouldBe None
  }

  test("keep join filters in the engine when most rows of the row groups they leave pass") {
    // Ids are clustered: the build's range leaves the first row group, where 600 of its 2048 rows
    // pass, though 600 of all 20480 ids would be 3%
    val q =
      """from $j as a
        |join { from $j where id < 600 select id as bid } on a.id = bid
        |select a.id, a.mode""".stripMargin
    val filters =
      run(on, q) { r =>
        val op = on.physicalPlan(r)
        op.drain()
        op.close()
        def scans(o: Operator): Seq[ScanOp] =
          o match
            case s: ScanOp =>
              Seq(s)
            case other =>
              other.children.flatMap(scans) ++ other.sharedInputs.flatMap(scans)
        scans(op).flatMap(_.spec.rowFilter.map(_.toString))
      }
    filters.exists(_.contains("In(")) shouldBe false
    sameRows(q).size shouldBe 600
  }

  test("estimate string conditions from the sampled values of the column") {
    val src = ParquetSource("t", file)
    val vc  = src.valueCounts("tag").get
    vc.rows shouldBe 1000L
    vc.values.toSeq.sorted shouldBe Seq("common", "rare")
    vc.counts.sum shouldBe 1000L
    src.valueCounts("mode").get.nulls shouldBe 91L
    // Not a string column
    src.valueCounts("k") shouldBe None
    // 'rare' keeps 2% of the rows, below MaxRowFilterSelectivity; the default 0.3 is above it
    val rare = "from $t where tag = 'rare' select id"
    explain(on, rare) shouldContain "row_filter="
    explain(noValues, rare) shouldNotContain "row_filter="
    rows(on, rare).size shouldBe 20
    rows(on, rare) shouldBe rows(noValues, rare)
    // 'common' keeps 98%: stays in the engine
    explain(on, "from $t where tag = 'common' select id") shouldNotContain "row_filter="
    sameRows("from $t where tag in ('rare', 'none') and mode is null select id")
  }

  test("give the row filter the per-column filters implied by a multi-column or") {
    // TPC-H q19's shape: each branch tests tag, k and qty
    val q =
      """from $t
        |where (tag = 'rare' and k = 1 and qty < 10) or (tag = 'none' and k in (2, 3) and qty < 20)
        |select id""".stripMargin
    val plan      = explain(on, q)
    val rowFilter = plan.linesIterator.find(_.contains("row_filter=")).getOrElse("")
    rowFilter shouldContain "Compare(tag,Eq,rare)"
    // The or itself (with its decimal comparisons) stays in the engine's filter
    plan shouldContain "Filter("
    explain(noImplied, q) shouldNotContain "row_filter="
    val r = rows(on, q)
    r shouldBe rows(noImplied, q)
    r shouldBe rows(off, q)
    r.nonEmpty shouldBe true
  }

  test("return the same rows with column-implied row filters on and off") {
    Seq(
      // A branch without a test of mode implies nothing for it
      "from $t where (k = 1 and mode = 'm1') or (k = 2) select id",
      "from $t where (k = 1 and mode is null) or (k = 3 and flag) select id, mode",
      "from $t where (d < '2020-02-01'::date and k = 1) or (d > '2020-12-01'::date and k > 4) select id",
      "from $t where (id < 100 and mode = 'm0') or (id > 900 and (mode = 'm4' or mode is null)) select id",
      "from $t where (tag = 'rare' and mode = 'm2') or (tag = 'rare' and k = 4) select id"
    ).foreach { q =>
      val r = rows(on, q)
      r shouldBe rows(noImplied, q)
      r shouldBe rows(off, q)
      r shouldBe rows(noValues, q)
    }
  }

end EngineScanRowFiltersTest

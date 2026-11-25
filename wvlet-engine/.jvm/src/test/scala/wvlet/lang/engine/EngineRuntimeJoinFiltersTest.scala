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
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/** Runtime join filters: probe-side scans skip row groups outside the range of the build keys */
class EngineRuntimeJoinFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  private val off     = engine.withOptimizer(options.withDisabled(Optimization.RuntimeJoinFilters))
  private val noRows  = engine.withOptimizer(options.withDisabled(Optimization.RowJoinFilters))
  // Per-row filters stay in the engine's ScanOp.narrow, which counts the rows they drop
  private val inEngine = engine.withOptimizer(options.withDisabled(Optimization.ScanJoinRowFilters))
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](sql: String)(f: Context ?=> wvlet.lang.model.plan.Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(sql)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${unit}"))
    f(GenSQL.expand(q, summon[Context]))

  private def scans(op: Operator): Seq[ScanOp] =
    op match
      case s: ScanOp =>
        Seq(s)
      case other =>
        other.children.flatMap(scans)

  /** Run the query, returning its rows and the filter each scan read with, by table */
  private def run(e: WvletEngine, sql: String): (Seq[IndexedSeq[Any]], Map[String, String]) =
    withQuery(sql) { r =>
      val op   = e.physicalPlan(r)
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toIndexedSeq))
      op.close()
      val filters = scans(op).map(s => s.source.name -> s.spec.filter.map(_.toString).getOrElse(""))
      (rows, filters.toMap)
    }

  private def explain(e: WvletEngine, sql: String): String = withQuery(sql)(e.explain(_))

  private val innerJoin =
    """select o_orderkey, l_linenumber from lineitem join orders on l_orderkey = o_orderkey
      |where o_orderdate < date '1992-01-10' order by 1, 2""".stripMargin

  test("filter the probe scan by the range of the build keys") {
    explain(engine, innerJoin) shouldContain "join_filter=l_orderkey"
    val (rows, filters) = run(engine, innerJoin)
    debug(filters)
    filters("lineitem") shouldContain "Compare(l_orderkey,GtEq,"
    filters("lineitem") shouldContain "Compare(l_orderkey,LtEq,"
    rows.nonEmpty shouldBe true
    run(off, innerJoin)._1 shouldBe rows
  }

  test("filter the probe scan of a semi join") {
    val q =
      """select l_orderkey, l_linenumber from lineitem
        |where l_orderkey in (select o_orderkey from orders where o_orderdate < date '1992-01-10')
        |order by 1, 2""".stripMargin
    val (rows, filters) = run(engine, q)
    filters("lineitem") shouldContain "Compare(l_orderkey,GtEq,"
    run(off, q)._1 shouldBe rows
  }

  test("filter each input scan of a union all on the probe side") {
    val q =
      """select u.k, u.n from (
        |  select l_partkey k, l_linenumber n from lineitem
        |  union all
        |  select ps_partkey k, ps_suppkey n from partsupp
        |) u join part on u.k = p_partkey
        |where p_name like 'forest%' order by 1, 2""".stripMargin
    val (rows, filters) = run(engine, q)
    debug(filters)
    filters("lineitem") shouldContain "Compare(l_partkey,GtEq,"
    filters("partsupp") shouldContain "Compare(ps_partkey,GtEq,"
    rows.nonEmpty shouldBe true
    run(off, q)._1 shouldBe rows
  }

  test("leave the probe side of joins that keep unmatched probe rows unfiltered") {
    val q =
      """select l_orderkey, l_linenumber, o_orderkey from lineitem
        |left join (select o_orderkey from orders where o_orderdate < date '1992-01-10') o
        |  on l_orderkey = o_orderkey
        |order by 1, 2""".stripMargin
    explain(engine, q) shouldNotContain "join_filter=l_orderkey"
    val (rows, _) = run(engine, q)
    run(off, q)._1 shouldBe rows
  }

  test("leave scans below an aggregation unfiltered") {
    val q =
      """select o_orderkey, n from orders
        |join (select l_orderkey, count(*) n from lineitem group by l_orderkey) l
        |  on l_orderkey = o_orderkey
        |where o_orderdate < date '1992-01-10' order by 1""".stripMargin
    // Without the semi join on the group key, which filters the aggregation's input on purpose
    val noGroupKeySemiJoins = engine.withOptimizer(
      options.withDisabled(Optimization.GroupKeySemiJoins)
    )
    explain(noGroupKeySemiJoins, q) shouldNotContain "join_filter=l_orderkey"
  }

  /** Run the query, returning its rows and the per-row filters its scans applied */
  private def runRows(e: WvletEngine, sql: String): (Seq[IndexedSeq[Any]], Seq[RowKeyFilter]) =
    withQuery(sql) { r =>
      val op   = e.physicalPlan(r)
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toIndexedSeq))
      op.close()
      (rows, scans(op).flatMap(_.runtimeFilters).flatMap(f => Option(f.rowFilter)).distinct)
    }

  test("drop probe rows whose key is no build key, per row") {
    val (rows, filters) = runRows(inEngine, innerJoin)
    filters.nonEmpty shouldBe true
    filters.map(_.droppedRows).sum > 0 shouldBe true
    rows.nonEmpty shouldBe true
    rows shouldBe runRows(noRows, innerJoin)._1
    rows shouldBe run(off, innerJoin)._1
  }

  test("keep the rows of semi, right, and anti joins and of NULL keys") {
    val queries = Seq(
      """select l_orderkey, l_linenumber from lineitem
        |where l_orderkey in (select o_orderkey from orders where o_orderdate < date '1992-03-01')
        |order by 1, 2""".stripMargin,
      // Probe rows and build rows with NULL keys never match
      """select a.k, b.n from (select case when l_linenumber = 1 then null else l_partkey end k
        |  from lineitem) a
        |join (select case when p_size < 10 then null else p_partkey end k, p_size n
        |  from part where p_size < 20) b on a.k = b.k
        |order by 1, 2""".stripMargin,
      """select o_orderkey from orders
        |where not exists (select 1 from lineitem where l_orderkey = o_orderkey and l_quantity > 49)
        |order by 1 limit 50""".stripMargin,
      """select c_custkey, o_orderkey from orders
        |right join (select c_custkey from customer where c_nationkey = 3) c on o_custkey = c_custkey
        |order by 1, 2""".stripMargin
    )
    queries.foreach { q =>
      val rows = runRows(engine, q)._1
      rows.nonEmpty shouldBe true
      rows shouldBe runRows(noRows, q)._1
    }
  }

  test("narrow probe batches with eager gathers when late_gather is off") {
    val eager = engine.withOptimizer(options.withDisabled(Optimization.LateGather))
    runRows(eager, innerJoin)._1 shouldBe runRows(noRows, innerJoin)._1
  }

  /** The row filter each scan read with, by table, after running the query */
  private def rowFilters(e: WvletEngine, sql: String): (Seq[IndexedSeq[Any]], Map[String, String]) =
    withQuery(sql) { r =>
      val op   = e.physicalPlan(r)
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toIndexedSeq))
      op.close()
      val filters = scans(op).map(s =>
        s.source.name -> s.spec.rowFilter.map(_.toString).getOrElse("")
      )
      (rows, filters.toMap)
    }

  test("test selective per-row filters in the probe scan's row filter") {
    // About 1% of the order keys pass, so the probe scan decodes the other columns only for them
    val (rows, filters) = rowFilters(engine, innerJoin)
    debug(filters)
    filters("lineitem") shouldContain "In(l_orderkey,"
    rows.nonEmpty shouldBe true
    rows shouldBe runRows(inEngine, innerJoin)._1
    rows shouldBe run(off, innerJoin)._1
    rowFilters(inEngine, innerJoin)._2("lineitem") shouldNotContain "In("
  }

  test("add per-row filters to the conjuncts already in the probe scan's row filter") {
    val q =
      """select o_orderkey, l_linenumber from lineitem join orders on l_orderkey = o_orderkey
        |where o_orderdate < date '1992-01-10' and l_shipdate < date '1992-03-01'
        |order by 1, 2""".stripMargin
    val (rows, filters) = rowFilters(engine, q)
    filters("lineitem") shouldContain "Compare(l_shipdate,Lt,"
    filters("lineitem") shouldContain "In(l_orderkey,"
    rows.nonEmpty shouldBe true
    rows shouldBe runRows(inEngine, q)._1
  }

  test("keep per-row filters that pass most rows in the engine") {
    val q =
      """select o_orderkey, l_linenumber from lineitem join orders on l_orderkey = o_orderkey
        |where o_orderdate < date '1997-01-01' order by 1, 2 limit 20""".stripMargin
    val (rows, filters) = rowFilters(engine, q)
    filters("lineitem") shouldNotContain "In("
    rows shouldBe runRows(inEngine, q)._1
  }

  test("combine per-row filters on several columns into one row filter") {
    // Each filter alone keeps too many rows, but together they keep few
    val q =
      """select l_orderkey, l_linenumber from lineitem
        |join orders on l_orderkey = o_orderkey
        |join part on l_partkey = p_partkey
        |where o_orderdate < date '1994-01-01' and p_size < 10
        |order by 1, 2""".stripMargin
    val (rows, filters) = rowFilters(engine, q)
    debug(filters)
    filters("lineitem") shouldContain "In(l_orderkey,"
    filters("lineitem") shouldContain "In(l_partkey,"
    rows.nonEmpty shouldBe true
    rows shouldBe runRows(inEngine, q)._1
    rows shouldBe run(off, q)._1
  }

  test("turn off per-row filters with row_join_filters") {
    runRows(noRows, innerJoin)._2 shouldBe Nil
  }

  test("turn off with runtime_join_filters") {
    explain(off, innerJoin) shouldNotContain "join_filter="
  }

end EngineRuntimeJoinFiltersTest

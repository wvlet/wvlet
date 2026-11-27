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
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.Schema
import wvlet.uni.test.UniTest

import java.util.concurrent.atomic.AtomicInteger

/**
  * A table that an uncorrelated scalar subquery and the rest of the query filter by the same
  * conjuncts is read once, as a shared CTE (`share_subquery_scans`), with the rows DuckDB returns
  */
class EngineSharedScansTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.ShareSubqueryScans))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def explain(e: WvletEngine, query: String): String =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val plan      = e.explain(GenSQL.expand(q, summon[Context]))
    debug(plan)
    plan

  private def readsCTE(plan: String): Boolean = plan.contains("CTE[__wv_scan")

  /**
    * The query matches DuckDB with the rewrite on and off; whether the rewrite read the table as a
    * shared CTE
    */
  private def sharesScan(query: String): Boolean =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    readsCTE(explain(disabled, query)) shouldBe false
    readsCTE(explain(conformance.engine, query))

  private val codes = "c_phone.substring(1, 2).in('13', '31', '23', '29', '30', '18', '17')"

  private val q22 =
    s"""from customer
       |where ${codes}
       |  and c_acctbal > {
       |    from customer
       |    where c_acctbal > 0.00 and ${codes}
       |    select c_acctbal.avg
       |  }
       |  and not exists { from orders where o_custkey = c_custkey }
       |select cntrycode = c_phone.substring(1, 2), c_acctbal
       |group by cntrycode
       |select cntrycode, numcust = _.count, totacctbal = c_acctbal.sum
       |order by cntrycode
       |""".stripMargin

  test("share the filtered customer scan of TPC-H q22's avg subquery") {
    sharesScan(q22) shouldBe true
  }

  test("share the scan with a subquery in an aggregation's condition") {
    val query =
      """from lineitem
        |where l_shipmode = 'AIR'
        |group by l_suppkey
        |where l_quantity.avg > {
        |  from lineitem
        |  where l_shipmode = 'AIR' and l_returnflag = 'R'
        |  select l_quantity.avg
        |}
        |select l_suppkey, q = l_quantity.avg
        |order by q desc, l_suppkey
        |limit 10
        |""".stripMargin
    sharesScan(query) shouldBe true
  }

  test("keep reading the table when the filters share no conjunct") {
    val query =
      """from customer
        |where c_mktsegment = 'BUILDING'
        |  and c_acctbal > { from customer where c_nationkey = 3 select c_acctbal.avg }
        |select c_custkey, c_acctbal
        |order by c_custkey
        |limit 10
        |""".stripMargin
    sharesScan(query) shouldBe false
  }

  test("keep reading the table when the output lists its columns unnamed") {
    val query =
      """from customer
        |where c_mktsegment = 'BUILDING'
        |  and c_acctbal > {
        |    from customer where c_mktsegment = 'BUILDING' select c_acctbal.avg
        |  }
        |order by c_custkey
        |limit 10
        |""".stripMargin
    sharesScan(query) shouldBe false
  }

  test("keep reading the table when dedup compares every column") {
    val query =
      """from customer
        |where c_mktsegment = 'BUILDING'
        |  and c_acctbal > {
        |    from customer where c_mktsegment = 'BUILDING' select c_acctbal.avg
        |  }
        |dedup
        |count
        |""".stripMargin
    sharesScan(query) shouldBe false
  }

  /** customer in memory, counting its scans */
  private class CountingCustomer(batches: Seq[Batch], schema: Schema)
      extends MemTable("customer", schema, batches):
    val scans                                          = AtomicInteger(0)
    override def scan(spec: ScanSpec): Iterator[Batch] =
      scans.incrementAndGet()
      super.scan(spec)

  /** The scans of customer by TPC-H q22, run on an engine with these options */
  private def customerScans(o: OptimizerOptions): Int =
    val parquet  = ParquetSource("customer", "spec/tpch/data/customer.parquet")
    val customer = CountingCustomer(parquet.scan(ScanSpec()).toSeq, parquet.schema)
    val e        = WvletEngine(optimizer = o)
    e.catalog.register(customer)
    e.catalog.registerParquet("orders", "spec/tpch/data/orders.parquet")
    val unit      = CompilationUnit.fromWvletString(q22)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${q22}"))
    e.execute(GenSQL.expand(q, summon[Context])).rows.size shouldBe 7
    customer.scans.get()

  test("read the shared scan in q22's subquery, which splitting the condition copies") {
    // The subquery's templates (avg, substring ... in) are expanded with the main query's
    // condition, so the planner binds a copy of it (`match_copied_subquery_scans`)
    customerScans(options) shouldBe 1
    customerScans(options.withDisabled(Optimization.MatchCopiedSubqueryScans)) shouldBe 2
  }

end EngineSharedScansTest

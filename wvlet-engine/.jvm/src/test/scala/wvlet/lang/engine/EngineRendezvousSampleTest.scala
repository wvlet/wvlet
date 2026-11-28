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
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SourceResolver
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.RawSQL
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** Rendezvous (key-consistent) sampling over the TPC-H sample data */
class EngineRendezvousSampleTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)
  EngineTPCHBench
    .tables
    .foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def count(query: String): Long =
    withQuery(query)(q => engine.execute(q).batches.map(_.size.toLong).sum)

  private def bound(query: String): RuntimePlan =
    withQuery(query) { r =>
      val planner = Planner(
        engine.catalog,
        new SourceResolver:
          override def tableSource(name: String, fullName: String): Option[TableSource] = None
          override def fileSource(path: String): Option[TableSource]                    = None
          override def parseSql(r: RawSQL): Relation = fail("No SQL")
        ,
        options
      )
      RuntimeBinder(planner).bind(r)
    }

  private def find[A](p: RuntimePlan)(f: PartialFunction[RuntimePlan, A]): Option[A] = f
    .lift(p)
    .orElse(p.children.iterator.flatMap(find(_)(f)).nextOption())

  test("should keep about the sampling rate of the keys") {
    val all     = count("from customer")
    val sampled = count("from customer sample rendezvous(20%) by c_custkey")
    debug(s"${sampled} of ${all}")
    (sampled > all * 15 / 100) shouldBe true
    (sampled < all * 25 / 100) shouldBe true
  }

  test("should keep the same keys in samples of two tables") {
    // Every order of a sampled customer finds its customer in the customer sample
    val orders = count("from orders sample rendezvous(20%) by o_custkey")
    val joined = count("""from orders
        |sample rendezvous(20%) by o_custkey
        |join { from customer sample rendezvous(20%) by c_custkey } as c
        |  on o_custkey = c.c_custkey""".stripMargin)
    (orders > 0) shouldBe true
    joined shouldBe orders
  }

  test("should keep or drop all rows of a key together") {
    // A customer's orders are either all in the sample or all out of it
    val partial = count("""from orders
        |group by o_custkey
        |agg _.count as total
        |join {
        |  from orders
        |  sample rendezvous(30%) by o_custkey
        |  group by o_custkey
        |  agg _.count as kept
        |  select o_custkey as kept_key, kept
        |} as s on o_custkey = s.kept_key""".stripMargin + "\nwhere total != s.kept")
    partial shouldBe 0
    val keys = count(
      "from orders sample rendezvous(30%) by o_custkey group by o_custkey agg _.count as n"
    )
    (keys > 0) shouldBe true
  }

  test("should draw a different sample with a different seed") {
    val q1   = "from customer sample rendezvous(50%) by c_custkey select c_custkey"
    val q2   = "from customer sample rendezvous(50%, seed = 7) by c_custkey select c_custkey"
    val both = count(s"""from { ${q1} } as a
         |join { ${q2} } as b on a.c_custkey = b.c_custkey""".stripMargin)
    val n = count(q1)
    (both < n) shouldBe true
  }

  test("should bind the sample as a filter on the key hash") {
    val p    = bound("from customer sample rendezvous(10%) by c_custkey")
    val cond =
      find(p) { case Filter(_: Scan, cond, _) =>
        cond.toString
      }
    cond.getOrElse(fail(s"No filter over the scan:\n${p.explain}")) shouldContain
      "rendezvous_sample[10.0%,seed=0]("
  }

  test("should keep at most k rows per key") {
    // No customer keeps more than 2 orders, and every customer with orders keeps some
    count("""from orders
        |sample rendezvous(2) by o_custkey
        |group by o_custkey
        |agg _.count as n
        |where n > 2""".stripMargin) shouldBe 0
    val customers = count("from orders group by o_custkey agg _.count as n")
    count(
      "from orders sample rendezvous(2) by o_custkey group by o_custkey agg _.count as n"
    ) shouldBe customers
    // Each customer keeps min(orders, 2) rows
    val bounded =
      withQuery("""from orders
        |group by o_custkey
        |agg _.count as n
        |select if n > 2 then 2 else n as m
        |agg m.sum as total""".stripMargin)(q => engine.execute(q).batches.head.column(0).get(0))
    count("from orders sample rendezvous(2) by o_custkey") shouldBe bounded.toString.toLong
  }

  test("should keep the same rows for the same seed") {
    val q1 = "from orders sample rendezvous(1, seed = 3) by o_custkey select o_orderkey"
    val q2 = "from orders sample rendezvous(1, seed = 4) by o_custkey select o_orderkey"
    val n  = count(q1)
    count(s"from { ${q1} } as a join { ${q1} } as b on a.o_orderkey = b.o_orderkey") shouldBe n
    val same = count(s"from { ${q1} } as a join { ${q2} } as b on a.o_orderkey = b.o_orderkey")
    (same < n) shouldBe true
  }

  test("should bound the groups each key contributes to after a group by") {
    // Contribution bounding: each customer adds to at most 2 order-priority groups
    val query =
      """from orders
        |group by o_custkey, o_orderpriority
        |agg _.count as n
        |sample rendezvous(2) by o_custkey""".stripMargin
    count(s"""from { ${query} }
         |group by o_custkey
         |agg _.count as groups
         |where groups > 2""".stripMargin) shouldBe 0
    val users = count(s"""from { ${query} }
         |group by o_orderpriority
         |agg o_custkey.count_distinct as users""".stripMargin)
    users shouldBe 5
  }

  test("should drop rows with null keys when keeping k rows per key") {
    count("""from [[1, 'a'], [1, 'b'], [null, 'c'], [null, 'd']] as rv(k, v)
        |sample rendezvous(5) by k""".stripMargin) shouldBe 2
  }

  test("should return the same rows as DuckDB's hash()") {
    // The spec compares the engine with DuckDB, which runs the generated hash(seed, keys...) SQL
    val conformance = EngineConformance("spec/basic", Nil, _ => (), options)
    try
      val unit = conformance
        .units
        .find(_.sourceFile.fileName == "sample-rendezvous.wv")
        .getOrElse(fail("spec/basic/sample-rendezvous.wv not found"))
      val results = conformance.run(unit)
      results.size shouldBe 6
      results.foreach { r =>
        debug(s"${r.name}: ${r.message}")
        if !r.matched || r.message.startsWith("skipped") || r.message.contains("nondeterministic")
        then
          fail(s"${r.name}: ${r.message}")
      }
    finally
      conformance.close()
  }

end EngineRendezvousSampleTest

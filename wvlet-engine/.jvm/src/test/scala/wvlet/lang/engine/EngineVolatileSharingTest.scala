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
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * Two `random()` calls draw independently, so the plans that share work between equal subplans,
  * filters, or aggregate calls (`prefetch_scalar_subqueries`, `share_subquery_scans`,
  * `having_totals_from_groups`, `share_in_subquery_groups`) never treat them as the same
  */
class EngineVolatileSharingTest extends UniTest:
  private val conformance = EngineConformance.tpch(OptimizerOptions().withValidatePlans(true))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> wvlet.lang.model.plan.Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(q)

  /** The values of the query's single row */
  private def row(query: String): IndexedSeq[Any] =
    withQuery(query) { q =>
      val rows = conformance.engine.execute(q).rows.toList
      rows.size shouldBe 1
      rows.head
    }

  private def explain(query: String): String =
    withQuery(query) { q =>
      val plan = conformance.engine.explain(GenSQL.expand(q, summon[Context]))
      debug(plan)
      plan
    }

  /**
    * Whether the plan of `query` shares work (`shares` of its plan text), checked against the same
    * query with each `random()` replaced by a constant, which must share it
    */
  private def shares(query: String)(shares: String => Boolean): Boolean =
    shares(explain(query.replace("random()", "abs(0.5)"))) shouldBe true
    shares(explain(query))

  private def computesGroups(plan: String): Boolean = plan
    .linesIterator
    .exists(_.startsWith("Values"))

  test("evaluate scalar subqueries over independent random() filters on their own") {
    val sample = "{ from lineitem where random() < 0.5 select l_extendedprice.sum }"
    val r      = row(s"from nation where n_nationkey = 0 select a = ${sample}, b = ${sample}")
    (r(0) == r(1)) shouldBe false
  }

  test("evaluate scalar subqueries of independent random() sums on their own") {
    val sum = "{ from lineitem select random().sum }"
    val r   = row(s"from nation where n_nationkey = 0 select a = ${sum}, b = ${sum}")
    (r(0) == r(1)) shouldBe false
  }

  test("don't share a scan by a random() filter with a scalar subquery") {
    shares("""from lineitem
        |where random() < 0.5
        |  and l_quantity > { from lineitem where random() < 0.5 select l_quantity.avg }
        |select l_orderkey, l_quantity
        |""".stripMargin)(_.contains("CTE[__wv_scan")) shouldBe false
  }

  test("don't total a HAVING subquery over random() maxes from the groups") {
    shares("""from supplier
        |group by s_nationkey
        |where (s_acctbal * random()).max > {
        |  from supplier select (s_acctbal * random()).max * 0.9
        |}
        |select s_nationkey, m = (s_acctbal * random()).max
        |""".stripMargin)(computesGroups) shouldBe false
  }

  test("don't merge groups of random() maxes for an IN subquery") {
    shares("""from supplier, nation
        |where s_nationkey = n_nationkey and n_name in ({
        |  from nation as n, supplier
        |  where s_nationkey = n.n_nationkey
        |  group by n_name
        |  select n_name, rank() over (order by (s_acctbal * random()).max desc) as ranking
        |  where ranking <= 5
        |  select n_name
        |})
        |group by n_name
        |select n_name, m = (s_acctbal * random()).max
        |""".stripMargin)(computesGroups) shouldBe false
  }

end EngineVolatileSharingTest

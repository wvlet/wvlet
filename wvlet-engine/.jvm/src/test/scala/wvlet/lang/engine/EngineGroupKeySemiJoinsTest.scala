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
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * An aggregation that a join reads by a group key only aggregates the rows whose key a small
  * relation on the join's other side holds (`group_key_semi_joins`), with the rows DuckDB returns
  */
class EngineGroupKeySemiJoinsTest extends UniTest:
  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val disabled    = conformance
    .engine
    .withOptimizer(options.withDisabled(Optimization.GroupKeySemiJoins))

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

  /**
    * The query matches DuckDB with the rewrite on and off; the number of semi joins it adds to the
    * plan
    */
  private def added(query: String): Int =
    val on = conformance.compareQuery(query)
    on.matched shouldBe true
    on.message.startsWith("skipped") shouldBe false
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val off       = conformance.compare(query, GenSQL.expand(q, summon[Context]), target = disabled)
    off.matched shouldBe true
    semiJoins(explain(conformance.engine, query)) - semiJoins(explain(disabled, query))

  private def semiJoins(plan: String): Int = plan
    .linesIterator
    .count(l => l.contains("HashJoin[Semi]") || l.contains("HashJoin[RightSemi]"))

  // A per-part aggregation over all of lineitem
  private val perPart = "{ from lineitem group by l_partkey agg l_quantity.sum as q } as t"

  test("aggregate only the keys of the filtered side of a correlated scalar subquery") {
    val query =
      """from lineitem, part
        |where p_partkey = l_partkey
        |  and p_brand = 'Brand#23'
        |  and p_container = 'MED BOX'
        |  and l_quantity < {
        |    from lineitem as l2
        |    where l2.l_partkey = p_partkey
        |    select 0.2 * l2.l_quantity.avg
        |  }
        |select l_extendedprice.sum as total
        |""".stripMargin
    added(query) shouldBe 1
  }

  test("reduce an aggregation inner-joined with a filtered relation") {
    val query =
      s"""from part
         |join ${perPart} on p_partkey = t.l_partkey
         |where p_brand = 'Brand#23' and p_container = 'MED BOX'
         |select p_partkey, q
         |order by p_partkey
         |""".stripMargin
    added(query) shouldBe 1
  }

  test("trace the key across an inner join to its smaller side") {
    val query =
      s"""from lineitem
         |join part on lineitem.l_partkey = p_partkey
         |join ${perPart} on lineitem.l_partkey = t.l_partkey
         |where p_brand = 'Brand#23' and p_container = 'MED BOX'
         |select l_orderkey, l_linenumber, q
         |order by l_orderkey, l_linenumber
         |""".stripMargin
    added(query) shouldBe 1
  }

  test("keep the NULL keys of a left join's unmatched rows unmatched") {
    // p_partkey is NULL on the lineitem rows without a matching part
    val query =
      s"""from lineitem
         |left join {
         |  from part where p_brand = 'Brand#23' and p_container = 'MED BOX'
         |} as p on l_partkey = p.p_partkey
         |left join ${perPart} on p.p_partkey = t.l_partkey
         |select l_orderkey, l_linenumber, p.p_partkey, q
         |order by l_orderkey, l_linenumber
         |""".stripMargin
    added(query) shouldBe 1
  }

  test("not reduce an aggregation on the preserved side of a left join") {
    val query =
      s"""from ${perPart}
         |left join {
         |  from part where p_brand = 'Brand#23' and p_container = 'MED BOX'
         |} as p on t.l_partkey = p.p_partkey
         |select t.l_partkey, q, p.p_partkey
         |order by t.l_partkey
         |""".stripMargin
    added(query) shouldBe 0
  }

  test("not reduce the subquery of NOT IN") {
    val query =
      """from part
        |where p_brand = 'Brand#23'
        |  and p_partkey not in ({ from lineitem group by l_partkey select l_partkey })
        |select p_partkey
        |order by p_partkey
        |""".stripMargin
    added(query) shouldBe 0
  }

  test("not reduce when the other side is no smaller than the groups") {
    val query =
      s"""from part
         |join ${perPart} on p_partkey = t.l_partkey
         |select p_partkey, q
         |order by p_partkey
         |""".stripMargin
    added(query) shouldBe 0
  }

end EngineGroupKeySemiJoinsTest

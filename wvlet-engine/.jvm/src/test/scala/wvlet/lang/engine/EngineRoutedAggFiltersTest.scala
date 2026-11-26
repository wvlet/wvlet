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
import wvlet.lang.engine.exec.AggCall
import wvlet.lang.engine.exec.HashAggOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.PartitionedAggOp
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

/**
  * `routed_agg_filters` marks aggregate FILTERs to route by dictionary code, and returns the same
  * rows as without it, over a join build's string column as in TPC-DS q59
  */
class EngineRoutedAggFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val noRoute = on.withOptimizer(options.withDisabled(Optimization.RoutedAggFilters))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val from =
    """from [
      |  [1, 1, 10], [1, 2, 20], [1, 1, null], [1, 3, 40], [2, 2, 5], [2, 2, 6], [3, 4, 7],
      |  [3, 9, 8], [2, 1, 30], [3, 3, 9]
      |] as sales(g, dk, price), [
      |  [1, 'Sunday'], [2, 'Monday'], [3, null], [4, 'Saturday']
      |] as days(d_key, day)
      |where dk = d_key""".stripMargin

  private val aggQuery =
    s"""${from}
      |group by g
      |select g,
      |  sum(if day = 'Sunday' then price else null) as sun_sum,
      |  count(if day = 'Monday' then price else null) as mon_count,
      |  max(if day is null then price else null) as null_max,
      |  sum(if day = 'Saturday' then price else null) as sat_sum,
      |  sum(price) as total
      |order by g""".stripMargin

  private def run[A](query: String)(body: Relation => Context ?=> A): A =
    val unit           = CompilationUnit.fromWvletString(query)
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

  private def collect[A](op: Operator)(f: PartialFunction[Operator, Seq[A]]): Seq[A] =
    f.applyOrElse(op, _ => Nil) ++ op.children.flatMap(collect(_)(f))

  private def physical(e: WvletEngine, query: String): Operator =
    run(query)(q => e.physicalPlan(GenSQL.expand(q, summon[Context])))

  /** The aggregate calls of the physical plan's hash aggregations */
  private def aggCalls(e: WvletEngine): Seq[AggCall] =
    collect(physical(e, aggQuery)) {
      case h: HashAggOp =>
        h.aggs
      case p: PartitionedAggOp =>
        p.aggs
    }

  private def rows(e: WvletEngine, query: String): Seq[String] =
    run(query)(q => e.execute(q).rows.map(_.mkString(",")).toSeq)

  test("mark aggregate FILTERs only with routed_agg_filters") {
    val filtered = aggCalls(on).filter(_.filter.nonEmpty)
    filtered.nonEmpty shouldBe true
    filtered.forall(_.routedFilter) shouldBe true
    aggCalls(on).filter(_.filter.isEmpty).exists(_.routedFilter) shouldBe false
    aggCalls(noRoute).exists(_.routedFilter) shouldBe false
  }

  test("return the same aggregates with and without routed_agg_filters") {
    val r = rows(on, aggQuery)
    r shouldBe rows(noRoute, aggQuery)
    r shouldBe Seq("1,10,1,40,null,70", "2,30,2,null,null,41", "3,null,0,9,7,16")
  }

end EngineRoutedAggFiltersTest

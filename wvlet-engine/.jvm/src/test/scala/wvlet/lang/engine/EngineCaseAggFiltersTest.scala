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
import wvlet.lang.engine.exec.ProjectOp
import wvlet.lang.engine.expr.CaseWhen
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

/**
  * `case_agg_filters` turns aggregates of a single-WHEN CASE without ELSE into filtered aggregates,
  * and `const_case_select` marks CASE expressions with constant THEN and ELSE to pick per row. Both
  * return the same rows as without them
  */
class EngineCaseAggFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(
    options.withDisabled(Optimization.CaseAggFilters).withDisabled(Optimization.ConstCaseSelect)
  )

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val query =
    """from [
      |  [1, 'Sunday', 10], [1, 'Monday', 20], [1, 'Sunday', null], [1, null, 40],
      |  [2, 'Monday', 5], [2, 'Monday', 6], [3, null, null]
      |] as days(g, day, price)
      |group by g
      |select g,
      |  sum(if day = 'Sunday' then price else null) as sun_sum,
      |  count(if day = 'Sunday' then price else null) as sun_count,
      |  min(if day = 'Monday' then price else null) as mon_min,
      |  max(if day = 'Monday' then price else null) as mon_max,
      |  avg(if day = 'Monday' then price else null) as mon_avg,
      |  sum(if day = 'Sunday' then 1 else 0) as sun_days,
      |  sum(price) as total
      |order by g""".stripMargin

  private def run[A](body: Relation => Context ?=> A): A =
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

  private def physical(e: WvletEngine): Operator = run(q =>
    e.physicalPlan(GenSQL.expand(q, summon[Context]))
  )

  private def collect[A](op: Operator)(f: PartialFunction[Operator, Seq[A]]): Seq[A] =
    f.applyOrElse(op, _ => Nil) ++ op.children.flatMap(collect(_)(f))

  private val aggsOf: PartialFunction[Operator, Seq[AggCall]] = {
    case h: HashAggOp =>
      h.aggs
    case p: PartitionedAggOp =>
      p.aggs
  }

  /** The aggregate calls of the physical plan's hash aggregations */
  private def aggCalls(e: WvletEngine): Seq[AggCall] = collect(physical(e))(aggsOf)

  /** The CASE expressions anywhere in the physical plan's projections and aggregates */
  private def cases(e: WvletEngine): Seq[CaseWhen] =
    def in(x: VExpr): Seq[CaseWhen] =
      (
        x match
          case c: CaseWhen =>
            Seq(c)
          case _ =>
            Nil
      ) ++ x.children.flatMap(in)
    collect(physical(e)) {
      case p: ProjectOp =>
        p.exprs.flatMap(in)
      case op if aggsOf.isDefinedAt(op) =>
        aggsOf(op).flatMap(c => c.args.flatMap(in) ++ c.filter.toSeq.flatMap(in))
    }

  private def rows(e: WvletEngine): Seq[String] = run(q =>
    e.execute(q).rows.map(_.mkString(",")).toSeq
  )

  test("aggregate the THEN value with the condition as FILTER only with case_agg_filters") {
    // sum, count, min, max and avg (sum and count) of a CASE without ELSE carry a FILTER
    (aggCalls(on).count(_.filter.nonEmpty) >= 5) shouldBe true
    aggCalls(off).exists(_.filter.nonEmpty) shouldBe false
  }

  test("mark CASE expressions with constant THEN and ELSE only with const_case_select") {
    // The CASE of sum_days stays a CASE: its ELSE is a constant
    cases(on).filter(_.elseExpr.nonEmpty).map(_.selectConsts).distinct shouldBe Seq(true)
    cases(off).map(_.selectConsts).distinct shouldBe Seq(false)
  }

  test("return the same rows with and without case_agg_filters and const_case_select") {
    val r = rows(on)
    r shouldBe rows(off)
    r shouldBe
      Seq("1,10,1,20,20,20.0,2,70", "2,null,0,5,6,5.5,0,11", "3,null,0,null,null,null,0,null")
  }

end EngineCaseAggFiltersTest

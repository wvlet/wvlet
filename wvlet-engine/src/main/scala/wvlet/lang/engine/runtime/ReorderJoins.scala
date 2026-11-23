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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.JoinEdge
import wvlet.lang.engine.plan.JoinGraph
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.JoinSearch
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions

/**
  * Orders inner joins: replaces each [[RuntimePlan.InnerJoins]], from the bottom up, with a tree of
  * joins. Single-input conditions filter their inputs, equalities between inputs become hash join
  * keys, and [[JoinOrderOptimizer]] chooses the order and the build sides from [[Stats]]. The tree
  * outputs the same columns as the node, in its join order.
  *
  * The pass always runs, as lowering needs ordered joins; with `reorder_joins` disabled, the inputs
  * join in their written order.
  */
object ReorderJoins extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.ReorderJoins.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    p.withChildren(p.children.map(run(_, ctx))) match
      case j: InnerJoins =>
        if ctx.options.isEnabled(Optimization.ReorderJoins) then
          ctx.fired(name)
        order(j, ctx)
      case j @ Join(left, right, JoinKind.Left | JoinKind.Right | JoinKind.Full, lk, _, _, false)
          if lk.nonEmpty =>
        // An outer hash join builds on its left input when the right one is much larger, as
        // `Planner.joinOn` decides it
        if JoinOrderOptimizer.swapBuild(ctx.options, Stats.rows(right), Stats.rows(left)) then
          j.copy(buildLeft = true)
        else
          j
      case j @ Join(left, right, JoinKind.Semi | JoinKind.Anti, lk, _, _, buildLeft)
          if lk.nonEmpty && ctx.replanSemiJoinBuilds &&
            (readsComputedCTE(left) || readsComputedCTE(right)) =>
        // The binder chose the build side before the CTE was computed, building on the outer
        // input when the other is much larger (`Planner.correlatedJoin`, `plans.inJoin`): choose
        // it again with the CTE's exact rows. Either side gives the same rows, as NULL keys
        // never match
        val swap = JoinOrderOptimizer.swapBuild(ctx.options, Stats.rows(right), Stats.rows(left))
        if swap == buildLeft then
          j
        else
          j.copy(buildLeft = swap)
      case other =>
        other

  /** Whether p reads a shared CTE whose rows are known */
  private def readsComputedCTE(p: RuntimePlan): Boolean =
    p match
      case CTERef(cte, _) =>
        cte.rows.isDefined
      case _ =>
        p.children.exists(readsComputedCTE)

  /** The join tree of an [[RuntimePlan.InnerJoins]] node */
  def order(j: InnerJoins, ctx: OptimizerContext): RuntimePlan =
    val options = ctx.options
    val inputs  = j.inputs
    val owner   = inputs.indices.flatMap(i => inputs(i).output.map(_.id -> i)).toMap
    def refs(e: BExpr): Set[Int] = e.refs.map(owner)
    val condRefs                 = j.conds.map(c => (c, refs(c.cond)))
    val pushed                   = inputs
      .indices
      .map { i =>
        val local = condRefs.collect {
          case (c, r) if r == Set(i) =>
            c
        }
        // Pushed into the input as far as filters go, so that a second run of the passes over
        // the ordered tree (the binder optimizes some inputs before their parents) keeps it
        val p = PushDownFilters.filter(inputs(i), local, ctx)
        (p, Stats.rows(inputs(i)) * local.map(Stats.selectivity(inputs(i), _)).product)
      }
    val edges =
      condRefs
        .collect {
          case (c, r) if r.size != 1 =>
            val sides =
              c.cond.expr match
                case Compare(CmpOp.Eq, l, r) =>
                  val (lb, rb) = (BExpr(l), BExpr(r))
                  Some(JoinEdge.Sides(lb, refs(lb), rb, refs(rb)))
                case _ =>
                  None
            JoinEdge(c.cond, r, sides)
        }
        .toList
    val graph             = JoinGraph(pushed.map(_._1), pushed.map(_._2), edges)
    val (tree, remaining) = JoinOrderOptimizer(options).plan(graph, search)
    if remaining.isEmpty then
      tree
    else
      Filter(tree, and(remaining))

  end order

  private def and(conds: Seq[BExpr]): BExpr = BExpr(conds.map(_.expr).reduce(AndExpr(_, _)))

  private type Keys = (IndexedSeq[BExpr], IndexedSeq[BExpr])

  /** Join trees of runtime plans, estimated with [[Stats]] */
  private object search extends JoinSearch[RuntimePlan, BExpr, Keys]:
    override def keys(current: RuntimePlan, next: RuntimePlan, pairs: Seq[(BExpr, BExpr)]): Keys =
      (pairs.map(_._1).toIndexedSeq, pairs.map(_._2).toIndexedSeq)

    override def distinct(input: RuntimePlan, side: BExpr): Option[Double] = Stats.distinct(
      input,
      side.expr
    )

    override def outputRows(
        current: RuntimePlan,
        currentRows: Double,
        next: RuntimePlan,
        nextRows: Double,
        keys: Keys
    ): Double = CardinalityEstimator.joinOutputRows(
      currentRows,
      keys._1.map(k => Stats.distinct(current, k.expr)),
      nextRows,
      keys._2.map(k => Stats.distinct(next, k.expr))
    )

    override def join(
        current: RuntimePlan,
        next: RuntimePlan,
        keys: Keys,
        buildLeft: Boolean
    ): RuntimePlan = Join(current, next, JoinKind.Inner, keys._1, keys._2, None, buildLeft)

    override def cross(current: RuntimePlan, next: RuntimePlan): RuntimePlan = Join(
      current,
      next,
      JoinKind.Cross,
      IndexedSeq.empty,
      IndexedSeq.empty,
      None,
      buildLeft = false
    )

    override def filter(p: RuntimePlan, conds: List[BExpr]): RuntimePlan = Filter(p, and(conds))

  end search

end ReorderJoins

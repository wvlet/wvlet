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
import wvlet.lang.engine.plan.KeyCoverage
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.PType

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
        if JoinOrderOptimizer.swapBuild(
            ctx.options,
            Stats.outerBuildRows(right, right, ctx.options),
            Stats.outerBuildRows(left, right, ctx.options)
          )
        then
          j.copy(buildLeft = true)
        else
          j
      case j @ Join(left, right, JoinKind.Semi | JoinKind.Anti, lk, _, _, buildLeft)
          if lk.nonEmpty && ctx.replanSemiJoinBuilds &&
            (readsComputedCTE(left) || readsComputedCTE(right)) =>
        // The binder chose the build side before the CTE was computed, building on the outer
        // input when the other is larger (`Planner.correlatedJoin`, `plans.inJoin`): choose
        // it again with the CTE's exact rows. Either side gives the same rows, as NULL keys
        // never match
        val swap = JoinOrderOptimizer.swapSemiBuild(
          ctx.options,
          Stats.buildRows(right, ctx.options),
          Stats.buildRows(left, ctx.options),
          Stats.aggregates(right)
        )
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

  /**
    * The join tree of an [[RuntimePlan.InnerJoins]] node. With `commute_left_joins`, an input that
    * is a LEFT join whose right input no condition reads joins above the tree instead, over its
    * left input's place in the tree: `(A ⟕ B) ⋈ C` has the rows of `(A ⋈ C) ⟕ B` when the
    * conditions of the inner joins read no column of `B` (formal/WvletFormal/LeftJoinCommute). The
    * tree then outputs the columns of `B` last
    */
  def order(j: InnerJoins, ctx: OptimizerContext): RuntimePlan = orderWithRows(j, ctx)._1

  /** [[order]], with the join search's estimate of the tree's rows */
  private def orderWithRows(j: InnerJoins, ctx: OptimizerContext): (RuntimePlan, Double) =
    val i =
      // With reorder_joins off, the inputs join in written order
      if ctx.options.isEnabled(Optimization.CommuteLeftJoins) &&
        ctx.options.isEnabled(Optimization.ReorderJoins)
      then
        val read = j.conds.flatMap(_.cond.refs).toSet
        j.inputs
          .indexWhere {
            case l: Join =>
              l.kind == JoinKind.Left && l.right.output.forall(c => !read.contains(c.id))
            case _ =>
              false
          }
      else
        -1
    if i < 0 then
      orderTree(j, ctx)
    else
      val l = j.inputs(i).asInstanceOf[Join]
      ctx.fired(Optimization.CommuteLeftJoins.name)
      val (tree, rows) = orderWithRows(j.copy(inputs = j.inputs.updated(i, l.left)), ctx)
      // The LEFT join's build side, chosen again over the estimated rows of the joined tree
      // (Stats.rows reads only the probe side's source rows)
      val buildLeft =
        l.leftKeys.nonEmpty &&
          JoinOrderOptimizer.swapBuild(
            ctx.options,
            Stats.outerBuildRows(l.right, l.right, ctx.options),
            rows
          )
      // Every row of the tree is kept, with its matches
      (l.copy(left = tree, buildLeft = buildLeft), rows)

  end orderWithRows

  /** The join tree of an [[RuntimePlan.InnerJoins]] node without LEFT joins to move */
  private def orderTree(j: InnerJoins, ctx: OptimizerContext): (RuntimePlan, Double) =
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
        val p    = PushDownFilters.filter(inputs(i), local, ctx)
        val rows = Stats.filteredRows(inputs(i), local, ctx.options)
        // An aggregated input, estimated from the plan below its aggregation
        // (`aggregate_input_rows`), scaled by the same local conditions
        val estimate = Stats
          .aggregatedRows(inputs(i), ctx.options)
          .map(r => rows * r / math.max(1.0, Stats.rows(inputs(i))))
          .getOrElse(rows)
        if estimate < rows then
          ctx.fired(Optimization.AggregateInputRows.name)
        (p, estimate)
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
    val graph = JoinGraph(
      pushed.map(_._1),
      pushed.map(_._2),
      edges,
      if options.isEnabled(Optimization.BuildRowWidth) then
        pushed.map(x => rowBytes(x._1))
      else
        IndexedSeq.empty
    )
    val (tree, remaining, rows) = JoinOrderOptimizer(options).planWithRows(graph, Search(options))
    if remaining.isEmpty then
      (tree, rows)
    else
      (Filter(tree, and(remaining)), rows)

  end orderTree

  /**
    * Estimated bytes of a row of p: fixed widths for numeric and date columns, [[StringBytes]] for
    * a string
    */
  private[runtime] def rowBytes(p: RuntimePlan): Double = p
    .output
    .iterator
    .map(c =>
      c.ptype match
        case PType.Bool =>
          1.0
        case PType.Int32 | PType.Date =>
          4.0
        case PType.Str =>
          StringBytes
        case PType.Decimal(precision, _) if precision > 18 =>
          16.0
        case _ =>
          8.0
    )
    .sum

  /** The bytes [[rowBytes]] counts for a string column, as no statistics give string widths */
  private val StringBytes = 24.0

  private def and(conds: Seq[BExpr]): BExpr = BExpr(conds.map(_.expr).reduce(AndExpr(_, _)))

  private type Keys = (IndexedSeq[BExpr], IndexedSeq[BExpr])

  /** Join trees of runtime plans, estimated with [[Stats]] */
  private class Search(options: OptimizerOptions) extends JoinSearch[RuntimePlan, BExpr, Keys]:
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
    ): Double = Stats.joinRows(
      current,
      currentRows,
      keys._1.map(_.expr),
      next,
      nextRows,
      keys._2.map(_.expr),
      options
    )

    override def keyCoverage(input: RuntimePlan, sides: Seq[BExpr]): KeyCoverage =
      if options.isEnabled(Optimization.KeyJoinEstimates) then
        Stats.keyCoverage(input, sides.map(_.expr))
      else
        KeyCoverage.Unknown

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

  end Search

end ReorderJoins

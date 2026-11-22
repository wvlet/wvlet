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
package wvlet.lang.engine.plan

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.Schema
import wvlet.lang.model.expr.*

import scala.collection.mutable

/** Binds join conditions for [[JoinOrderOptimizer]], which has no binder of its own */
trait JoinBinding:
  /** Equi-join keys bound over each side and cast to common types */
  def joinKeys(
      left: Schema,
      right: Schema,
      pairs: Seq[(Expression, Expression)]
  ): (IndexedSeq[VExpr], IndexedSeq[VExpr])

  /** A condition bound over a schema */
  def bind(schema: Schema, e: Expression): VExpr

/**
  * Orders the inner joins of a [[JoinGraph]] and builds the join tree, choosing the build side of
  * each hash join
  */
class JoinOrderOptimizer(options: OptimizerOptions, binding: JoinBinding):
  import JoinOrderOptimizer.*

  private def enabled(o: Optimization): Boolean = options.isEnabled(o)

  /**
    * The cheapest left-deep join tree of multi-start greedy search: every input is tried as the
    * start (ties: the largest start, whose scan then drives the parallel pipeline). Returns the
    * tree and the conditions left to apply over it
    */
  def plan(g: JoinGraph): (Operator, List[Expression]) =
    val bySize =
      if enabled(Optimization.ReorderJoins) then
        g.inputs.indices.sortBy(i => -g.inputs(i).rows)
      else
        g.inputs.indices.take(1)
    val trees                = bySize.map(greedy(g, _))
    val best                 = trees.indices.minBy(k => (trees(k)._3, k))
    val (tree, remaining, _) = trees(best)
    (tree, remaining)

  /**
    * A left-deep join tree from `start`, joining next the connected input that gives the fewest
    * rows. Returns the tree, the conditions left to apply, and its cost: the estimated rows of
    * every intermediate result (C_out, which DuckDB's join order optimizer minimizes) plus the rows
    * of every hash table built
    */
  private def greedy(g: JoinGraph, start: Int): (Operator, List[Expression], Double) =
    val pushed            = g.inputs
    var remaining         = g.edges
    val joined            = mutable.Set.empty[Int]
    var current: Operator = pushed(start).op
    var currentRows       = pushed(start).rows
    var cost              = 0.0
    joined += start
    while joined.size < pushed.size do
      // Candidates connected by an equality condition
      val candidates = pushed
        .indices
        .filterNot(joined.contains)
        .map { i =>
          (i, remaining.filter(_.isEquiTo(joined, i)))
        }
      val connected = candidates.filter(_._2.nonEmpty)
      // As DuckDB's greedy join ordering does, join next the input giving the fewest rows
      val keyed = connected.map { (i, eqs) =>
        val right    = pushed(i).op
        val pairs    = eqs.map(_.sidesOf(joined))
        val (lk, rk) = binding.joinKeys(current.schema, right.schema, pairs)
        val out      = CardinalityEstimator.joinOutputRows(
          current,
          currentRows,
          lk,
          right,
          pushed(i).rows,
          rk
        )
        (i, eqs, lk, rk, out)
      }
      val (next, eqs) =
        if !enabled(Optimization.ReorderJoins) then
          // Written order: the first input connected by an equality, else the first one
          connected.headOption.getOrElse(candidates.head)
        else if keyed.nonEmpty then
          val (i, e, _, _, _) = keyed.minBy((i, _, _, _, out) => (out, pushed(i).rows))
          (i, e)
        else
          candidates.minBy(c => pushed(c._1).rows)
      val right    = pushed(next).op
      val combined = current.schema ++ right.schema
      if eqs.nonEmpty then
        val (_, _, lk, rk, out) = keyed.find(_._1 == next).get
        current =
          if swapBuild(options, pushed(next).rows, currentRows) then
            // Build on the joined rows, much fewer than the input to join (e.g., the returns
            // joined so far vs. a sales table), and probe with that input
            cost += currentRows
            swappedHashJoin(current, right, lk, rk, JoinKind.Inner, None, combined)
          else
            cost += pushed(next).rows
            HashJoinOp(current, right, lk, rk, JoinKind.Inner, None, combined)
        currentRows = out
      else
        current = NestedLoopJoinOp(current, right, None, JoinKind.Cross, combined)
        currentRows = currentRows * pushed(next).rows
      remaining = remaining.filterNot(eqs.contains)
      joined += next
      // Apply conditions whose inputs are now all joined (constant ones, reading no input, too)
      val ready = remaining.filter(_.refs.subsetOf(joined))
      if ready.nonEmpty then
        current = FilterOp(
          current,
          binding.bind(current.schema, ready.map(_.cond).reduce((a, b) => And(a, b, NoSpan)))
        )
        currentRows = math.max(
          1.0,
          currentRows * math.pow(CardinalityEstimator.DefaultSelectivity, ready.size)
        )
        remaining = remaining.filterNot(ready.contains)
      cost += currentRows
    end while
    (current, remaining.map(_.cond), cost)

  end greedy

end JoinOrderOptimizer

object JoinOrderOptimizer:
  /** How much larger the build input of a join must be before the inputs swap sides */
  val SwapBuildRatio: Double = 4.0

  /** Whether a join builds on the other input, as this one has much more rows than that */
  def swapBuild(options: OptimizerOptions, rows: Double, otherRows: Double): Boolean =
    options.isEnabled(Optimization.SwapBuildSide) && rows > SwapBuildRatio * otherRows

  /**
    * A hash join of left and right that probes with right and builds on left (`kind` is from
    * right's side), with the output columns in left ++ right order (`combined`)
    */
  def swappedHashJoin(
      left: Operator,
      right: Operator,
      lk: IndexedSeq[VExpr],
      rk: IndexedSeq[VExpr],
      kind: JoinKind,
      residual: Option[VExpr],
      combined: Schema
  ): Operator =
    val lw = left.schema.size
    val rw = right.schema.size
    // Position of each left ++ right column in right ++ left
    val toSwapped =
      Array.tabulate(lw + rw)(i =>
        if i < lw then
          rw + i
        else
          i - lw
      )
    val join = HashJoinOp(
      right,
      left,
      rk,
      lk,
      kind,
      residual.map(VExprs.remap(_, toSwapped)),
      right.schema ++ left.schema
    )
    ProjectOp(
      join,
      combined.fields.indices.map(i => ColRef(toSwapped(i), combined(i).ptype, combined(i).name)),
      combined
    )

  end swappedHashJoin

end JoinOrderOptimizer

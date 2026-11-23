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
  * Builds join trees for [[JoinOrderOptimizer]]: runtime-plan nodes paired with their operators,
  * for the runtime binder. Schemas and estimates come from the operator form
  */
trait JoinTreeBuilder[P]:
  /** The operator form of a plan */
  def op(p: P): Operator

  /**
    * A join of `left` and `right`, with the output columns in left ++ right order: a hash join on
    * the keys (building on `left` when `buildLeft` is set), or a nested loop join without keys
    */
  def join(
      left: P,
      right: P,
      kind: JoinKind,
      leftKeys: IndexedSeq[VExpr],
      rightKeys: IndexedSeq[VExpr],
      residual: Option[VExpr],
      buildLeft: Boolean
  ): P

  /** Rows of `p` for which a condition over its columns is true */
  def filter(p: P, cond: VExpr): P

/** Builds operator trees */
trait OperatorTreeBuilder extends JoinTreeBuilder[Operator]:
  override def op(p: Operator): Operator = p
  override def join(
      left: Operator,
      right: Operator,
      kind: JoinKind,
      leftKeys: IndexedSeq[VExpr],
      rightKeys: IndexedSeq[VExpr],
      residual: Option[VExpr],
      buildLeft: Boolean
  ): Operator = JoinOrderOptimizer.joinOp(
    left,
    right,
    kind,
    leftKeys,
    rightKeys,
    residual,
    buildLeft,
    left.schema ++ right.schema
  )

  override def filter(p: Operator, cond: VExpr): Operator = FilterOp(p, cond)

object OperatorTreeBuilder extends OperatorTreeBuilder

/**
  * What the join order search needs from a plan form `P` with conditions of form `C`: equi-join
  * keys of form `K`, their estimated output, and the join and filter nodes
  */
trait JoinSearch[P, C, K]:
  /** The keys of an equi-join of `current` and `next` on pairs of (current side, next side) */
  def keys(current: P, next: P, pairs: Seq[(C, C)]): K

  /** Distinct values of a side of an equality that reads only `input`, if known */
  def distinct(input: P, side: C): Option[Double]

  /** Estimated rows of an inner equi-join of `current` and `next` on `keys` */
  def outputRows(current: P, currentRows: Double, next: P, nextRows: Double, keys: K): Double

  /** An inner hash join on `keys`, building on `current` when `buildLeft` is set */
  def join(current: P, next: P, keys: K, buildLeft: Boolean): P

  /** A cross join */
  def cross(current: P, next: P): P

  /** Rows of `p` for which every condition is true */
  def filter(p: P, conds: List[C]): P

/**
  * Orders the inner joins of a [[JoinGraph]] and builds the join tree, choosing the build side of
  * each hash join
  */
class JoinOrderOptimizer(options: OptimizerOptions):
  import JoinOrderOptimizer.*

  private def enabled(o: Optimization): Boolean = options.isEnabled(o)

  /**
    * The cheapest join tree: of exact enumeration ([[JoinEnumerator]]) for graphs of at most
    * [[JoinEnumerator.MaxInputs]] inputs, else of multi-start greedy search. Returns the tree and
    * the conditions left to apply over it
    */
  def plan[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): (P, List[C]) =
    val exact =
      if enabled(Optimization.ReorderJoins) && enabled(Optimization.ExactJoinOrder) then
        JoinEnumerator(options).plan(g, search)
      else
        None
    exact.getOrElse(greedyPlan(g, search))

  /**
    * The cheapest left-deep join tree of multi-start greedy search: every input is tried as the
    * start (ties: the largest start, whose scan then drives the parallel pipeline). Returns the
    * tree and the conditions left to apply over it
    */
  def greedyPlan[P, C, K](g: JoinGraph[P, C], search: JoinSearch[P, C, K]): (P, List[C]) =
    val bySize =
      if enabled(Optimization.ReorderJoins) then
        g.inputs.indices.sortBy(i => -g.rows(i))
      else
        g.inputs.indices.take(1)
    val trees                = bySize.map(greedy(g, search, _))
    val best                 = trees.indices.minBy(k => (trees(k)._3, k))
    val (tree, remaining, _) = trees(best)
    (tree, remaining)

  /**
    * A left-deep join tree from `start`, joining next the connected input that gives the fewest
    * rows. Returns the tree, the conditions left to apply, and its cost: the estimated rows of
    * every intermediate result (C_out, which DuckDB's join order optimizer minimizes) plus the rows
    * of every hash table built
    */
  private def greedy[P, C, K](
      g: JoinGraph[P, C],
      search: JoinSearch[P, C, K],
      start: Int
  ): (P, List[C], Double) =
    val rows        = g.rows
    var remaining   = g.edges
    val joined      = mutable.Set.empty[Int]
    var current: P  = g.inputs(start)
    var currentRows = rows(start)
    var cost        = 0.0
    joined += start
    while joined.size < rows.size do
      // Candidates connected by an equality condition
      val candidates = rows
        .indices
        .filterNot(joined.contains)
        .map { i =>
          (i, remaining.filter(_.isEquiTo(joined, i)))
        }
      val connected = candidates.filter(_._2.nonEmpty)
      // As DuckDB's greedy join ordering does, join next the input giving the fewest rows
      val keyed = connected.map { (i, eqs) =>
        val keys = search.keys(current, g.inputs(i), eqs.map(_.sidesOf(joined)))
        (i, eqs, keys, search.outputRows(current, currentRows, g.inputs(i), rows(i), keys))
      }
      val (next, eqs) =
        if !enabled(Optimization.ReorderJoins) then
          // Written order: the first input connected by an equality, else the first one
          connected.headOption.getOrElse(candidates.head)
        else if keyed.nonEmpty then
          val (i, e, _, _) = keyed.minBy((i, _, _, out) => (out, rows(i)))
          (i, e)
        else
          candidates.minBy(c => rows(c._1))
      val right = g.inputs(next)
      if eqs.nonEmpty then
        val (_, _, keys, out) = keyed.find(_._1 == next).get
        // Build on the joined rows when they are much fewer than the input to join (e.g., the
        // returns joined so far vs. a sales table), and probe with that input
        val buildLeft = swapBuild(options, rows(next), currentRows)
        cost += (
          if buildLeft then
            currentRows
          else
            rows(next)
        )
        current = search.join(current, right, keys, buildLeft)
        currentRows = out
      else
        current = search.cross(current, right)
        currentRows = currentRows * rows(next)
      remaining = remaining.filterNot(eqs.contains)
      joined += next
      // Apply conditions whose inputs are now all joined (constant ones, reading no input, too)
      val ready = remaining.filter(_.refs.subsetOf(joined))
      if ready.nonEmpty then
        current = search.filter(current, ready.map(_.cond))
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
  /** Equi-join keys bound over each side */
  type Keys = (IndexedSeq[VExpr], IndexedSeq[VExpr])

  /**
    * The search over a plan form with operators (`builder.op`) and compiler expressions, which
    * `binding` binds over the operators' schemas
    */
  def expressionSearch[P](
      builder: JoinTreeBuilder[P],
      binding: JoinBinding
  ): JoinSearch[P, Expression, Keys] =
    new JoinSearch[P, Expression, Keys]:
      override def keys(current: P, next: P, pairs: Seq[(Expression, Expression)]): Keys = binding
        .joinKeys(builder.op(current).schema, builder.op(next).schema, pairs)

      override def distinct(input: P, side: Expression): Option[Double] =
        val op = builder.op(input)
        try
          CardinalityEstimator.keyDistinct(op, binding.bind(op.schema, side))
        catch
          case _: Exception =>
            None

      override def outputRows(
          current: P,
          currentRows: Double,
          next: P,
          nextRows: Double,
          keys: Keys
      ): Double = CardinalityEstimator.joinOutputRows(
        builder.op(current),
        currentRows,
        keys._1,
        builder.op(next),
        nextRows,
        keys._2
      )

      override def join(current: P, next: P, keys: Keys, buildLeft: Boolean): P = builder.join(
        current,
        next,
        JoinKind.Inner,
        keys._1,
        keys._2,
        None,
        buildLeft
      )

      override def cross(current: P, next: P): P = builder.join(
        current,
        next,
        JoinKind.Cross,
        IndexedSeq.empty,
        IndexedSeq.empty,
        None,
        buildLeft = false
      )

      override def filter(p: P, conds: List[Expression]): P = builder.filter(
        p,
        binding.bind(builder.op(p).schema, conds.reduce((a, b) => And(a, b, NoSpan)))
      )

  /** How much larger the build input of a join must be before the inputs swap sides */
  val SwapBuildRatio: Double = 4.0

  /** Whether a join builds on the other input, as this one has much more rows than that */
  def swapBuild(options: OptimizerOptions, rows: Double, otherRows: Double): Boolean =
    options.isEnabled(Optimization.SwapBuildSide) && rows > SwapBuildRatio * otherRows

  /** The position of each column of left ++ right (of widths lw, rw) in right ++ left */
  private def swappedPositions(lw: Int, rw: Int): Array[Int] =
    Array.tabulate(lw + rw)(i =>
      if i < lw then
        rw + i
      else
        i - lw
    )

  /** The kind of a join whose inputs swap sides */
  def swappedKind(kind: JoinKind): JoinKind =
    kind match
      case JoinKind.Left =>
        JoinKind.Right
      case JoinKind.Right =>
        JoinKind.Left
      case JoinKind.Semi =>
        JoinKind.RightSemi
      case JoinKind.Anti =>
        JoinKind.RightAnti
      case JoinKind.RightSemi =>
        JoinKind.Semi
      case JoinKind.RightAnti =>
        JoinKind.Anti
      case other =>
        other

  /**
    * The join operator of left and right, with the output columns in left ++ right order
    * (`combined`): a hash join on the keys, building on left when `buildLeft` is set, or a nested
    * loop join without keys
    */
  def joinOp(
      left: Operator,
      right: Operator,
      kind: JoinKind,
      lk: IndexedSeq[VExpr],
      rk: IndexedSeq[VExpr],
      residual: Option[VExpr],
      buildLeft: Boolean,
      combined: Schema,
      filters: Seq[RuntimeFilter] = Nil
  ): Operator =
    if kind == JoinKind.Semi || kind == JoinKind.Anti then
      semiJoinOp(left, right, lk, rk, kind, residual, buildLeft, filters)
    else if lk.isEmpty then
      NestedLoopJoinOp(left, right, residual, kind, combined)
    else if buildLeft then
      swappedHashJoin(left, right, lk, rk, swappedKind(kind), residual, combined, filters)
    else
      HashJoinOp(left, right, lk, rk, kind, residual, combined, runtimeFilters = filters)

  /**
    * A semi or anti join (`kind`) of left with right, outputting the columns of left; the residual
    * reads left ++ right. Without keys it is a nested loop join. A hash join builds on right, or,
    * with `buildLeft`, holds left's rows in the hash table while right is probed in parallel parts
    * marking the matched rows (RIGHT SEMI / RIGHT ANTI), and left's matched (or unmatched) rows are
    * emitted once at the end
    */
  def semiJoinOp(
      left: Operator,
      right: Operator,
      lk: IndexedSeq[VExpr],
      rk: IndexedSeq[VExpr],
      kind: JoinKind,
      residual: Option[VExpr],
      buildLeft: Boolean,
      filters: Seq[RuntimeFilter] = Nil
  ): Operator =
    if lk.isEmpty then
      NestedLoopJoinOp(left, right, residual, kind, left.schema)
    else if buildLeft then
      val toSwapped = swappedPositions(left.schema.size, right.schema.size)
      HashJoinOp(
        right,
        left,
        rk,
        lk,
        swappedKind(kind),
        residual.map(VExprs.remap(_, toSwapped)),
        left.schema,
        runtimeFilters = filters
      )
    else
      HashJoinOp(left, right, lk, rk, kind, residual, left.schema, runtimeFilters = filters)

  end semiJoinOp

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
      combined: Schema,
      filters: Seq[RuntimeFilter] = Nil
  ): Operator =
    val toSwapped = swappedPositions(left.schema.size, right.schema.size)
    val join      = HashJoinOp(
      right,
      left,
      rk,
      lk,
      kind,
      residual.map(VExprs.remap(_, toSwapped)),
      right.schema ++ left.schema,
      runtimeFilters = filters
    )
    ProjectOp(
      join,
      combined.fields.indices.map(i => ColRef(toSwapped(i), combined(i).ptype, combined(i).name)),
      combined
    )

  end swappedHashJoin

end JoinOrderOptimizer

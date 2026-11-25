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

import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.exec.GroupTable
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CastExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.InList
import wvlet.lang.engine.expr.Kernels
import wvlet.lang.engine.expr.LikeExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.PType

import scala.util.control.NonFatal

/**
  * Row and distinct-count estimates of [[RuntimePlan]] nodes, read from source statistics. They
  * follow [[CardinalityEstimator]] over the lowered operators rule for rule, so that join order
  * reads the same estimates on both forms, except for the prefix LIKE estimate of [[selectivity]]
  * (`prefix_like_selectivity`), which only the runtime plan reads
  */
object Stats:
  import RuntimePlan.*

  /**
    * Whether p aggregates or deduplicates rows, which [[rows]] estimates as their input's rows; a
    * shared CTE counts as its known rows
    */
  def aggregates(p: RuntimePlan): Boolean =
    p match
      case _: Aggregate | _: Distinct | _: SetOp =>
        true
      case _: CTERef =>
        false
      case other =>
        other.children.exists(aggregates)

  /** Estimated rows of p: source row counts, passed through every node but joins */
  def rows(p: RuntimePlan): Double =
    p match
      case Scan(source, _, _, _) =>
        source.estimatedRows.map(_.toDouble).getOrElse(1e6)
      case Values(columns, rows) if columns.isEmpty && rows.size == 1 =>
        // Lowered to a SingleRowOp, which CardinalityEstimator reads as a leaf without statistics
        1e6
      case Values(_, _) =>
        1e3
      case Join(left, right, kind, lk, _, _, buildLeft) =>
        if lk.isEmpty then
          // A nested loop join
          rows(left)
        else
          // About the rows of the side every output row comes from. A join building on the left
          // probes with the right input
          val (probe, build, k) =
            if buildLeft then
              (right, left, JoinOrderOptimizer.swappedKind(kind))
            else
              (left, right, kind)
          k match
            case JoinKind.Right | JoinKind.RightSemi | JoinKind.RightAnti =>
              rows(build)
            case JoinKind.Full =>
              math.max(rows(probe), rows(build))
            case _ =>
              rows(probe)
      case AsOfJoin(left, _, _, _, _, _, _, _) =>
        // As CardinalityEstimator does for AsOfJoinOp: every output row comes from a left row
        rows(left)
      case InnerJoins(inputs, _) =>
        rows(inputs.head)
      case CTERef(cte, _) =>
        // As for a scan of a shared result: its rows are unknown until it is computed
        cte.rows.map(_.toDouble).getOrElse(1e6)
      case Filter(child, _, _) =>
        rows(child)
      case Project(child, _, _) =>
        rows(child)
      case Sort(child, _, _) =>
        rows(child)
      case Limit(child, _, _) =>
        rows(child)
      case Aggregate(child, _, _, _) =>
        rows(child)
      case Union(inputs, _) =>
        // As CardinalityEstimator does over operators: the rows of the first input
        rows(inputs.head)
      case SetOp(left, _, _, _, _) =>
        rows(left)
      case Distinct(child) =>
        rows(child)
      case Sample(child, _) =>
        // As CardinalityEstimator does for the filter a sample lowers to
        rows(child)
      case Window(child, _, _) =>
        rows(child)
      case Unnest(child, _, _, _, _) =>
        // As CardinalityEstimator does for UnnestOp: the rows of its input
        rows(child)

  /** Distinct values of column `id` of p from source statistics, if traceable */
  def distinct(p: RuntimePlan, id: ColumnId): Option[Double] = sourceStat(
    p,
    id,
    _.estimatedDistinct(_),
    // Counted from the result once it is computed, as the CTE table's `estimatedDistinct`
    // returns it to CardinalityEstimator
    _.distinct(_)
  )

  /**
    * [min, max] of integer or date (days) column `id` of p from source statistics, if traceable
    * (not for shared CTE results)
    */
  def range(p: RuntimePlan, id: ColumnId): Option[(Long, Long)] = sourceStat(
    p,
    id,
    _.columnRange(_),
    (_, _) => None
  )

  /**
    * A statistic of the source column that column `id` of p reads: `scan` of a source column, `cte`
    * of a shared CTE result column, traced through filters, samples, plain projections and hash
    * joins
    */
  private def sourceStat[A](
      p: RuntimePlan,
      id: ColumnId,
      scan: (TableSource, String) => Option[A],
      cte: (SharedCTE, Int) => Option[A]
  ): Option[A] =
    def trace(p: RuntimePlan, id: ColumnId): Option[A] =
      p match
        case s @ Scan(source, columns, _, _) =>
          val i = columns.indexWhere(_.id == id)
          Option.when(i >= 0)(i).flatMap(i => scan(source, s.sourceColumnName(i)))
        case Filter(child, _, _) =>
          trace(child, id)
        case Sample(child, _) =>
          trace(child, id)
        case Project(child, exprs, columns) =>
          val i = columns.indexWhere(_.id == id)
          if i < 0 then
            None
          else
            exprs(i).expr match
              case c: ColRef =>
                trace(child, ColumnId(c.index))
              case _ =>
                None
        case Join(left, right, _, lk, _, _, _) =>
          // A hash join; a nested loop join doesn't trace its columns
          if lk.isEmpty then
            None
          else if left.output.exists(_.id == id) then
            trace(left, id)
          else
            trace(right, id)
        case CTERef(shared, columns) =>
          val i = columns.indexWhere(_.id == id)
          Option.when(i >= 0)(i).flatMap(cte(shared, _))
        case Values(_, _) | Sort(_, _, _) | Limit(_, _, _) | Aggregate(_, _, _, _) |
            InnerJoins(_, _) | Union(_, _) | SetOp(_, _, _, _, _) | Distinct(_) | Window(_, _, _) |
            Unnest(_, _, _, _, _) | AsOfJoin(_, _, _, _, _, _, _, _) =>
          // As CardinalityEstimator does over operators
          None
    trace(p, id)

  end sourceStat

  /**
    * Estimated groups of an aggregation of p by `keys`: the product of the keys' distinct values,
    * at most the rows of p. None when a key's distinct values are unknown
    */
  def groups(p: RuntimePlan, keys: Seq[VExpr]): Option[Double] = keys
    .foldLeft(Option(1.0)) { (acc, k) =>
      acc.flatMap(a => distinct(p, k).map(d => a * math.max(1.0, d)))
    }
    .map(math.min(_, rows(p)))

  /** Distinct values of an expression over p: a column, possibly cast */
  def distinct(p: RuntimePlan, e: VExpr): Option[Double] =
    e match
      case c: ColRef =>
        distinct(p, ColumnId(c.index))
      case c: CastExpr =>
        distinct(p, c.child)
      case _ =>
        None

  /**
    * Estimated rows of p passing all of `conds`: counted from the result for a read of a computed
    * shared CTE, and rows(p) times each condition's [[selectivity]] otherwise
    */
  def filteredRows(p: RuntimePlan, conds: Seq[JoinCondition], options: OptimizerOptions): Double =
    val counted =
      p match
        case CTERef(cte, columns) if conds.nonEmpty && cte.rows.isDefined =>
          val all = conds.map(_.cond.expr).reduce(AndExpr(_, _))
          cte.filteredRows(BExpr(cte.overBody(columns, all))).map(_.toDouble)
        case _ =>
          None
    counted.getOrElse(rows(p) * conjunctionSelectivity(p, conds, options))

  /**
    * Estimated rows of an aggregation, possibly under filters, projections, sorts and limits (an
    * aggregated subquery), from the plan below it: inner hash joins estimated as the join search
    * estimates them ([[CardinalityEstimator.joinOutputRows]]), filters by the [[selectivity]] of
    * each conjunct, and the aggregation as at most the product of its keys' distinct values. At
    * most [[rows]]. None when p is not an aggregation (`aggregate_input_rows`), as [[rows]] passes
    * the probe source's rows through joins, which makes an aggregated star join as large as its
    * fact table (TPC-DS q73, q34, q68)
    */
  def aggregatedRows(p: RuntimePlan, options: OptimizerOptions): Option[Double] =
    def aggregated(p: RuntimePlan): Boolean =
      p match
        case Aggregate(_, _, _, _) =>
          true
        case Filter(child, _, _) =>
          aggregated(child)
        case Project(child, _, _) =>
          aggregated(child)
        case Sort(child, _, _) =>
          aggregated(child)
        case Limit(child, _, _) =>
          aggregated(child)
        case _ =>
          false
    Option.when(options.isEnabled(Optimization.AggregateInputRows) && aggregated(p))(
      math.min(rows(p), treeRows(p, options))
    )

  /**
    * Estimated rows of p as [[aggregatedRows]] reads the plan below an aggregation: inner hash
    * joins as the join search estimates them, filters by their conjuncts' selectivity
    */
  private[runtime] def treeRows(p: RuntimePlan, options: OptimizerOptions): Double =
    p match
      case Filter(child, cond, literalCounts) =>
        treeRows(child, options) *
          conjunctionSelectivity(
            child,
            FactorOr.conjuncts(cond.expr).map(c => JoinCondition(BExpr(c), literalCounts)),
            options
          )
      case Project(child, _, _) =>
        treeRows(child, options)
      case Sort(child, _, limit) =>
        val r = treeRows(child, options)
        limit.map(l => math.min(l.toDouble, r)).getOrElse(r)
      case Limit(child, limit, _) =>
        math.min(limit.toDouble, treeRows(child, options))
      case Aggregate(child, keys, _, _) =>
        if keys.isEmpty then
          1.0
        else
          val r = treeRows(child, options)
          groups(child, keys.map(_.expr)).map(math.min(_, r)).getOrElse(r)
      case Join(left, right, JoinKind.Inner, lk, rk, _, _) if lk.nonEmpty =>
        CardinalityEstimator.joinOutputRows(
          treeRows(left, options),
          lk.map(k => distinct(left, k.expr)),
          treeRows(right, options),
          rk.map(k => distinct(right, k.expr))
        )
      case Join(left, _, JoinKind.Left | JoinKind.Semi | JoinKind.Anti, lk, _, _, _)
          if lk.nonEmpty =>
        // Every output row comes from a left row, as for rows
        treeRows(left, options)
      case other =>
        rows(other)

  /**
    * Estimated fraction of the rows of p passing all of `conds`: with `range_selectivity`, the
    * range comparisons of each integer or date column with known [min, max] as one interval
    * ([[RangeSelectivity.conjunction]]), and the [[selectivity]] of each other condition
    */
  def conjunctionSelectivity(
      p: RuntimePlan,
      conds: Seq[JoinCondition],
      options: OptimizerOptions
  ): Double =
    if options.isEnabled(Optimization.RangeSelectivity) then
      RangeSelectivity.conjunction(
        conds,
        _.cond.expr,
        id => range(p, ColumnId(id)),
        selectivity(p, _, options)
      )
    else
      conds.map(selectivity(p, _, options)).product

  /**
    * Estimated fraction of the rows of p passing a condition: 1 / ndv for `x = literal` (n / ndv
    * for `x in (n literals)`), as in DuckDB, [[PrefixLike.Selectivity]] for `x like 'p%'` (when
    * `prefix_like_selectivity` is on), and [[CardinalityEstimator.DefaultSelectivity]] otherwise
    */
  def selectivity(p: RuntimePlan, c: JoinCondition, options: OptimizerOptions): Double =
    def compared(e: VExpr): Option[VExpr] =
      e match
        case Compare(CmpOp.Eq, Const(_, _), x) =>
          Some(x)
        case Compare(CmpOp.Eq, x, _) =>
          Some(x)
        case in: InList if !in.negate =>
          Some(in.child)
        case _ =>
          None
    c.literals
      .flatMap { n =>
        compared(c.cond.expr).flatMap(distinct(p, _)).filter(_ >= 1).map(d => math.min(1.0, n / d))
      }
      .getOrElse {
        c.cond.expr match
          case l: LikeExpr
              if options.isEnabled(Optimization.PrefixLikeSelectivity) && PrefixLike.isPrefix(l) =>
            PrefixLike.Selectivity
          case _ =>
            CardinalityEstimator.DefaultSelectivity
      }

  /**
    * The distinct non-NULL values of column k of a computed result, grouped as DISTINCT groups them
    * (where NULL is a group of its own)
    */
  def resultDistinct(batches: Seq[Batch], k: Int, ptype: PType): Long =
    val groups  = GroupTable(IndexedSeq(ptype))
    var hasNull = false
    batches.foreach { b =>
      if b.size > 0 then
        val v = b.column(k)
        groups.groupIds(Array(v), b.size)
        if !hasNull && v.nulls != null then
          hasNull = (0 until b.size).exists(v.isNull)
    }
    groups.numGroups - (
      if hasNull then
        1
      else
        0
    )

  /**
    * The rows of a computed result for which `cond` (over the batch columns) is TRUE, as FilterOp
    * keeps them: NULL and FALSE drop the row. None if the condition fails to evaluate, e.g. a cast
    * failing on a row that a join would have dropped first
    */
  def resultRows(batches: Seq[Batch], cond: VExpr): Option[Long] =
    try
      Some(
        batches
          .iterator
          .filter(_.size > 0)
          .map(b => Kernels.trueMask(cond.eval(b)).iterator.take(b.size).count(identity).toLong)
          .sum
      )
    catch
      case NonFatal(_) =>
        None

end Stats

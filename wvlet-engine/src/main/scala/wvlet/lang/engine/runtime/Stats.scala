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
import wvlet.lang.engine.catalog.ValueCounts
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
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.KeyCoverage
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.ScanFilters
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.IntVec
import wvlet.lang.engine.vector.LongVec
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
  def distinct(p: RuntimePlan, id: ColumnId): Option[Double] = distinct(p, id, false)

  /**
    * [[distinct]] of column `id`; with `throughGroups`, a grouping key column of an aggregation
    * traces to the column it groups by, whose distinct values it keeps
    */
  def distinct(p: RuntimePlan, id: ColumnId, throughGroups: Boolean): Option[Double] = sourceStat(
    p,
    id,
    (s, i) => s.source.estimatedDistinct(s.sourceColumnName(i)),
    // Counted from the result once it is computed, as the CTE table's `estimatedDistinct`
    // returns it to CardinalityEstimator
    _.distinct(_),
    throughGroups
  )

  /**
    * [min, max] of integer or date (days) column `id` of p from source statistics, if traceable
    * (not for shared CTE results)
    */
  def range(p: RuntimePlan, id: ColumnId): Option[(Long, Long)] = sourceStat(
    p,
    id,
    (s, i) => s.source.columnRange(s.sourceColumnName(i)),
    (_, _) => None
  )

  /**
    * A statistic of the source column that column `id` of p reads: `scan` of a scan's column, `cte`
    * of a shared CTE result column, traced through filters, samples, plain projections and hash
    * joins
    */
  private def sourceStat[A](
      p: RuntimePlan,
      id: ColumnId,
      scan: (Scan, Int) => Option[A],
      cte: (SharedCTE, Int) => Option[A],
      throughGroups: Boolean = false
  ): Option[A] =
    def trace(p: RuntimePlan, id: ColumnId): Option[A] =
      p match
        case s @ Scan(source, columns, _, _) =>
          val i = columns.indexWhere(_.id == id)
          Option.when(i >= 0)(i).flatMap(scan(s, _))
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
        case Aggregate(child, keys, _, columns) if throughGroups =>
          // The output columns are the keys, then the calls
          val i = columns.indexWhere(_.id == id)
          if i >= 0 && i < keys.size then
            keys(i).expr match
              case c: ColRef =>
                trace(child, ColumnId(c.index))
              case _ =>
                None
          else
            None
        case Values(_, _) | Sort(_, _, _) | Limit(_, _, _) | Aggregate(_, _, _, _) |
            InnerJoins(_, _) | Union(_, _) | SetOp(_, _, _, _, _) | Distinct(_) | Window(_, _, _) |
            Unnest(_, _, _, _, _) | AsOfJoin(_, _, _, _, _, _, _, _) =>
          // As CardinalityEstimator does over operators
          None
    trace(p, id)

  end sourceStat

  /**
    * What the declared unique keys say about `keys` over p (`key_join_estimates`): columns traced,
    * as [[distinct]] traces them, to one scan of a table that declares unique keys either cover one
    * of them or none. A key that is no column, or columns of several scans, are
    * [[KeyCoverage.Unknown]]
    */
  def keyCoverage(p: RuntimePlan, keys: Seq[VExpr]): KeyCoverage =
    val traced = keys.map {
      case c: ColRef =>
        sourceStat(p, ColumnId(c.index), (s, i) => Some((s, s.sourceColumnName(i))), (_, _) => None)
      case _ =>
        None
    }
    if traced.isEmpty || traced.exists(_.isEmpty) then
      KeyCoverage.Unknown
    else
      val scan     = traced.head.get._1
      val declared = scan.source.uniqueKeys
      if declared.isEmpty || traced.exists(!_.get._1.eq(scan)) then
        KeyCoverage.Unknown
      else
        val names = traced.map(_.get._2.toLowerCase).toSet
        if declared.exists(k => k.nonEmpty && k.forall(c => names.contains(c.toLowerCase))) then
          scan
            .source
            .estimatedRows
            .map(r => KeyCoverage.Unique(r.toDouble))
            .getOrElse(KeyCoverage.Unknown)
        else
          KeyCoverage.NotUnique

  /**
    * Estimated rows of an inner equi-join of `left` and `right` on `leftKeys` = `rightKeys`
    * ([[CardinalityEstimator.joinOutputRows]]); with `key_join_estimates`, keys of several pairs
    * are estimated from the tables' declared unique keys
    * ([[CardinalityEstimator.compositeDivisor]])
    */
  def joinRows(
      left: RuntimePlan,
      leftRows: Double,
      leftKeys: IndexedSeq[VExpr],
      right: RuntimePlan,
      rightRows: Double,
      rightKeys: IndexedSeq[VExpr],
      options: OptimizerOptions
  ): Double =
    val ld        = leftKeys.map(distinct(left, _))
    val rd        = rightKeys.map(distinct(right, _))
    val composite =
      if leftKeys.size > 1 && options.isEnabled(Optimization.KeyJoinEstimates) then
        CardinalityEstimator.compositeDivisor(
          leftRows,
          ld,
          rightRows,
          rd,
          keyCoverage(left, leftKeys),
          keyCoverage(right, rightKeys)
        )
      else
        None
    composite match
      case Some(d) =>
        CardinalityEstimator.joinOutputRows(
          leftRows,
          IndexedSeq.empty,
          rightRows,
          IndexedSeq.empty,
          Seq(d)
        )
      case None =>
        CardinalityEstimator.joinOutputRows(leftRows, ld, rightRows, rd)

  end joinRows

  /**
    * Estimated groups of an aggregation of p by `keys`: the product of the keys' distinct values,
    * at most the rows of p. None when a key's distinct values are unknown. With `throughGroups`,
    * keys trace through the grouping keys of aggregations below
    */
  def groups(p: RuntimePlan, keys: Seq[VExpr], throughGroups: Boolean = false): Option[Double] =
    combinations(p, keys, throughGroups).map(math.min(_, rows(p)))

  /** The product of the distinct values of `keys` over p. None when one is unknown */
  def combinations(
      p: RuntimePlan,
      keys: Seq[VExpr],
      throughGroups: Boolean = false
  ): Option[Double] =
    keys.foldLeft(Option(1.0)) { (acc, k) =>
      acc.flatMap(a => distinct(p, k, throughGroups).map(d => a * math.max(1.0, d)))
    }

  /** Distinct values of an expression over p: a column, possibly cast */
  def distinct(p: RuntimePlan, e: VExpr): Option[Double] = distinct(p, e, false)

  /** [[distinct]] of an expression, through grouping keys with `throughGroups` */
  def distinct(p: RuntimePlan, e: VExpr, throughGroups: Boolean): Option[Double] =
    e match
      case c: ColRef =>
        distinct(p, ColumnId(c.index), throughGroups)
      case c: CastExpr =>
        distinct(p, c.child, throughGroups)
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
    * Estimated rows of p for choosing a join's build side outside the join search: [[treeRows]]
    * with `join_tree_build_estimates`, and [[rows]] (the probe source's rows) otherwise
    */
  def buildRows(p: RuntimePlan, options: OptimizerOptions): Double =
    if options.isEnabled(Optimization.JoinTreeBuildEstimates) then
      treeRows(p, options, build = true)
    else
      rows(p)

  /**
    * Whether a semi / anti join builds on its left (outer) input, from the [[buildRows]] of each
    * side: with `join_tree_build_estimates` by any margin ([[JoinOrderOptimizer.swapSemiBuild]]),
    * otherwise by [[JoinOrderOptimizer.SwapBuildRatio]]
    */
  def swapsSemiBuild(left: RuntimePlan, right: RuntimePlan, options: OptimizerOptions): Boolean =
    if options.isEnabled(Optimization.JoinTreeBuildEstimates) then
      JoinOrderOptimizer.swapSemiBuild(
        options,
        buildRows(right, options),
        buildRows(left, options),
        aggregates(right)
      )
    else
      JoinOrderOptimizer.swapBuild(options, rows(right), rows(left))

  /**
    * Estimated rows of p for choosing the build side of an outer join whose right input is `right`:
    * [[buildRows]], except [[rows]] when `right` aggregates, as a RIGHT join probing with an
    * aggregation's output gains no scan to filter and holds back the rows above it until its
    * unmatched build rows are emitted (TPC-DS q10's EXISTS marks)
    */
  def outerBuildRows(p: RuntimePlan, right: RuntimePlan, options: OptimizerOptions): Double =
    if aggregates(right) then
      rows(p)
    else
      buildRows(p, options)

  /**
    * Estimated rows of p as [[aggregatedRows]] reads the plan below an aggregation: inner hash
    * joins as the join search estimates them, filters by their conjuncts' selectivity. For a build
    * side (`build`, with `join_tree_build_estimates`), plan-time rows count exactly, a union as the
    * sum of its inputs, and RIGHT and FULL joins by the side their rows come from
    */
  private[runtime] def treeRows(
      p: RuntimePlan,
      options: OptimizerOptions,
      build: Boolean = false
  ): Double =
    lazy val tree = build && options.isEnabled(Optimization.JoinTreeBuildEstimates)
    p match
      case Values(columns, batch) if tree && !(columns.isEmpty && batch.size == 1) =>
        math.max(1.0, batch.size.toDouble)
      case Union(inputs, _) if tree =>
        inputs.map(treeRows(_, options, build)).sum
      case Join(_, right, JoinKind.Right, lk, _, _, _) if tree && lk.nonEmpty =>
        treeRows(right, options, build)
      case Join(left, right, JoinKind.Full, lk, _, _, _) if tree && lk.nonEmpty =>
        math.max(treeRows(left, options, build), treeRows(right, options, build))
      case Filter(child, cond, literalCounts) =>
        treeRows(child, options, build) *
          conjunctionSelectivity(
            child,
            FactorOr.conjuncts(cond.expr).map(c => JoinCondition(BExpr(c), literalCounts)),
            options
          )
      case Project(child, _, _) =>
        treeRows(child, options, build)
      case Sort(child, _, limit) =>
        val r = treeRows(child, options, build)
        limit.map(l => math.min(l.toDouble, r)).getOrElse(r)
      case Limit(child, limit, _) =>
        math.min(limit.toDouble, treeRows(child, options, build))
      case Aggregate(child, keys, _, _) =>
        if keys.isEmpty then
          1.0
        else
          val r = treeRows(child, options, build)
          // An aggregation of an aggregation (TPC-DS q65's average by store of the sums by store
          // and item) has at most its keys' distinct values below the inner one
          val throughGroups = options.isEnabled(Optimization.NestedGroupEstimates)
          if options.isEnabled(Optimization.ClusteredGroupEstimates) then
            combinations(child, keys.map(_.expr), throughGroups)
              .map(c =>
                ClusteredGroups.groups(child, keys.map(_.expr), math.min(c, rows(child)), r)
              )
              .getOrElse(r)
          else
            groups(child, keys.map(_.expr), throughGroups).map(math.min(_, r)).getOrElse(r)
      case Join(left, right, JoinKind.Inner, lk, rk, _, _) if lk.nonEmpty =>
        joinRows(
          left,
          treeRows(left, options, build),
          lk.map(_.expr),
          right,
          treeRows(right, options, build),
          rk.map(_.expr),
          options
        )
      case Join(left, _, JoinKind.Left | JoinKind.Semi | JoinKind.Anti, lk, _, _, _)
          if lk.nonEmpty =>
        // Every output row comes from a left row, as for rows
        treeRows(left, options, build)
      case other =>
        rows(other)
    end match

  end treeRows

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
    * The sampled values ([[wvlet.lang.engine.catalog.ValueCounts]]) of the source string column
    * that column `id` of p reads, if traceable (not for shared CTE results)
    */
  def valueCounts(p: RuntimePlan, id: ColumnId): Option[ValueCounts] = sourceStat(
    p,
    id,
    (s, i) => s.source.valueCounts(s.sourceColumnName(i)),
    (_, _) => None
  )

  /**
    * Estimated fraction of the rows of p passing a condition: with `string_value_selectivity`, a
    * condition over one string column from its source's sampled values
    * ([[ScanFilters.valueSelectivity]]); otherwise 1 / ndv for `x = literal` (n / ndv for
    * `x in (n literals)`), as in DuckDB, [[PrefixLike.Selectivity]] for `x like 'p%'` (when
    * `prefix_like_selectivity` is on), and [[CardinalityEstimator.DefaultSelectivity]] otherwise
    */
  def selectivity(p: RuntimePlan, c: JoinCondition, options: OptimizerOptions): Double =
    val sampled =
      if options.isEnabled(Optimization.StringValueSelectivity) then
        ScanFilters.valueSelectivity(c.cond.expr, i => valueCounts(p, ColumnId(i)))
      else
        None
    sampled.getOrElse(literalSelectivity(p, c, options))

  private def literalSelectivity(
      p: RuntimePlan,
      c: JoinCondition,
      options: OptimizerOptions
  ): Double =
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
          case o: OrExpr if options.isEnabled(Optimization.OrEqualsSelectivity) =>
            orEqualsSelectivity(p, o).getOrElse(CardinalityEstimator.DefaultSelectivity)
          case _ =>
            CardinalityEstimator.DefaultSelectivity
      }

  /**
    * n / ndv for an `or` of n comparisons of one expression with literals (`x = a or x = b`, also
    * `x in (...)` disjuncts), as for `x in (a, b)`. TPC-DS q75's readers push `d_year = 2002 or
    * d_year = 2001` into its CTE body. None for other `or`s, or when ndv is unknown
    */
  private def orEqualsSelectivity(p: RuntimePlan, o: OrExpr): Option[Double] =
    def literals(e: VExpr): Option[(VExpr, Int)] =
      e match
        case Compare(CmpOp.Eq, x, Const(v, _)) if v != null =>
          Some(x -> 1)
        case Compare(CmpOp.Eq, Const(v, _), x) if v != null =>
          Some(x -> 1)
        case in: InList if !in.negate =>
          Some(in.child -> in.values.size)
        case _ =>
          None
    def disjuncts(e: VExpr): List[VExpr] =
      e match
        case OrExpr(l, r) =>
          disjuncts(l) ++ disjuncts(r)
        case other =>
          List(other)
    val parts = disjuncts(o).map(literals)
    if parts.exists(_.isEmpty) then
      None
    else
      val xs = parts.flatten
      val x  = xs.head._1
      if xs.exists(_._1 != x) then
        None
      else
        distinct(p, x).filter(_ >= 1).map(d => math.min(1.0, xs.map(_._2).sum / d))

  /**
    * The distinct non-NULL values of column k of a computed result, grouped as DISTINCT groups them
    * (where NULL is a group of its own)
    */
  def resultDistinct(batches: Seq[Batch], k: Int, ptype: PType, primitive: Boolean = true): Long =
    val live = batches.filter(_.size > 0)
    if primitive && live.forall(b => b.column(k).isInstanceOf[LongVec | IntVec]) then
      primitiveDistinct(live, k)
    else
      groupDistinct(live, k, ptype)

  /** The distinct non-NULL values of column k by a [[GroupTable]] (DISTINCT's grouping) */
  private def groupDistinct(batches: Seq[Batch], k: Int, ptype: PType): Long =
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
    * The distinct non-NULL values of integer column k (every batch's column a [[LongVec]] or
    * [[IntVec]]), counted in an open-addressing set of longs sized for every row: a GroupTable
    * assigns a group id per row and grows as it goes, which took about 15 ms per plan for TPC-DS
    * q01's 540K `ctr_customer_sk` at SF10 (`primitive_result_distinct`)
    */
  private def primitiveDistinct(batches: Seq[Batch], k: Int): Long =
    val rows = batches.map(_.size.toLong).sum
    // At most half full; 0 marks an empty slot, so a 0 value is counted on the side
    val bits    = math.max(4, 64 - java.lang.Long.numberOfLeadingZeros(math.max(1L, rows * 2 - 1)))
    val slots   = Array.ofDim[Long](1 << bits)
    val mask    = slots.length - 1
    var count   = 0L
    var hasZero = false
    inline def add(v: Long): Unit =
      if v == 0L then
        hasZero = true
      else
        var i = ((v * 0x9e3779b97f4a7c15L) >>> (64 - bits)).toInt
        while slots(i) != 0L && slots(i) != v do
          i = (i + 1) & mask
        if slots(i) == 0L then
          slots(i) = v
          count += 1
    batches.foreach { b =>
      val n = b.size
      b.column(k) match
        case v: LongVec =>
          val values = v.values
          val nulls  = v.nulls
          var i      = 0
          while i < n do
            if nulls == null || !nulls(i) then
              add(values(i))
            i += 1
        case v: IntVec =>
          val values = v.values
          val nulls  = v.nulls
          var i      = 0
          while i < n do
            if nulls == null || !nulls(i) then
              add(values(i).toLong)
            i += 1
        case other =>
          throw IllegalStateException(s"Not an integer column: ${other}")
    }
    count + (
      if hasZero then
        1
      else
        0
    )

  end primitiveDistinct

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

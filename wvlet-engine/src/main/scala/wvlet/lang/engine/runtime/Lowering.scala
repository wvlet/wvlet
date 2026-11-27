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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.DeferredNullChecks
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.ScanFilters
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Schema
import wvlet.uni.cloud.parquet.ParquetFilter

import scala.collection.mutable

/**
  * Turns a [[RuntimePlan]] into physical operators: column ids become input positions. The physical
  * passes over operators (column pruning, parallelism) run afterwards, as for the recursive
  * planner.
  */
object Lowering:
  import RuntimePlan.*

  /**
    * Lower `p`. With `joinFilters`, hash joins whose unmatched probe rows are dropped give the
    * Parquet scans on their probe side the range of their build keys ([[RuntimeFilter]]), and with
    * `rowJoinFilters` a per-row test of their keys too ([[wvlet.lang.engine.exec.RowKeyFilter]]).
    * With `buildSideJoinFilters`, the filters also reach scans on the build sides of joins below
    * the probe side that keep the rows of that side ([[scanColumns]]). With `inSet`, p's consumer
    * reads only its distinct rows ([[ReduceSemiJoins.childContexts]]). With `footerCache`, Parquet
    * scans reuse cached footers ([[wvlet.lang.engine.catalog.ParquetFooterCache]]). With
    * `windowTopN`, a filter bounding the rank of a window keeps only the top rows of each partition
    * ([[WindowOp.topNOf]]). With `windowKernels`, running window aggregates fold primitive values
    * ([[WindowOp.runningPrimitive]]). With `windowSortKernels`, window partitions ordered by one
    * integer or date key sort packed longs ([[WindowOp.sortByPrimitiveKey]]). With
    * `scanRowFilters`, the conjuncts of a filter over a scan that its source evaluates exactly move
    * into the scan's row filter ([[ScanOp.rowFilter]]); with `selectiveRowFilters`, only when they
    * are estimated to keep at most [[ScanFilters.MaxRowFilterSelectivity]] of its rows. With
    * `dictionaryCodes`, Parquet strings decoded from a dictionary carry their codes
    * ([[ScanOp.dictionaryCodes]]); with `dictionaryEntries`, such rows are read as their dictionary
    * entries only ([[ScanOp.dictionaryEntries]]); with `lazyStrings`, such vectors hold only their
    * codes and entries ([[ScanOp.lazyStrings]]). With `maskedCaseWhen`, a CASE of one WHEN without
    * ELSE masks its THEN vector ([[MaskCaseWhen]]). With `windowSharePartitions`, window calls with
    * the same PARTITION BY and ORDER BY are partitioned and sorted once ([[WindowOp.callGroups]]).
    * With `constCaseSelect`, a CASE of one WHEN with constant THEN and ELSE picks one per row
    * ([[MaskCaseWhen]]). With `caseAggFilters`, an aggregate of a CASE of one WHEN without ELSE
    * aggregates its THEN value with the condition as FILTER ([[CaseAggFilters]]). With
    * `dictionaryAggFilters`, aggregate FILTERs over one string column run once per dictionary entry
    * ([[AggCall.dictionaryFilter]]); with `routedAggFilters`, several such FILTERs over one column
    * route each row by its code in one pass ([[AggCall.routedFilter]]). With `substringInLists`,
    * `substring(s, k, n) IN (strings)` compares string regions with the values
    * ([[SubstringInLists]]). With `windowTopNStream`, a window under a rank bound keeps each input
    * chunk's top rows as it reads them ([[wvlet.lang.engine.exec.WindowTopN.keepTop]]). With
    * `valueSelectivity`, the row-filter gate estimates string conjuncts from the source's sampled
    * values ([[ScanFilters.valueSelectivity]]). With `columnImpliedRowFilters`, a row filter also
    * takes the per-column filters implied by a multi-column `or` conjunct
    * ([[ScanFilters.columnImplied]]). With `deferredNullChecks`, IS NULL of a column not gathered
    * yet reads the column's positions and source nulls ([[DeferredNullChecks]]). With
    * `fusedDecimalArith`, decimal `x * (c ± y)` computes in one loop ([[FuseDecimalArith]])
    */
  def apply(
      p: RuntimePlan,
      joinFilters: Boolean = false,
      rowJoinFilters: Boolean = false,
      buildSideJoinFilters: Boolean = false,
      presizeAggs: Boolean = false,
      inSet: Boolean = false,
      accelerator: Option[PlanAccelerator] = None,
      footerCache: Boolean = false,
      windowTopN: Boolean = false,
      windowKernels: Boolean = false,
      commonSubexpressions: Boolean = false,
      foldConstants: Boolean = false,
      windowSortKernels: Boolean = false,
      scanRowFilters: Boolean = false,
      dictionaryCodes: Boolean = false,
      dictionaryEntries: Boolean = false,
      lazyStrings: Boolean = false,
      selectiveRowFilters: Boolean = false,
      maxRowFilterSelectivity: Double = ScanFilters.MaxRowFilterSelectivity,
      maskedCaseWhen: Boolean = false,
      windowSharePartitions: Boolean = false,
      constCaseSelect: Boolean = false,
      caseAggFilters: Boolean = false,
      dictionaryAggFilters: Boolean = false,
      routedAggFilters: Boolean = false,
      substringInLists: Boolean = false,
      windowTopNStream: Boolean = false,
      valueSelectivity: Boolean = false,
      columnImpliedRowFilters: Boolean = false,
      deferredNullChecks: Boolean = false,
      fusedDecimalArith: Boolean = false,
      semiAntiOneProbe: Boolean = false,
      cseHashedKeys: Boolean = false
  ): Operator =
    val l = Lower(
      joinFilters,
      rowJoinFilters,
      buildSideJoinFilters,
      presizeAggs,
      accelerator,
      footerCache,
      windowTopN,
      windowKernels,
      commonSubexpressions,
      foldConstants,
      windowSortKernels,
      scanRowFilters,
      dictionaryCodes,
      dictionaryEntries,
      lazyStrings,
      selectiveRowFilters,
      maxRowFilterSelectivity,
      maskedCaseWhen,
      windowSharePartitions,
      constCaseSelect,
      caseAggFilters,
      dictionaryAggFilters,
      routedAggFilters,
      substringInLists,
      windowTopNStream,
      valueSelectivity,
      columnImpliedRowFilters,
      deferredNullChecks,
      fusedDecimalArith,
      semiAntiOneProbe,
      cseHashedKeys
    )
    if inSet then
      l.markInSet(p)
    l.lower(p)

  end apply

  /**
    * The group table size of an aggregation of p by `keys`, from [[Stats.groups]]: 0 when unknown
    */
  def expectedGroups(p: RuntimePlan, keys: Seq[BExpr]): Int =
    if keys.isEmpty then
      0
    else
      Stats
        .groups(p, keys.map(_.expr))
        .map(g => math.min(g, GroupTable.MaxPresizeGroups.toDouble).toInt)
        .getOrElse(0)

  private class Lower(
      joinFilters: Boolean,
      rowJoinFilters: Boolean,
      buildSideJoinFilters: Boolean,
      presizeAggs: Boolean,
      accelerator: Option[PlanAccelerator],
      footerCache: Boolean,
      windowTopN: Boolean,
      windowKernels: Boolean,
      commonSubexpressions: Boolean,
      foldConstants: Boolean,
      windowSortKernels: Boolean,
      scanRowFilters: Boolean,
      dictionaryCodes: Boolean,
      dictionaryEntries: Boolean,
      lazyStrings: Boolean,
      selectiveRowFilters: Boolean,
      maxRowFilterSelectivity: Double,
      maskedCaseWhen: Boolean,
      windowSharePartitions: Boolean,
      constCaseSelect: Boolean,
      caseAggFilters: Boolean,
      dictionaryAggFilters: Boolean,
      routedAggFilters: Boolean,
      substringInLists: Boolean,
      windowTopNStream: Boolean,
      valueSelectivity: Boolean,
      columnImpliedRowFilters: Boolean,
      deferredNullChecks: Boolean,
      fusedDecimalArith: Boolean,
      semiAntiOneProbe: Boolean,
      cseHashedKeys: Boolean
  ):
    // Filters for the scans not lowered yet, by the id of the column they filter
    private val pending = mutable.Map.empty[Int, List[RuntimeFilter]]
    // The nodes not lowered yet that are in a duplicate-insensitive context, set by their parent
    // ([[ReduceSemiJoins.childContexts]]), so that shared CTE readers report it
    private val setNodes = java.util.IdentityHashMap[RuntimePlan, Boolean]()
    // Windows under a rank bound: lowered without shared subexpressions, which would wrap the
    // WindowOp that takes the bound in a projection
    private val topNWindows = java.util.IdentityHashMap[RuntimePlan, Boolean]()

    /** Lower p as read only for its distinct rows */
    def markInSet(p: RuntimePlan): Unit = setNodes.put(p, true)

    def lower(p: RuntimePlan): Operator =
      val inSet = setNodes.getOrDefault(p, false)
      val kids  = p.children
      if kids.nonEmpty then
        val contexts = ReduceSemiJoins.childContexts(p, inSet)
        kids
          .indices
          .foreach { i =>
            // A node read twice is in such a context only if both reads are
            val c = contexts(i) && setNodes.getOrDefault(kids(i), true)
            setNodes.put(kids(i), c)
          }
      // An accelerator takes the whole subtree, or none of it; its CPU fallback lowers p alone
      accelerator.flatMap(
        _.accelerate(
          p,
          q =>
            Lowering(
              q,
              presizeAggs = presizeAggs,
              inSet = inSet,
              footerCache = footerCache,
              commonSubexpressions = commonSubexpressions,
              cseHashedKeys = cseHashedKeys,
              foldConstants = foldConstants,
              scanRowFilters = scanRowFilters,
              dictionaryCodes = dictionaryCodes,
              dictionaryEntries = dictionaryEntries,
              lazyStrings = lazyStrings,
              selectiveRowFilters = selectiveRowFilters,
              maxRowFilterSelectivity = maxRowFilterSelectivity,
              maskedCaseWhen = maskedCaseWhen,
              constCaseSelect = constCaseSelect,
              caseAggFilters = caseAggFilters,
              dictionaryAggFilters = dictionaryAggFilters,
              routedAggFilters = routedAggFilters,
              substringInLists = substringInLists,
              valueSelectivity = valueSelectivity,
              columnImpliedRowFilters = columnImpliedRowFilters,
              deferredNullChecks = deferredNullChecks
            )
        )
      ) match
        case Some(op) =>
          op
        case None =>
          lowerNode(p, inSet)
      end match

    end lower

    /** A bound expression over an input whose column of id `i` is at `pos(i)`, folded if enabled */
    private def expr(e: BExpr, pos: Array[Int]): VExpr =
      val x      = e.lower(pos)
      val folded =
        if foldConstants then
          FoldConstants(x)
        else
          x
      val fused =
        if fusedDecimalArith then
          FuseDecimalArith(folded)
        else
          folded
      val masked  = MaskCaseWhen(fused, mask = maskedCaseWhen, consts = constCaseSelect)
      val regions =
        if substringInLists then
          SubstringInLists(masked)
        else
          masked
      if deferredNullChecks then
        DeferredNullChecks(regions)
      else
        regions

    /**
      * The input with the common subexpressions of `exprs` appended ([[CommonSubexpressions]]), and
      * the expressions over it
      */
    private def shared(input: Operator, exprs: IndexedSeq[VExpr]): (Operator, IndexedSeq[VExpr]) =
      if commonSubexpressions then
        CommonSubexpressions.share(input, exprs, cseHashedKeys)
      else
        (input, exprs)

    /** Rows of `in` for which `cond` is true, with its common subexpressions computed once */
    private def filterOver(in: Operator, cond: VExpr): Operator =
      val (input, conds) = shared(in, IndexedSeq(cond))
      if input eq in then
        FilterOp(in, conds.head)
      else
        // Without the shared columns
        FilterOp(input, conds.head, Array.range(0, in.schema.size))

    private def lowerNode(p: RuntimePlan, inSet: Boolean): Operator =
      p match
        case Scan(source, _, read, filter) =>
          ScanOp(source, p.schema, read, filter, scanFilters(p.output), footerCache)
            .withDictionaryCodes(dictionaryCodes)
            .withDictionaryEntries(dictionaryEntries)
            .withLazyStrings(lazyStrings)
        case Values(columns, rows) if columns.isEmpty && rows.size == 1 =>
          // One row without columns (a query without FROM), as the planner plans it
          SingleRowOp()
        case Values(_, rows) =>
          BatchesOp(p.schema, Seq(rows), "Values")
        case Filter(ref @ CTERef(cte, columns), cond, counts) =>
          // The reader reports its condition over the body's columns, which share positions
          val e      = cte.overBody(columns, cond.expr)
          val filter = JoinCondition(BExpr(e), PushDownFilters.remapCounts(cond.expr, e, counts))
          FilterOp(
            cte.reader(ref.schema, Some(filter), setNodes.getOrDefault(ref, false)),
            cond.lower(positions(ref))
          )
        case Filter(w: Window, cond, _) if windowTopN =>
          val filter = cond.lower(positions(w))
          topNWindows.put(w, true)
          val window =
            lower(w) match
              case op: WindowOp =>
                WindowOp(
                  op.child,
                  op.calls,
                  op.schema,
                  topN = WindowOp.topNOf(op.calls, op.child.schema.size, filter),
                  runningKernels = op.runningKernels,
                  sortKernels = op.sortKernels,
                  sharePartitions = op.sharePartitions,
                  streamTopN = windowTopNStream
                )
              case other =>
                other
          FilterOp(window, filter)
        case Filter(s: Scan, cond, _) if scanRowFilters && s.source.supportsRowFilter =>
          lower(s) match
            case scan: ScanOp =>
              val conds = ScanFilters.conjuncts(expr(cond, positions(s)))
              val name  = (i: Int) => Some(scan.sourceColumnName(i)).filter(s.source.canRowFilterOn)
              val (exactMoved, exactKept) = conds.partitionMap(c =>
                ScanFilters.exact(c, name).map(f => (c, f)).toLeft(c)
              )
              // A multi-column `or` stays in the engine when it implies per-column row filters
              val implied =
                if columnImpliedRowFilters then
                  conds.map(c => c -> ScanFilters.columnImplied(c)).filter(_._2.nonEmpty).toMap
                else
                  Map.empty[VExpr, List[VExpr]]
              val moved =
                exactMoved.filterNot((c, _) => implied.contains(c)) ++
                  implied.values.flatten.flatMap(c => ScanFilters.exact(c, name).map(f => (c, f)))
              val kept = exactKept ++ exactMoved.map(_._1).filter(implied.contains)
              // Filter-first decode pays per row for the row filter and the selective gathers, and
              // saves only on the rows it drops: filters keeping many rows stay in the engine
              lazy val passRate = ScanFilters.estimatedSelectivity(
                moved.map(_._1),
                s.source,
                scan.sourceColumnName,
                valueSelectivity
              )
              val selective =
                !selectiveRowFilters || moved.isEmpty || passRate <= maxRowFilterSelectivity
              if !selective then
                filterOver(scan, expr(cond, positions(s)))
              else
                val in =
                  if moved.isEmpty then
                    scan
                  else
                    scan.withRowFilter(
                      ScanFilters.ordered(moved, s.source, scan.sourceColumnName, valueSelectivity),
                      passRate
                    )
                kept.reduceOption(AndExpr(_, _)).map(filterOver(in, _)).getOrElse(in)
            case other =>
              filterOver(other, expr(cond, positions(s)))
        case Filter(child, cond, _) =>
          filterOver(lower(child), expr(cond, positions(child)))
        case Project(child, exprs, _) =>
          val pos         = positions(child)
          val (input, es) = shared(lower(child), exprs.map(expr(_, pos)))
          ProjectOp(input, es, p.schema)
        case Sort(child, keys, limit) =>
          val pos = positions(child)
          SortOp(lower(child), keys.map(_.lower(pos)), limit)
        case Limit(child, limit, offset) =>
          LimitOp(lower(child), limit, offset)
        case j: Join if semiAntiOneProbe && SemiAntiProbe.of(j).isDefined =>
          lowerFused(SemiAntiProbe.of(j).get)
        case j @ Join(left, right, kind, lk, rk, residual, buildLeft) =>
          val lpos = positions(left)
          val rpos = positions(right)
          // Before lowering the probe side, whose scans take the filters
          val filters =
            if joinFilters then
              runtimeFilters(j)
            else
              Nil
          JoinOrderOptimizer.joinOp(
            lower(left),
            lower(right),
            kind,
            lk.map(_.lower(lpos)),
            rk.map(_.lower(rpos)),
            residual.map(_.lower(positions(j.inputs))),
            buildLeft,
            p.schema,
            filters
          )
        case AsOfJoin(left, right, kind, lk, rk, lv, rv, op) =>
          val lpos = positions(left)
          val rpos = positions(right)
          AsOfJoinOp(
            lower(left),
            lower(right),
            lk.map(_.lower(lpos)),
            rk.map(_.lower(rpos)),
            lv.lower(lpos),
            rv.lower(rpos),
            op,
            kind,
            p.schema
          )
        case Aggregate(child, keys, calls, _) =>
          val pos = positions(child)
          val ks  = keys.map(expr(_, pos))
          val cs  = calls.map { c =>
            val l = c
              .lower(pos)
              .copy(args = c.args.map(expr(_, pos)), filter = c.filter.map(expr(_, pos)))
            val c2 =
              if caseAggFilters then
                CaseAggFilters(l)
              else
                l
            c2.copy(
              dictionaryFilter = dictionaryAggFilters && c2.filter.nonEmpty,
              routedFilter = routedAggFilters && c2.filter.nonEmpty
            )
          }
          // Keys, then each call's arguments and filter
          val (input, es) = shared(lower(child), ks ++ cs.flatMap(c => c.args ++ c.filter))
          val it          = es.iterator.drop(ks.size)
          val calls2      = cs.map { c =>
            val args = c.args.map(_ => it.next())
            c.copy(args = args, filter = c.filter.map(_ => it.next()))
          }
          HashAggOp(
            input,
            es.take(ks.size),
            calls2,
            p.schema,
            if presizeAggs then
              expectedGroups(child, keys)
            else
              0
          )
        case _: InnerJoins =>
          throw StatusCode
            .UNEXPECTED_STATE
            .newException(
              s"InnerJoins must be ordered by ReorderJoins before lowering:\n${p.explain}"
            )
        case Union(inputs, _) =>
          UnionAllOp(inputs.map(lower), p.schema)
        case SetOp(left, right, intersect, all, _) =>
          SetOpOp(lower(left), lower(right), intersect, all, p.schema)
        case Distinct(child) =>
          DistinctOp(lower(child))
        case Sample(child, percentage) =>
          Planner.sampleFilter(lower(child), percentage)
        case Window(child, calls, _) =>
          val pos = positions(child)
          val ws  = calls.map { c =>
            c.lower(pos)
              .copy(
                args = c.args.map(expr(_, pos)),
                partitionBy = c.partitionBy.map(expr(_, pos)),
                orderBy = c.orderBy.map(k => k.lower(pos).copy(expr = expr(k.expr, pos)))
              )
          }
          // Each call's arguments, partition keys, and sort keys
          val in          = lower(child)
          val exprs       = ws.flatMap(w => w.args ++ w.partitionBy ++ w.orderBy.map(_.expr))
          val (input, es) =
            if topNWindows.containsKey(p) then
              (in, exprs)
            else
              shared(in, exprs)
          val it     = es.iterator
          val calls2 = ws.map { w =>
            w.copy(
              args = w.args.map(_ => it.next()),
              partitionBy = w.partitionBy.map(_ => it.next()),
              orderBy = w.orderBy.map(_.copy(expr = it.next()))
            )
          }
          if input eq in then
            WindowOp(
              in,
              calls2,
              p.schema,
              runningKernels = windowKernels,
              sortKernels = windowSortKernels,
              sharePartitions = windowSharePartitions
            )
          else
            // The window appends its results to its input: drop the shared columns after it
            val width  = in.schema.size
            val extra  = input.schema.size - width
            val result = p.schema.fields.drop(width)
            val w      = WindowOp(
              input,
              calls2,
              Schema(input.schema.fields ++ result),
              runningKernels = windowKernels,
              sortKernels = windowSortKernels,
              sharePartitions = windowSharePartitions
            )
            ProjectOp(
              w,
              p.schema
                .fields
                .indices
                .map { i =>
                  val j =
                    if i < width then
                      i
                    else
                      i + extra
                  ColRef(j, p.schema(i).ptype, p.schema(i).name)
                },
              p.schema
            )
          end if
        case Unnest(child, lists, keepInput, outer, _) =>
          val pos = positions(child)
          UnnestOp(lower(child), lists.map(_.lower(pos)), keepInput, p.schema, outer)
        case CTERef(cte, _) =>
          cte.reader(p.schema, None, inSet)

    /** One RIGHT SEMI join probing T once for an EXISTS and a NOT EXISTS ([[SemiAntiProbe]]) */
    private def lowerFused(f: SemiAntiProbe.Fused): Operator =
      val j      = f.semiJoin
      val inputs = positions(j.inputs)
      // Before lowering the probe side, whose scan takes the filters
      val filters =
        if joinFilters then
          runtimeFilters(j)
        else
          Nil
      JoinOrderOptimizer.semiJoinOp(
        lower(f.build),
        lower(f.probe),
        f.buildKeys.map(_.lower(positions(f.build))),
        f.probeKeys.map(_.lower(positions(f.probe))),
        JoinKind.Semi,
        f.matches.map(_.lower(inputs)),
        buildLeft = true,
        filters,
        exclude = Some(f.exclude.lower(inputs))
      )

    private def scanFilters(columns: IndexedSeq[Column]): Seq[RuntimeFilter] =
      if pending.isEmpty then
        Nil
      else
        columns.flatMap(c => pending.remove(c.id.value).getOrElse(Nil))

    /**
      * A filter for each key of `j` whose probe key is a column read from a Parquet scan, given to
      * that scan once lowered. Only for joins that drop unmatched probe rows: inner, semi, and
      * right joins, and left / anti joins building on the left (a right join / right anti join over
      * swapped inputs)
      */
    private def runtimeFilters(j: Join): Seq[RuntimeFilter] = filteredProbe(j.kind, j.buildLeft)
      .toSeq
      .flatMap { left =>
        val (probe, probeKeys, buildKeys) =
          if left then
            (j.left, j.leftKeys, j.rightKeys)
          else
            (j.right, j.rightKeys, j.leftKeys)
        probeKeys
          .indices
          .flatMap { k =>
            probeKeys(k).expr match
              case c: ColRef if c.ptype == buildKeys(k).ptype && rangeType(c.ptype) =>
                scanColumns(probe, c.index, buildSideJoinFilters).map { (id, column) =>
                  val f = RuntimeFilter(
                    k,
                    column,
                    rangeFilter(column, c.ptype),
                    rows = rowJoinFilters
                  )
                  pending(id) = f :: pending.getOrElse(id, Nil)
                  f
                }
              case _ =>
                Nil
          }
      }

    end runtimeFilters

  end Lower

  /**
    * Whether the probe input of a join of `kind` (building on the left input when `buildLeft`) is
    * its left input (Some(true)) or its right one (Some(false)), when the join drops unmatched
    * probe rows, so that a scan on the probe side may drop rows whose key is no build key; None
    * otherwise. Inner, semi, and right joins over the left input; left / anti joins building on the
    * left (a right join / right anti join over swapped inputs). The formal model
    * `JoinKeyFilter.Probe` proves these sides sound, and `JoinKeyFilterConformanceTest` replays it
    */
  private[engine] def filteredProbe(kind: JoinKind, buildLeft: Boolean): Option[Boolean] =
    (kind, buildLeft) match
      case (JoinKind.Inner | JoinKind.Semi, buildLeft) =>
        Some(!buildLeft)
      case (JoinKind.Right, false) =>
        Some(true)
      case (JoinKind.Left | JoinKind.Anti, true) =>
        Some(false)
      case _ =>
        None

  /**
    * Whether a range of build keys of type `t` translates to a row-group filter. Join keys are cast
    * to a common type, so the build key has the probe column's type when the probe key is a column
    */
  private def rangeType(t: PType): Boolean =
    t match
      case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp | PType.TimestampTz =>
        true
      case _ =>
        false

  /** The scan filter for a [min, max] range of build key values over a column of type t */
  private def rangeFilter(column: String, t: PType): (Long, Long) => Option[ParquetFilter] =
    t match
      case PType.Int32 | PType.Date =>
        (lo, hi) => ScanFilters.range(column, t, lo.toInt, hi.toInt)
      case _ =>
        (lo, hi) => ScanFilters.range(column, t, lo, hi)

  /**
    * The Parquet columns of the scans that column `id` of `p` reads, with the id of each scan's
    * column, when each row of `p` holds the value of the scan row it comes from, so that dropping
    * scan rows by that column only drops rows of `p` with those values: through filters, plain
    * projections, the inputs of UNION ALL, and the probe sides of joins that preserve their rows.
    * With `buildSides`, also the preserved build sides of joins that keep unmatched build rows or
    * drop them by their matches (the left input of LEFT, semi, and anti joins building on it, the
    * right input of RIGHT joins building on it): a row of `p` from a build row comes with that
    * row's value too. Inner joins' builds are left out: a morsel pipeline mostly prepares them
    * before the build above, so the filter came too late and only cost time (TPC-DS q04, q76)
    */
  private def scanColumns(p: RuntimePlan, id: Int, buildSides: Boolean): Seq[(Int, String)] =
    p match
      case s @ Scan(source, columns, _, _) if source.supportsFilter =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            Seq(s.sourceColumnName(i)).filter(source.canFilterOn).map(id -> _)
      case Filter(child, _, _) =>
        scanColumns(child, id, buildSides)
      case Project(child, exprs, columns) =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            exprs(i).expr match
              case c: ColRef =>
                scanColumns(child, c.index, buildSides)
              case _ =>
                Nil
      // Each output row is a row of one input. An input column cast to a wider type is skipped
      case Union(inputs, columns) =>
        columns.indexWhere(_.id.value == id) match
          case -1 =>
            Nil
          case i =>
            inputs.flatMap { in =>
              val c = in.output(i)
              if c.ptype == columns(i).ptype then
                scanColumns(in, c.id.value, buildSides)
              else
                Nil
            }
      case Join(left, right, kind, _, _, _, buildLeft) =>
        val fromLeft = left.output.exists(_.id.value == id)
        val input    =
          if fromLeft then
            left
          else
            right
        if passesFilter(kind, buildLeft, fromLeft, buildSides) then
          scanColumns(input, id, buildSides)
        else
          Nil
      case _ =>
        Nil

  /**
    * Whether a runtime filter over a column of the left (`fromLeft`) or right input of a join below
    * the probe side may go into that input ([[scanColumns]]): the join's probe input when the join
    * drops unmatched probe rows, and with `buildSides` (`build_side_join_filters`) also the
    * preserved build input of a LEFT, semi, or anti join building on the left or a RIGHT join
    * building on the right. Never a NULL-padded input. `JoinKeyFilter/Probe.lean` proves these
    * inputs sound (`through_left`, `through_right_inner`, `through_right_of_right`) and the padded
    * ones unsound
    */
  private[engine] def passesFilter(
      kind: JoinKind,
      buildLeft: Boolean,
      fromLeft: Boolean,
      buildSides: Boolean
  ): Boolean =
    if fromLeft then
      kind match
        case JoinKind.Inner =>
          !buildLeft
        case JoinKind.Semi | JoinKind.Anti | JoinKind.Left =>
          !buildLeft || buildSides
        case _ =>
          false
    else
      kind match
        case JoinKind.Inner =>
          buildLeft
        case JoinKind.Right =>
          buildLeft || buildSides
        case _ =>
          false

  /** Column id -> position in the output of `p` (-1 for ids it doesn't output) */
  private[engine] def positions(p: RuntimePlan): Array[Int] = positions(p.output)

  /** Column id -> position in `out` (-1 for ids it doesn't have) */
  private[engine] def positions(out: IndexedSeq[Column]): Array[Int] =
    val pos = Array.fill(out.map(_.id.value).maxOption.getOrElse(-1) + 1)(-1)
    out.indices.foreach(i => pos(out(i).id.value) = i)
    pos

end Lowering

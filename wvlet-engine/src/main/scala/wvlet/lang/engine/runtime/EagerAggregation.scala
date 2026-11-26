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
import wvlet.lang.engine.expr.Arith
import wvlet.lang.engine.expr.ArithOp
import wvlet.lang.engine.expr.CastExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.InList
import wvlet.lang.engine.expr.Negate
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.PType

/**
  * Eager aggregation (Yan and Larson's eager group-by) below an inner hash join. An aggregation
  * `Aggregate(A ⋈ D, keys, calls)` whose calls read only A, and whose keys read D, is computed as
  * `Aggregate(Aggregate(A, g, partials) ⋈ D, keys, merges)`, where g are the columns of A that the
  * keys, A's join keys and the join's residual read. The join rows of an outer group split into
  * blocks (a partial group of A) × (a row of D): every row of a block has the same join keys and
  * the same outer group, so combining the partial results per block gives the same value for `sum`,
  * `min`, `max` and the counts (summed), whatever the duplicates of D. NULL keys of A form a
  * partial group that matches nothing, as its rows did.
  *
  * The join then probes (and gathers D's columns for) one row per partial group instead of one per
  * row of A: TPC-DS q04, q11 and q74 group the sales of each customer and year by seven customer
  * strings; the sales are now summed by customer key and year first, as DuckDB's
  * PARTIAL_AGGREGATE_PUSHDOWN does.
  *
  * It fires when the partial groups are estimated at most 1/[[MinReduction]] of A's rows and at
  * most [[MinReduction]] times the outer groups, and D is a scan, or a shared CTE read, under
  * projections and filters that keep at least [[MinRetention]] of its rows: a selective D would
  * drop most of A's rows in the join (and its runtime join filters would skip them in A's scan)
  * before they are aggregated. The outer aggregation needs keys, as a count over no rows is 0 while
  * the sum of no partial counts is NULL.
  *
  * With [[Optimization.EagerOtherSideMinMax]], calls that are mins or maxes of D's columns stay in
  * the outer aggregation over the joined rows (a block repeats D's row, which doesn't change them),
  * and D may be a cross join with a one-row side (a scalar subquery's result) that keeps its rows.
  *
  * With [[Optimization.EagerDistinct]], a DISTINCT over a projection of the join is rewritten the
  * same way, with the projection inlined into its keys ([[throughProject]]).
  */
object EagerAggregation extends Pass:
  import RuntimePlan.*

  /** The estimated rows of A per estimated partial group, at least */
  val MinReduction: Double = 4.0

  /**
    * The estimated rows of X per estimated partial group, at least, when the pre-aggregation
    * replaces the outer one ([[inJoinTree]])
    */
  val MinLookupReduction: Double = 1.5

  /** The estimated fraction of D's rows its filters keep, at least */
  val MinRetention: Double = 0.7

  override def name: String = Optimization.EagerAggregation.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    ctx.newColumnId match
      case Some(newId) if ctx.options.isEnabled(Optimization.EagerAggregation) =>
        walk(p, ctx, newId)
      case _ =>
        p

  private def walk(p: RuntimePlan, ctx: OptimizerContext, newId: () => ColumnId): RuntimePlan =
    val children = p.children
    val next     = children.map(walk(_, ctx, newId))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case a: Aggregate =>
        rewrite(a, ctx.options, newId) match
          case Some(r) =>
            ctx.fired(name)
            r
          case None =>
            val inJoin =
              if ctx.options.isEnabled(Optimization.EagerAggregationInJoinTrees) then
                inJoinTree(a, ctx.options, newId)
              else
                None
            inJoin match
              case Some(r) =>
                ctx.fired(Optimization.EagerAggregationInJoinTrees.name)
                r
              case None if ctx.options.isEnabled(Optimization.EagerDistinct) =>
                eagerDistinct(a, ctx, newId).getOrElse(node)
              case None =>
                node
      case Distinct(child) if ctx.options.isEnabled(Optimization.EagerDistinct) =>
        val a: Aggregate = Aggregate(
          child,
          child.output.map(BExpr.ref),
          IndexedSeq.empty,
          child.output
        )
        rewrite(a, ctx.options, newId) match
          case Some(r) =>
            ctx.fired(Optimization.EagerDistinct.name)
            r
          case None =>
            eagerDistinct(a, ctx, newId).getOrElse(node)
      case _ =>
        node
    end match

  end walk

  /** [[rewrite]] of a DISTINCT through the projection under it ([[throughProject]]) */
  private def eagerDistinct(
      a: Aggregate,
      ctx: OptimizerContext,
      newId: () => ColumnId
  ): Option[RuntimePlan] = throughProject(a)
    .flatMap(rewrite(_, ctx.options, newId))
    .map { r =>
      ctx.fired(Optimization.EagerDistinct.name)
      r
    }

  /**
    * A DISTINCT (an aggregation without calls) over a deterministic projection of a join, as the
    * same aggregation over the join with the projection's expressions inlined into its keys
    * ([[Optimization.EagerDistinct]]). The keys then read the join's columns, so [[rewrite]] can
    * deduplicate a side of the join by the columns they and the join keys read. The outer distinct
    * stays over the join, and the deduplicated side still holds every distinct value of the columns
    * read above it, so the result is the same distinct projected rows (TPC-DS q38's customer names
    * and dates), not the distinct join keys
    */
  private def throughProject(a: Aggregate): Option[Aggregate] =
    a.child match
      case Project(j: Join, exprs, columns) if a.calls.isEmpty && exprs.forall(deterministic) =>
        val byId = columns.map(_.id).zip(exprs).toMap
        val keys = a
          .keys
          .map(k =>
            BExpr(VExprs.mapRefs(k.expr, c => byId.get(ColumnId(c.index)).fold(c: VExpr)(_.expr)))
          )
        Some(Aggregate(j, keys, a.calls, a.columns))
      case _ =>
        None

  private def lower(c: AggregateCall): String = c.name.toLowerCase

  /**
    * Whether `c` is a min or max without a filter, whose value over the joined rows of an outer
    * group doesn't depend on how many times each row repeats
    */
  private def isMinMax(c: AggregateCall): Boolean =
    c.filter.isEmpty && Set("min", "max").contains(lower(c))

  /** Whether `c` counts rows, so that its partial results are summed */
  private def isCount(c: AggregateCall): Boolean =
    lower(c) match
      case "count" | "count_star" | "count_if" | "countif" =>
        true
      case _ =>
        false

  /** Whether the partial results of `c` combine with another aggregate */
  private def supported(c: AggregateCall): Boolean =
    !c.distinct && c.order.isEmpty && c.exprs.forall(e => safe(e.expr)) &&
      (isCount(c) || Set("sum", "min", "max").contains(lower(c)))

  /**
    * Whether `e` can't fail on any row: the partial aggregation evaluates the calls on rows of A
    * that the join would drop, as DuckDB's PARTIAL_AGGREGATE_PUSHDOWN checks too. Columns,
    * constants, `+`, `-`, `*`, negation, division by a nonzero constant, and casts that widen a
    * number
    */
  private def safe(e: VExpr): Boolean =
    e match
      case _: ColRef | _: Const =>
        true
      case Negate(child) =>
        safe(child)
      case Arith(ArithOp.Add | ArithOp.Sub | ArithOp.Mul, l, r, _) =>
        safe(l) && safe(r)
      case Arith(ArithOp.Div | ArithOp.IntDiv | ArithOp.Mod, l, Const(v: Number, _), _) =>
        v.doubleValue != 0 && safe(l)
      case CastExpr(child, to, tryCast) =>
        val widens =
          (child.ptype, to) match
            case (PType.Int32 | PType.Int64 | _: PType.Decimal, PType.Float64) =>
              true
            case (PType.Int32, PType.Int64) =>
              true
            case _ =>
              false
        (tryCast || widens) && safe(child)
      case _ =>
        false

  private def rewrite(
      a: Aggregate,
      options: OptimizerOptions,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    // A condition over both sides of the join, which stays above it as a filter
    val (join, above) =
      a.child match
        case Filter(j: Join, cond, counts) =>
          (Some(j), Some((cond, counts)))
        case j: Join =>
          (Some(j), None)
        case _ =>
          (None, None)
    join match
      case Some(j @ Join(left, right, JoinKind.Inner, lk, rk, residual, _))
          if lk.nonEmpty && a.keys.nonEmpty && a.keys.forall(deterministic) &&
            a.calls.forall(supported) &&
            (lk ++ rk ++ residual ++ above.map(_._1)).forall(deterministic) =>
        val keyRefs     = a.keys.flatMap(_.refs).toSet
        val conds       = residual.toSeq ++ above.map(_._1)
        val otherMinMax = options.isEnabled(Optimization.EagerOtherSideMinMax)
        // The calls computed over side `in` before the join, or None when a call reads neither
        // `in` only nor, as a min or max that stays above the join, `other` only
        def pushable(in: RuntimePlan, other: RuntimePlan): Option[Set[Int]] =
          val ids      = in.output.map(_.id).toSet
          val otherIds = other.output.map(_.id).toSet
          val split    = a
            .calls
            .zipWithIndex
            .map { (c, i) =>
              val refs = c.exprs.flatMap(_.refs).toSet
              if refs.subsetOf(ids) then
                Some(Some(i))
              else if otherMinMax && isMinMax(c) && refs.subsetOf(otherIds) then
                Some(None)
              else
                None
            }
          Option.when(
            split.forall(_.isDefined) && other.output.exists(c => keyRefs.contains(c.id)) &&
              retained(other, options)
          )(split.flatten.flatten.toSet)
        // With `eager_lookup_projection`: the join looks up at most one row of `other` per row of
        // the pre-aggregation, and every call is computed before the join
        def lookup(eagerCalls: Set[Int], other: RuntimePlan, otherKeys: Seq[BExpr]): Boolean =
          options.isEnabled(Optimization.EagerLookupProjection) &&
            eagerCalls.size == a.calls.size && DependentGroupKeys.looksUp(other, otherKeys)
        val pushed =
          pushable(left, right) match
            case Some(eagerCalls) =>
              eager(a, eagerCalls, left, lk ++ conds, lookup(eagerCalls, right, rk), options, newId)
                .map { (pre, m, outer) =>
                  (j.copy(left = pre, leftKeys = lk.map(remap(_, m))), m, outer)
                }
            case None =>
              pushable(right, left).flatMap { eagerCalls =>
                eager(
                  a,
                  eagerCalls,
                  right,
                  rk ++ conds,
                  lookup(eagerCalls, left, lk),
                  options,
                  newId
                ).map { (pre, m, outer) =>
                  (j.copy(right = pre, rightKeys = rk.map(remap(_, m))), m, outer)
                }
              }
        pushed.map { (joined, m, outer) =>
          val withResidual = joined.copy(residual = residual.map(remap(_, m)))
          outer(
            above.fold(withResidual: RuntimePlan)((cond, counts) =>
              Filter(withResidual, remap(cond, m), counts)
            )
          )
        }
      case _ =>
        None
    end match

  end rewrite

  /**
    * The partial aggregation of side `in` of the join under `a`, by the columns of `in` that a's
    * keys and `exprs` (the side's join keys and the conditions over the join) read, if it reduces
    * `in` enough: with the new columns of those columns, and the outer aggregation over the join.
    * When the join is a `lookup` (one row of the other side per partial group, all calls computed
    * eagerly) and a's keys determine the partial groups ([[DependentGroupKeys.determines]]), each
    * outer group is one joined row, and the outer aggregation is a projection of the partials
    */
  private def eager(
      a: Aggregate,
      eagerCalls: Set[Int],
      in: RuntimePlan,
      exprs: Seq[BExpr],
      lookup: Boolean,
      options: OptimizerOptions,
      newId: () => ColumnId
  ): Option[(RuntimePlan, Map[ColumnId, Column], RuntimePlan => RuntimePlan)] =
    val read   = (a.keys ++ exprs).flatMap(_.refs).toSet
    val groups = in.output.filter(c => read.contains(c.id))
    val est    =
      groups.foldLeft(Option(1.0)) { (acc, c) =>
        acc.flatMap(n => distinct(in, c.id).map(d => n * math.max(1.0, d)))
      }
    // The outer groups, unbounded when a key's distinct values are unknown: few outer groups
    // (TPC-DS q36's 100 item classes) aggregate cheaply after the join, while finer partial
    // groups (its 102K items) cost a second aggregation that the join's savings don't repay
    val outer       = outerGroups(a)
    lazy val inRows = Stats.treeRows(in, options)
    // With `clustered_group_estimates`, the partial groups of rows clustered by their keys
    val partialGroups = est.map { e =>
      if options.isEnabled(Optimization.ClusteredGroupEstimates) then
        ClusteredGroups.groups(in, groups.map(c => BExpr.ref(c).expr), e, inRows)
      else
        e
    }
    val reduces = partialGroups.exists(e =>
      e * MinReduction <= inRows && outer.forall(_ * MinReduction >= e)
    )
    if groups.isEmpty || !reduces then
      None
    else
      val keyCols     = groups.map(c => Column(newId(), c.name, c.ptype, c.qualifiers))
      val m           = groups.map(_.id).zip(keyCols).toMap
      val calls       = a.calls.zip(a.columns.drop(a.keys.size)).zipWithIndex
      val partials    = calls.filter((_, i) => eagerCalls.contains(i))
      val partialCols = partials.map { case ((p, out), i) =>
        i -> Column(newId(), out.name, p.ptype)
      }
      val partialOf = partialCols.toMap
      val pre       = Aggregate(
        in,
        groups.map(BExpr.ref),
        partials.map(_._1._1),
        keyCols ++ partialCols.map(_._2)
      )
      // A min or max of the other side's columns stays as it is, over the joined rows
      val merges = calls.map { case ((c, _), i) =>
        partialOf.get(i) match
          case Some(p) =>
            val name =
              if isCount(c) then
                "sum"
              else
                c.name
            AggregateCall(name, Seq(BExpr.ref(p)), distinct = false, filter = None, c.ptype)
          case None =>
            c
      }
      val keys = a.keys.map(remap(_, m))
      if lookup && DependentGroupKeys.determines(a, in, groups.map(_.id)) then
        Some((pre, m, j => Project(j, keys ++ partialCols.map(p => BExpr.ref(p._2)), a.columns)))
      else
        Some((pre, m, j => Aggregate(j, keys, merges, a.columns)))
    end if

  end eager

  /**
    * A pre-aggregation of an input X deeper in the join tree under `a`
    * ([[Optimization.EagerAggregationInJoinTrees]]): X is a side of an inner hash join below the
    * aggregation's child join, the calls read X's columns only, and the tree between X and the
    * aggregation is inner hash joins, filters and projections that read no other columns of X than
    * its group columns g (the ones the keys, join keys, residuals, filters and projections above
    * read). Every join above X looks up at most one row of its other side by a declared unique key
    * ([[DependentGroupKeys.uniqueOn]]), and a's keys determine g
    * ([[DependentGroupKeys.determines]]). Each output row of the tree is then one X row combined
    * with rows that depend on it only through g, an outer group holds the X rows of one partial
    * group, and so the tree over the partial groups is one row per outer group: the outer
    * aggregation becomes a projection of the partial results, and the pre-aggregation replaces it
    * once it reduces X at all ([[MinLookupReduction]]). The joins and filters above X keep at least
    * [[MinRetention]] of their rows, or they would drop most of X's rows before the join.
    *
    * TPC-H q10 sums its order lines per `o_custkey` before looking up their customer and nation:
    * the customer join builds 380K rows instead of 1.15M at SF10, and the aggregation by customer
    * is gone. Without lookups the outer aggregation would merge the partials; that pays off less
    * (TPC-DS q04's CTE bodies get their year filters after this pass, and a pre-aggregation below
    * their date join ran 1.07x slower at SF10), so it isn't done.
    */
  private def inJoinTree(
      a: Aggregate,
      options: OptimizerOptions,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    if a.keys.isEmpty || !a.keys.forall(deterministic) || !a.calls.forall(supported) then
      None
    else
      val callRefs = a.calls.flatMap(_.exprs.flatMap(_.refs)).toSet
      // The plan with the pre-aggregation, the new ids of X's group columns, and the partial
      // results (one per call)
      case class Pushed(plan: RuntimePlan, m: Map[ColumnId, Column], partials: Seq[Column])

      def preAggregate(
          x: RuntimePlan,
          needed: Set[ColumnId],
          sub: Map[ColumnId, ColumnId]
      ): Option[Pushed] =
        val ids    = x.output.map(_.id).toSet
        val groups = x.output.filter(c => needed.contains(c.id))
        if groups.isEmpty || !callRefs.map(sub).subsetOf(ids) ||
          !DependentGroupKeys.determines(a, x, groups.map(_.id))
        then
          None
        else
          val est =
            groups.foldLeft(Option(1.0)) { (acc, c) =>
              acc.flatMap(n => distinct(x, c.id).map(d => n * math.max(1.0, d)))
            }
          lazy val xRows    = Stats.treeRows(x, options)
          val partialGroups = est.map { e =>
            if options.isEnabled(Optimization.ClusteredGroupEstimates) then
              ClusteredGroups.groups(x, groups.map(c => BExpr.ref(c).expr), e, xRows)
            else
              e
          }
          Option.when(partialGroups.exists(_ * MinLookupReduction <= xRows)) {
            val keyCols  = groups.map(c => Column(newId(), c.name, c.ptype, c.qualifiers))
            val calls    = a.calls.zip(a.columns.drop(a.keys.size))
            val partials = calls.map((c, out) => Column(newId(), out.name, c.ptype))
            val byId     = x.output.map(c => c.id -> c).toMap
            val toX      = sub.view.mapValues(byId).toMap
            val subbed   = calls.map((c, _) =>
              c.copy(args = c.args.map(remap(_, toX)), filter = c.filter.map(remap(_, toX)))
            )
            Pushed(
              Aggregate(x, groups.map(BExpr.ref), subbed, keyCols ++ partials),
              groups.map(_.id).zip(keyCols).toMap,
              partials
            )
          }
        end if
      end preAggregate

      // `needed`: the columns of p read above it other than by the calls; `sub`: the calls'
      // columns as columns of p
      def down(
          p: RuntimePlan,
          needed: Set[ColumnId],
          sub: Map[ColumnId, ColumnId]
      ): Option[Pushed] =
        p match
          case j @ Join(left, right, JoinKind.Inner, lk, rk, residual, _)
              if lk.nonEmpty && (lk ++ rk ++ residual).forall(deterministic) =>
            val callIds = callRefs.map(sub)
            val below   = needed ++ (lk ++ rk ++ residual).flatMap(_.refs)
            // Every join above the pre-aggregation looks up one row of its other side, and keeps
            // most rows of its other side and of x: their runtime join filters no longer reach
            // x's scans, and a selective join would drop most of x's rows before they are
            // aggregated. A join whose other side the aggregation reads nothing of only filters
            // x, and is best done first
            def side(x: RuntimePlan, other: RuntimePlan, otherKeys: Seq[BExpr]): Option[Pushed] =
              val lookup =
                residual.isEmpty && other.output.exists(c => needed.contains(c.id)) &&
                  DependentGroupKeys.looksUp(other, otherKeys) && retainedTree(other, options) &&
                  Stats.treeRows(j, options) >= MinRetention * Stats.treeRows(x, options)
              if lookup then
                preAggregate(x, below, sub).orElse(down(x, below, sub))
              else
                None
            // Calls reading no columns (count(*)) fit either side
            def viaLeft = Option
              .when(callIds.subsetOf(left.output.map(_.id).toSet))(side(left, right, rk))
              .flatten
              .map { r =>
                r.copy(plan =
                  j.copy(
                    left = r.plan,
                    leftKeys = lk.map(remap(_, r.m)),
                    residual = residual.map(remap(_, r.m))
                  )
                )
              }
            def viaRight = Option
              .when(callIds.subsetOf(right.output.map(_.id).toSet))(side(right, left, lk))
              .flatten
              .map { r =>
                r.copy(plan =
                  j.copy(
                    right = r.plan,
                    rightKeys = rk.map(remap(_, r.m)),
                    residual = residual.map(remap(_, r.m))
                  )
                )
              }
            viaLeft.orElse(viaRight)
          case Filter(child, cond, counts)
              if deterministic(cond) && keeps(child, cond, counts, options) =>
            down(child, needed ++ cond.refs, sub).map { r =>
              r.copy(plan = Filter(r.plan, remap(cond, r.m), counts))
            }
          case Project(child, exprs, columns) if exprs.forall(deterministic) =>
            val callIds = callRefs.map(sub)
            // The calls read plain copies of the child's columns
            val passed = columns
              .indices
              .collect {
                case i if callIds.contains(columns(i).id) =>
                  exprs(i).expr match
                    case c: ColRef =>
                      Some(columns(i).id -> ColumnId(c.index))
                    case _ =>
                      None
              }
            if passed.exists(_.isEmpty) then
              None
            else
              val through   = passed.flatten.toMap
              val neededIdx = columns.indices.filter(i => needed.contains(columns(i).id))
              val below     = neededIdx.flatMap(i => exprs(i).refs).toSet
              down(child, below, sub.view.mapValues(through).toMap).flatMap { r =>
                val childIds = r.plan.output.map(_.id).toSet
                // The columns the calls alone read are gone below
                val kept = columns
                  .indices
                  .map(i => (remap(exprs(i), r.m), columns(i)))
                  .filter((e, _) => e.refs.forall(childIds.contains))
                Option.when(neededIdx.forall(i => kept.exists(_._2.id == columns(i).id))) {
                  val outIds = columns.map(_.id).toSet
                  r.copy(
                    plan = Project(
                      r.plan,
                      kept.map(_._1) ++ r.partials.map(BExpr.ref),
                      kept.map(_._2) ++ r.partials
                    ),
                    m = r.m.filterNot((id, _) => outIds.contains(id))
                  )
                }
              }
            end if
          case _ =>
            None

      down(a.child, a.keys.flatMap(_.refs).toSet, callRefs.map(id => id -> id).toMap).map { r =>
        Project(r.plan, a.keys.map(remap(_, r.m)) ++ r.partials.map(BExpr.ref), a.columns)
      }
    end if

  end inJoinTree

  /**
    * Whether p is [[retained]], or an inner hash join of retained trees that keeps at least
    * [[MinRetention]] of its larger input's estimated rows (a customer ⋈ nation lookup)
    */
  private def retainedTree(p: RuntimePlan, options: OptimizerOptions): Boolean =
    p match
      case j @ Join(l, r, JoinKind.Inner, lk, _, _, _) if lk.nonEmpty =>
        retainedTree(l, options) && retainedTree(r, options) &&
        Stats.treeRows(j, options) >=
          MinRetention * math.max(Stats.treeRows(l, options), Stats.treeRows(r, options))
      case _ =>
        retained(p, options)

  /** Whether the filter keeps at least [[MinRetention]] of its input's estimated rows */
  private def keeps(
      child: RuntimePlan,
      cond: BExpr,
      counts: Map[String, Int],
      options: OptimizerOptions
  ): Boolean =
    FactorOr
      .conjuncts(cond.expr)
      .map(c => Stats.selectivity(child, JoinCondition(BExpr(c), counts), options))
      .product >= MinRetention

  /** The estimated groups of `a`, unknown when a key's distinct values are */
  private def outerGroups(a: Aggregate): Option[Double] =
    a.keys
      .foldLeft(Option(1.0)) { (acc, k) =>
        acc.flatMap(n =>
          k.expr match
            case c: ColRef =>
              distinct(a.child, ColumnId(c.index)).map(d => n * math.max(1.0, d))
            case _ =>
              None
        )
      }

  /** e with its references to the keys of `m` replaced by references to their columns */
  private[runtime] def remap(e: BExpr, m: Map[ColumnId, Column]): BExpr = BExpr(
    VExprs.mapRefs(
      e.expr,
      c =>
        m.get(ColumnId(c.index)) match
          case Some(to) =>
            ColRef(to.id.value, c.ptype, to.name)
          case None =>
            c
    )
  )

  /**
    * Whether p is a scan or a shared CTE read under projections and filters estimated to keep at
    * least [[MinRetention]] of its rows
    */
  private def retained(p: RuntimePlan, options: OptimizerOptions): Boolean =
    def loop(p: RuntimePlan): Option[Double] =
      p match
        case _: Scan | _: CTERef =>
          Some(1.0)
        case Project(child, exprs, _) if exprs.forall(deterministic) =>
          loop(child)
        case Join(l, r, JoinKind.Cross, _, _, None, _)
            if options.isEnabled(Optimization.EagerOtherSideMinMax) =>
          if oneRow(r) then
            loop(l)
          else if oneRow(l) then
            loop(r)
          else
            None
        case Filter(child, cond, counts) =>
          loop(child).map { r =>
            r *
              FactorOr
                .conjuncts(cond.expr)
                .map(c => Stats.selectivity(child, JoinCondition(BExpr(c), counts), options))
                .product
          }
        case _ =>
          None
    loop(p).exists(_ >= MinRetention)

  /** Whether p returns exactly one row: an aggregation without keys under projections */
  private def oneRow(p: RuntimePlan): Boolean =
    p match
      case Aggregate(_, keys, _, _) =>
        keys.isEmpty
      case Project(child, _, _) =>
        oneRow(child)
      case _ =>
        false

  /**
    * The distinct values of column `id` of p, as [[Stats.distinct]] estimates them, but at most the
    * number of constants a filter on the way compares the column to (`x = c`, `x in (...)`, or an
    * `or` of them): TPC-DS's `d_year = 2001 or d_year = 2002` leaves 2 years of date_dim's 201
    */
  private[runtime] def distinct(p: RuntimePlan, id: ColumnId): Option[Double] =
    p match
      case Filter(child, cond, _) =>
        val below = distinct(child, id)
        FactorOr.conjuncts(cond.expr).flatMap(constants(_, id)).minOption match
          case Some(n) =>
            Some(below.fold(n.toDouble)(math.min(_, n.toDouble)))
          case None =>
            below
      case Project(child, exprs, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          exprs(i).expr match
            case c: ColRef =>
              distinct(child, ColumnId(c.index))
            case _ =>
              None
      case Join(left, right, _, lk, _, _, _) if lk.nonEmpty =>
        if left.output.exists(_.id == id) then
          distinct(left, id)
        else
          distinct(right, id)
      case other =>
        Stats.distinct(other, id)

  /** The number of constants condition `e` restricts column `id` to, if it does */
  private def constants(e: VExpr, id: ColumnId): Option[Int] =
    def isCol(x: VExpr): Boolean =
      x match
        case c: ColRef =>
          c.index == id.value
        case _ =>
          false
    e match
      case Compare(CmpOp.Eq, x, _: Const) if isCol(x) =>
        Some(1)
      case Compare(CmpOp.Eq, _: Const, x) if isCol(x) =>
        Some(1)
      case InList(x, values, _, false, _) if isCol(x) =>
        Some(values.size)
      case o: OrExpr =>
        val ds = FactorOr.disjuncts(o).map(constants(_, id))
        Option.when(ds.forall(_.isDefined))(ds.flatten.sum)
      case _ =>
        None

  private def deterministic(e: BExpr): Boolean = PushDownFilters.isDeterministic(e.expr)

end EagerAggregation

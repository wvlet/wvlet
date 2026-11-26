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
  */
object EagerAggregation extends Pass:
  import RuntimePlan.*

  /** The estimated rows of A per estimated partial group, at least */
  val MinReduction: Double = 4.0

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
            node
      case _ =>
        node

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
        val pushed =
          pushable(left, right) match
            case Some(eagerCalls) =>
              eager(a, eagerCalls, left, lk ++ conds, options, newId).map { (pre, m, outer) =>
                (j.copy(left = pre, leftKeys = lk.map(remap(_, m))), m, outer)
              }
            case None =>
              pushable(right, left).flatMap { eagerCalls =>
                eager(a, eagerCalls, right, rk ++ conds, options, newId).map { (pre, m, outer) =>
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
    * `in` enough: with the new columns of those columns, and the outer aggregation over the join
    */
  private def eager(
      a: Aggregate,
      eagerCalls: Set[Int],
      in: RuntimePlan,
      exprs: Seq[BExpr],
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
    val outer =
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
    val reduces = est.exists(e =>
      e * MinReduction <= Stats.treeRows(in, options) && outer.forall(_ * MinReduction >= e)
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
      Some((pre, m, j => Aggregate(j, a.keys.map(remap(_, m)), merges, a.columns)))
    end if

  end eager

  private def remap(e: BExpr, m: Map[ColumnId, Column]): BExpr = BExpr(
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

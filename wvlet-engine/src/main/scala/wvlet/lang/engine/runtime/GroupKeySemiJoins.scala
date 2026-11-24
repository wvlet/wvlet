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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions

/**
  * Semi-join reduction of an aggregation by the keys its join can match. A hash join whose one side
  * (B) is, through projections and deterministic filters, an aggregation with group keys, and that
  * drops B's unmatched rows (an inner, left, or semi join with B on the right, or a right join with
  * B on the left that builds on the right), outputs nothing of a group whose key no row of the
  * other side (O) has. When a key of O is traced to a small relation S whose column s holds all of
  * its non-NULL values, the aggregation's input I becomes `I ⋉ S` on `g = s` for B's key g: a
  * filter on a group key commutes with grouping, so the groups with a key in S keep all their rows,
  * and the others are no longer computed. A NULL key matches nothing, in the join as in the semi
  * join.
  *
  * TPC-H q17's decorrelated `avg(l_quantity)` per `l_partkey` over all of `lineitem` is joined only
  * with the parts of one brand and container; it now aggregates only their rows. DuckDB pushes the
  * outer keys into such subqueries with a delim join. TPC-H q20's sum per part and supplier is on
  * the left of a right join, and is reduced by the parts of a prefix LIKE.
  *
  * S is read again for the semi join, so it must be a scan under filters and projections (or a
  * shared CTE read), estimated at most 1/[[MinKeyRatio]] of the group key's distinct values, and
  * its scan no larger than the aggregation's input. The copy keeps its column ids: the semi join
  * outputs only I's columns, so no node outputs both copies.
  */
object GroupKeySemiJoins extends Pass:
  import RuntimePlan.*

  /** The estimated distinct values of the group key per estimated row of S, at least */
  val MinKeyRatio: Double = 8.0

  override def name: String = Optimization.GroupKeySemiJoins.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if !ctx.options.isEnabled(Optimization.GroupKeySemiJoins) then
      p
    else
      walk(p, ctx)

  private def walk(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    val children = p.children
    val next     = children.map(walk(_, ctx))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case j @ Join(left, right, kind, lk, rk, _, _) if lk.nonEmpty =>
        val pairs = lk.indices.map(i => (lk(i), rk(i)))
        // An anti join is left out, as a NOT IN reads B's NULL keys
        val reduced =
          if kind == JoinKind.Inner || kind == JoinKind.Left || kind == JoinKind.Semi then
            // B on the right: every kind that drops B's unmatched rows. B stays on the right, so
            // that O, lowered first, takes the runtime join filters registered for the scan
            // column ids the copy of S shares with it
            reduce(right, left, pairs, ctx).map(r => j.copy(right = r))
          else if kind == JoinKind.Right && !j.buildLeft then
            // The mirror of a left join, with B on the left. B is lowered first, but no runtime
            // join filter from above reaches through a right join into its build side O, so the
            // copy of S in B can take none of the filters meant for S
            reduce(left, right, pairs.map(_.swap), ctx).map(l => j.copy(left = l))
          else
            None
        reduced match
          case Some(r) =>
            ctx.fired(name)
            r
          case None =>
            node
      case _ =>
        node

  end walk

  /** A key source: column `s` of S holds every non-NULL value of the traced key */
  private case class Source(plan: RuntimePlan, s: Column, rows: Double, scanRows: Double)

  /**
    * B with its aggregation's input semi-joined with a source of O's key, for the key pairs `pairs`
    * (O's key, B's key), if one pays off
    */
  private def reduce(
      b: RuntimePlan,
      o: RuntimePlan,
      pairs: Seq[(BExpr, BExpr)],
      ctx: OptimizerContext
  ): Option[RuntimePlan] = pairs
    .iterator
    .flatMap { (ok, bk) =>
      for
        oc  <- column(ok)
        bc  <- column(bk)
        rew <- rewriteAgg(b, bc, oc, o, ctx)
      yield rew
    }
    .nextOption()

  private def column(e: BExpr): Option[ColumnId] =
    e.expr match
      case c: ColRef =>
        Some(ColumnId(c.index))
      case _ =>
        None

  /**
    * p (B) with the aggregation that its column `id` is a group key of semi-joined with the best
    * source of O's column `oc`; None when B is no such aggregation or no source pays off
    */
  private def rewriteAgg(
      p: RuntimePlan,
      id: ColumnId,
      oc: ColumnId,
      o: RuntimePlan,
      ctx: OptimizerContext
  ): Option[RuntimePlan] =
    p match
      case Project(child, exprs, columns) =>
        val i = columns.indexWhere(_.id == id)
        if i < 0 then
          None
        else
          exprs(i).expr match
            case c: ColRef =>
              rewriteAgg(child, ColumnId(c.index), oc, o, ctx).map(Project(_, exprs, columns))
            case _ =>
              None
      case f @ Filter(child, cond, _) if deterministic(cond) =>
        rewriteAgg(child, id, oc, o, ctx).map(c => f.copy(child = c))
      case a @ Aggregate(child, keys, _, columns) if keys.nonEmpty =>
        val k = columns.indexWhere(_.id == id)
        if k < 0 || k >= keys.size || !keys.forall(deterministic) || reduced(child, keys) then
          None
        else
          val g = keys(k)
          // The distinct values of g, of which S has to hold few
          val distinct = Stats.distinct(child, g.expr)
          val inRows   = Stats.rows(child)
          val outType  = o.output.find(_.id == oc).map(_.ptype)
          if distinct.isEmpty || !outType.contains(g.ptype) then
            None
          else
            sources(o, oc, ctx.options)
              .filter(s => s.s.ptype == g.ptype && s.scanRows <= inRows)
              .filter(s => s.rows * MinKeyRatio <= distinct.get)
              .minByOption(_.rows)
              .map { s =>
                val keyed = Project(s.plan, IndexedSeq(BExpr.ref(s.s)), IndexedSeq(s.s))
                val semi  = Join(
                  child,
                  keyed,
                  JoinKind.Semi,
                  IndexedSeq(g),
                  IndexedSeq(BExpr.ref(s.s)),
                  None,
                  buildLeft = JoinOrderOptimizer.swapBuild(
                    ctx.options,
                    Stats.rows(keyed),
                    Stats.rows(child)
                  )
                )
                a.copy(child = semi)
              }
        end if
      case _ =>
        None

  /**
    * Whether `input` is already semi-joined on one of the group keys, so that a re-run of the
    * passes (the planner optimizes some inputs before their parents) adds no other
    */
  private def reduced(input: RuntimePlan, keys: IndexedSeq[BExpr]): Boolean =
    input match
      case Join(_, _, JoinKind.Semi, lk, _, _, _) =>
        lk.exists(k => keys.exists(_.expr == k.expr))
      case _ =>
        false

  /**
    * The sources of column `id` of p: relations whose column holds every non-NULL value `id` takes
    * in p's output, traced through filters, pass-through projections, the side of a join it comes
    * from, and the key equalities of inner and semi joins. Only the cheap ones (a scan under
    * filters and projections, or a shared CTE read) are returned
    */
  private def sources(p: RuntimePlan, id: ColumnId, options: OptimizerOptions): Seq[Source] =
    val self = p
      .output
      .find(_.id == id)
      .flatMap(c => cheap(p, options).map((r, s) => Source(p, c, r, s)))
    val traced =
      p match
        case Filter(child, _, _) =>
          sources(child, id, options)
        case Project(child, exprs, columns) =>
          val i = columns.indexWhere(_.id == id)
          if i < 0 then
            Nil
          else
            exprs(i).expr match
              case c: ColRef if c.ptype == columns(i).ptype =>
                sources(child, ColumnId(c.index), options)
              case _ =>
                Nil
        case Join(left, right, kind, lk, rk, _, _) if lk.nonEmpty =>
          val inLeft                         = left.output.exists(_.id == id)
          val (side, other, keys, otherKeys) =
            if inLeft then
              (left, right, lk, rk)
            else
              (right, left, rk, lk)
          // Through a key equality, when every output row is a match: an inner join, a semi join
          // from its left side, or a left join from its right side (whose values are NULL on the
          // unmatched rows)
          val equated =
            kind == JoinKind.Inner ||
              (kind == JoinKind.Semi && inLeft) ||
              (kind == JoinKind.Left && !inLeft)
          val across =
            if !equated then
              Nil
            else
              keys
                .indices
                .flatMap { i =>
                  (column(keys(i)), column(otherKeys(i))) match
                    case (Some(k), Some(ok))
                        if k == id && keys(i).ptype == otherKeys(i).ptype &&
                          other.output.exists(_.id == ok) =>
                      sources(other, ok, options)
                    case _ =>
                      Nil
                }
          sources(side, id, options) ++ across
        case _ =>
          Nil
    self.toSeq ++ traced

  end sources

  /**
    * The estimated rows of p and of the scan under it, when p is a scan or a shared CTE read under
    * deterministic filters and projections, so that reading it again gives the same rows
    */
  private def cheap(p: RuntimePlan, options: OptimizerOptions): Option[(Double, Double)] =
    p match
      case s: Scan =>
        val r = Stats.rows(s)
        Some((r, r))
      case c: CTERef =>
        // Computed once, whatever reads it
        Some((Stats.rows(c), 0.0))
      case Filter(child, cond, counts) if deterministic(cond) =>
        cheap(child, options).map { (r, scan) =>
          val conds = FactorOr.conjuncts(cond.expr).map(c => JoinCondition(BExpr(c), counts))
          val rows  =
            child match
              case _: CTERef =>
                // Counted from the result once the CTE is computed
                Stats.filteredRows(child, conds, options)
              case _ =>
                r * conds.map(Stats.selectivity(child, _, options)).product
          (rows, scan)
        }
      case Project(child, exprs, _) if exprs.forall(deterministic) =>
        cheap(child, options)
      case _ =>
        None

  private def deterministic(e: BExpr): Boolean = PushDownFilters.isDeterministic(e.expr)

end GroupKeySemiJoins

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
import wvlet.lang.engine.expr.CastExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.RowFn
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.PType

import scala.collection.mutable

/**
  * Pushes filter conjuncts down the plan, in one top-down walk after [[Normalize]] and before
  * [[ReorderJoins]]:
  *
  *   - through a projection, substituting the projected expressions (when they are deterministic),
  *     a sort without a limit, and an aggregation with grouping keys when the conjunct reads only
  *     the keys;
  *   - into the preserved input of a LEFT or RIGHT join, when the conjunct reads only that input;
  *   - into an inner join tree, as a condition that [[ReorderJoins]] places. There, `a = b` and
  *     `a = constant` add `b = constant` for columns `a` and `b` of the same type.
  *
  * It stops at filters (whose own conjuncts push down from there), limits, full joins, and
  * [[RuntimePlan.Legacy]] leaves. A conjunct keeps its literal count (selectivity) as it moves.
  *
  * The pass also turns the equalities between the inputs of an outer join's residual into hash join
  * keys, as the planner does for ON conditions. That always happens, since the binder leaves them
  * to the pass; with `push_down_filters` disabled, only the conjuncts stay where they are.
  */
object PushDownFilters extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.PushDownFilters.name

  /** A conjunct moving down, with the literal counts of its sub-conditions by bound signature */
  private case class Conjunct(expr: VExpr, counts: Map[String, Int]):
    lazy val refs: Set[ColumnId] = BExpr(expr).refs

    /** The conjunct with its column references replaced, keeping the literal counts */
    def substitute(f: ColRef => VExpr): Conjunct =
      val e = VExprs.mapRefs(expr, f)
      Conjunct(e, remapCounts(expr, e, counts))

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan = Walk(
    ctx,
    ctx.options.isEnabled(Optimization.PushDownFilters)
  ).push(p, Nil)

  /**
    * Rows of p for which the conditions are true: pushed down p as far as they go, or a filter over
    * p when the pass is disabled
    */
  def filter(p: RuntimePlan, conds: Seq[JoinCondition], ctx: OptimizerContext): RuntimePlan =
    if conds.isEmpty then
      p
    else if ctx.options.isEnabled(Optimization.PushDownFilters) then
      Walk(ctx, enabled = true).push(
        p,
        conds.map(c => Conjunct(c.cond.expr, c.literalCounts)).toList
      )
    else
      Filter(p, BExpr(conds.map(_.cond.expr).reduce(AndExpr(_, _))))

  private class Walk(ctx: OptimizerContext, enabled: Boolean):
    /** Rows of p for which every conjunct is true, with the conjuncts pushed as far as they go */
    def push(p: RuntimePlan, conds: List[Conjunct]): RuntimePlan =
      p match
        case f @ Filter(child, cond, counts) =>
          if enabled then
            // The filter's own conjuncts move down from here; those from above stay above it.
            // A non-deterministic conjunct (random()) stays, since moving it changes which rows
            // it sees
            val own = conjuncts(cond.expr).map(Conjunct(_, counts))
            if !own.forall(c => isDeterministic(c.expr)) then
              filter(f.copy(child = push(child, Nil)), conds)
            else
              val pushed =
                push(child, own) match
                  case Filter(c, rest, _) if conjuncts(rest.expr) == own.map(_.expr) =>
                    // Every conjunct stayed: keep the filter as it was
                    f.copy(child = c)
                  case other =>
                    other
              filter(pushed, conds)
          else
            filter(f.copy(child = push(child, Nil)), conds)
        case pr @ Project(child, exprs, columns) =>
          val byId      = columns.indices.map(i => columns(i).id -> exprs(i).expr).toMap
          val (in, out) = conds.partition(c =>
            c.refs.forall(id => byId.get(id).exists(isDeterministic))
          )
          moved(in)
          val pushed = in.map(_.substitute(r => byId(ColumnId(r.index))))
          filter(withChild(pr, push(child, pushed)), out)
        case s @ Sort(child, _, None) =>
          moved(conds)
          withChild(s, push(child, conds))
        case a @ Aggregate(child, keys, _, columns) if keys.nonEmpty =>
          // Group key columns come first
          val byId      = keys.indices.map(i => columns(i).id -> keys(i).expr).toMap
          val (in, out) = conds.partition(_.refs.forall(id => byId.get(id).exists(isDeterministic)))
          moved(in)
          val pushed = in.map(_.substitute(r => byId(ColumnId(r.index))))
          filter(withChild(a, push(child, pushed)), out)
        case j: Join =>
          val keyed = joinKeys(j)
          val side  =
            keyed.kind match
              case JoinKind.Left =>
                Some(true)
              case JoinKind.Right =>
                Some(false)
              case _ =>
                None
          side match
            case Some(left) =>
              val ids =
                (
                  if left then
                    keyed.left
                  else
                    keyed.right
                ).output.map(_.id).toSet
              val (in, out) = conds.partition(_.refs.subsetOf(ids))
              moved(in)
              val l =
                if left then
                  push(keyed.left, in)
                else
                  push(keyed.left, Nil)
              val r =
                if left then
                  push(keyed.right, Nil)
                else
                  push(keyed.right, in)
              filter(keyed.withChildren(Seq(l, r)), out)
            case None =>
              filter(keyed.withChildren(keyed.children.map(push(_, Nil))), conds)
        case j @ InnerJoins(inputs, joinConds) =>
          val added = conds
            .filterNot(c => c.expr == Const(true, PType.Bool))
            .map(c => JoinCondition(BExpr(c.expr), c.counts))
          val all          = joinConds ++ added
          val withInferred =
            if enabled then
              all ++ inferred(all)
            else
              all
          if withInferred.size > joinConds.size then
            ctx.fired(name)
          val newInputs = inputs.map(push(_, Nil))
          if withInferred.size == joinConds.size && newInputs.lazyZip(inputs).forall(_ eq _) then
            j
          else
            InnerJoins(newInputs, withInferred)
        case Scan(_, _) | Values(_, _) | Legacy(_, _) | Sort(_, _, _) | Limit(_, _, _) |
            Aggregate(_, _, _, _) =>
          filter(p.withChildren(p.children.map(push(_, Nil))), conds)

    private def withChild(p: RuntimePlan, child: RuntimePlan): RuntimePlan = p.withChildren(
      Seq(child)
    )

    /** Rows of p for which the conjuncts are true */
    private def filter(p: RuntimePlan, conds: List[Conjunct]): RuntimePlan =
      if conds.isEmpty then
        p
      else
        val counts = conds.foldLeft(Map.empty[String, Int])(_ ++ _.counts)
        Filter(p, BExpr(conds.map(_.expr).reduce(AndExpr(_, _))), counts)

    /** Record that conjuncts moved down */
    private def moved(conds: List[Conjunct]): Unit =
      if conds.nonEmpty then
        ctx.fired(name)

  end Walk

  /**
    * An outer join with the equalities between its inputs in the residual moved to its hash join
    * keys, in written order, as `Planner.joinOn` builds them: each side cast to their common type
    */
  def joinKeys(j: Join): Join =
    j.kind match
      case JoinKind.Left | JoinKind.Right | JoinKind.Full if j.residual.nonEmpty =>
        val leftIds                                 = j.left.output.map(_.id).toSet
        val rightIds                                = j.right.output.map(_.id).toSet
        def sides(e: VExpr): Option[(VExpr, VExpr)] =
          e match
            case Compare(CmpOp.Eq, l, r) =>
              val (lr, rr)                                    = (BExpr(l).refs, BExpr(r).refs)
              def on(ids: Set[ColumnId], refs: Set[ColumnId]) = refs.nonEmpty && refs.subsetOf(ids)
              if on(leftIds, lr) && on(rightIds, rr) then
                Some((l, r))
              else if on(rightIds, lr) && on(leftIds, rr) then
                Some((r, l))
              else
                None
            case _ =>
              None
        val conds       = conjuncts(j.residual.get.expr)
        val (eqs, rest) = conds.partition(sides(_).isDefined)
        if eqs.isEmpty then
          j
        else
          val pairs = eqs.map { e =>
            val (l, r) = sides(e).get
            val t      = PType.common(l.ptype, r.ptype)
            (BExpr(castTo(l, t)), BExpr(castTo(r, t)))
          }
          j.copy(
            leftKeys = j.leftKeys ++ pairs.map(_._1),
            rightKeys = j.rightKeys ++ pairs.map(_._2),
            residual = Option.when(rest.nonEmpty)(BExpr(rest.reduce(AndExpr(_, _))))
          )
      case _ =>
        j

  private def castTo(e: VExpr, t: PType): VExpr =
    if e.ptype == t then
      e
    else
      CastExpr(e, t)

  /**
    * `b = constant` for each `a = constant` (a literal) and `a = b` among the conditions, when the
    * columns have the same type and no condition compares `b` to that constant yet
    */
  private def inferred(conds: IndexedSeq[JoinCondition]): Seq[JoinCondition] =
    // Classes of columns equal to each other
    val parent            = mutable.Map.empty[Int, Int]
    def find(i: Int): Int =
      val p = parent.getOrElse(i, i)
      if p == i then
        i
      else
        val root = find(p)
        parent(i) = root
        root
    val refs = mutable.Map.empty[Int, ColRef]
    conds.foreach { c =>
      c.cond.expr match
        case Compare(CmpOp.Eq, a: ColRef, b: ColRef) if a.ptype == b.ptype && a.index != b.index =>
          refs(a.index) = a
          refs(b.index) = b
          parent(find(a.index)) = find(b.index)
        case _ =>
      end match
    }
    if refs.isEmpty then
      Nil
    else
      val existing  = conds.map(_.cond.expr.sig).toSet
      val constants = conds.flatMap { c =>
        if c.literals.contains(1) then
          c.cond.expr match
            case Compare(CmpOp.Eq, a: ColRef, k: Const) if refs.contains(a.index) =>
              Some(a -> k)
            case Compare(CmpOp.Eq, k: Const, a: ColRef) if refs.contains(a.index) =>
              Some(a -> k)
            case _ =>
              None
        else
          None
      }
      val added = mutable.LinkedHashMap.empty[String, JoinCondition]
      constants.foreach { (a, k) =>
        refs
          .values
          .toSeq
          .sortBy(_.index)
          .filter(b => b.index != a.index && find(b.index) == find(a.index) && b.ptype == a.ptype)
          .foreach { b =>
            val e   = Compare(CmpOp.Eq, b, k)
            val sig = e.sig
            if !existing.contains(sig) && !added.contains(sig) then
              added(sig) = JoinCondition(BExpr(e), Map(sig -> 1))
          }
      }
      added.values.toSeq
    end if

  end inferred

  private def isDeterministic(e: VExpr): Boolean =
    e match
      case f: RowFn if f.name == "random" =>
        false
      case other =>
        other.children.forall(isDeterministic)

  /** The conjuncts of a condition, as [[FactorOr]] splits them */
  private def conjuncts(e: VExpr): List[VExpr] = FactorOr.conjuncts(e)

  /**
    * The literal counts of a rewritten condition: the counts of `from`'s `and`/`or` sub-conditions,
    * keyed by the signatures of the matching sub-conditions of `to`
    */
  private def remapCounts(from: VExpr, to: VExpr, counts: Map[String, Int]): Map[String, Int] =
    val b                                  = Map.newBuilder[String, Int]
    def loop(from: VExpr, to: VExpr): Unit =
      (from, to) match
        case (AndExpr(l, r), AndExpr(l2, r2)) =>
          loop(l, l2)
          loop(r, r2)
        case (OrExpr(l, r), OrExpr(l2, r2)) =>
          loop(l, l2)
          loop(r, r2)
        case _ =>
          counts.get(from.sig).foreach(n => b += to.sig -> n)
    loop(from, to)
    b.result()

end PushDownFilters

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
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.Optimization

import scala.collection.mutable

/**
  * Semi-join reduction where only the set of rows matters. Below the subquery side of a semi or
  * anti join, a DISTINCT, a group-by without aggregate calls, or an INTERSECT / EXCEPT, how often a
  * row occurs doesn't change the result (a duplicate-insensitive context). There an inner join
  * whose needed columns all come from some of its inputs becomes a semi join of those inputs with
  * the others: the distinct rows of `X ⋈ Y` projected to X's columns are those of `X ⋉ Y` on the
  * same condition, and a NULL condition drops the pair in both (`formal/WvletFormal/SemiReduce`).
  *
  * The context passes through deterministic filters and projections, sorts without a limit, inner
  * joins, the left side of semi and anti joins, and UNION ALL; Window, Limit, top-N sorts, Sample,
  * aggregate calls, and outer joins count duplicates and end it. Each split needs at least one
  * equality key, so that the semi join is a hash join. Q95's
  * `ws_order_number in (from web_returns, ws_wh ...)` no longer expands every web return by its
  * warehouse pairs.
  *
  * A shared CTE body is reduced when it is finalized, if every reader reads it in such a context
  * ([[reduceBody]]); the readers' contexts come from [[childContexts]], which lowering follows too.
  * The walk also applies [[SelfJoinAggregate]] to the semi joins it meets or makes.
  */
object ReduceSemiJoins extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.ReduceSemiJoins.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if !ctx.options.isEnabled(Optimization.ReduceSemiJoins) then
      p
    else
      walk(p, inSet = false, ids(p), ctx)

  /**
    * A shared CTE body all of whose readers are in a duplicate-insensitive context, reduced for the
    * readers reading the columns `needed`
    */
  def reduceBody(body: RuntimePlan, needed: Set[ColumnId], ctx: OptimizerContext): RuntimePlan =
    walk(body, inSet = true, needed, ctx)

  /**
    * Whether each child of p (in [[RuntimePlan.children]] order) is in a duplicate-insensitive
    * context, when p is (`inSet`) or isn't
    */
  def childContexts(p: RuntimePlan, inSet: Boolean): Seq[Boolean] =
    p match
      case Filter(_, cond, _) =>
        Seq(inSet && deterministic(cond))
      case Project(_, exprs, _) =>
        Seq(inSet && exprs.forall(deterministic))
      case Sort(_, _, limit) =>
        // A top-N keeps the first rows, duplicates included
        Seq(inSet && limit.isEmpty)
      case Join(_, _, kind, lk, rk, residual, _) =>
        val det = (lk ++ rk ++ residual).forall(deterministic)
        kind match
          case JoinKind.Inner | JoinKind.Cross =>
            Seq(inSet && det, inSet && det)
          case JoinKind.Semi | JoinKind.Anti =>
            Seq(inSet && det, det)
          case _ =>
            Seq(false, false)
      case InnerJoins(inputs, conds) =>
        val det = conds.forall(c => deterministic(c.cond))
        inputs.map(_ => inSet && det)
      case Union(inputs, _) =>
        inputs.map(_ => inSet)
      case Distinct(_) =>
        Seq(true)
      case Aggregate(_, keys, calls, _) =>
        Seq(calls.isEmpty && keys.forall(deterministic))
      case SetOp(_, _, _, all, _) =>
        Seq(!all, !all)
      case other =>
        other.children.map(_ => false)

  private def deterministic(e: BExpr): Boolean = PushDownFilters.isDeterministic(e.expr)

  private def ids(p: RuntimePlan): Set[ColumnId] = p.output.map(_.id).toSet

  /**
    * p with its joins reduced, where `inSet` says whether p is in a duplicate-insensitive context
    * and `needed` are the columns of p its parent reads
    */
  private def walk(
      p: RuntimePlan,
      inSet: Boolean,
      needed: Set[ColumnId],
      ctx: OptimizerContext
  ): RuntimePlan =
    val reduced =
      if !inSet then
        None
      else
        p match
          case Filter(child, cond, counts) if deterministic(cond) =>
            semi(child, Some(JoinCondition(cond, counts)), needed, ctx)
          case _: InnerJoins | _: Join =>
            semi(p, None, needed, ctx)
          case _ =>
            None
    reduced.foreach(_ => ctx.fired(name))
    val node = reduced.getOrElse(p)
    // A semi join of a table with itself, possibly one just reduced, as a group-by of the table
    SelfJoinAggregate.rewrite(node, inSet, needed, ctx) match
      case Some(r) =>
        ctx.fired(Optimization.SelfJoinAggregate.name)
        walkChildren(r, inSet, needed, ctx)
      case None =>
        walkChildren(node, inSet, needed, ctx)

  private def walkChildren(
      p: RuntimePlan,
      inSet: Boolean,
      needed: Set[ColumnId],
      ctx: OptimizerContext
  ): RuntimePlan =
    val children = p.children
    if children.isEmpty then
      p
    else
      val contexts = childContexts(p, inSet)
      val reads    = childNeeds(p, needed)
      val next     = children.indices.map(i => walk(children(i), contexts(i), reads(i), ctx))
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)

  /** The columns of each child of p that p reads, when its parent reads `needed` of p */
  private def childNeeds(p: RuntimePlan, needed: Set[ColumnId]): Seq[Set[ColumnId]] =
    def refs(es: Iterable[BExpr]): Set[ColumnId] = es.iterator.flatMap(_.refs).toSet
    p match
      case Filter(child, cond, _) =>
        Seq(needed ++ cond.refs)
      case Project(_, exprs, _) =>
        // Every expression, so that no reduced child drops a column a projection reads
        Seq(refs(exprs))
      case Sort(_, keys, _) =>
        Seq(needed ++ refs(keys.map(_.expr)))
      case Join(left, right, _, lk, rk, residual, _) =>
        // A semi or anti join outputs only left's columns, so `needed` reads none of right's
        val both = needed ++ refs(lk) ++ refs(rk) ++ refs(residual)
        Seq(both & ids(left), both & ids(right))
      case InnerJoins(inputs, conds) =>
        val all = needed ++ refs(conds.map(_.cond))
        inputs.map(i => all & ids(i))
      case Aggregate(_, keys, calls, _) =>
        Seq(refs(keys) ++ refs(calls.flatMap(_.exprs)))
      case other =>
        other.children.map(ids)

  /**
    * The semi join that p (an inner join, with a condition `extra` of the filter above it) reduces
    * to when its parent reads only the columns `needed`, if it does
    */
  private def semi(
      p: RuntimePlan,
      extra: Option[JoinCondition],
      needed: Set[ColumnId],
      ctx: OptimizerContext
  ): Option[RuntimePlan] =
    p match
      case Join(left, right, JoinKind.Inner, lk, rk, residual, buildLeft) if lk.nonEmpty =>
        val cond = (residual.toSeq ++ extra.map(_.cond)).reduceOption(and)
        if !needed.exists(ids(right).contains) then
          Some(Join(left, right, JoinKind.Semi, lk, rk, cond, buildLeft))
        else if !needed.exists(ids(left).contains) then
          Some(Join(right, left, JoinKind.Semi, rk, lk, cond, !buildLeft))
        else
          None
      case InnerJoins(inputs, conds) =>
        splitInnerJoins(inputs, conds ++ extra, needed, ctx)
      case _ =>
        None

  /**
    * An inner join tree reduced to its inputs that output `needed` (S), semi-joined with each
    * connected group of the others. None if every input outputs a needed column, none does, or a
    * group shares no equality with S
    */
  private def splitInnerJoins(
      inputs: IndexedSeq[RuntimePlan],
      conds: IndexedSeq[JoinCondition],
      needed: Set[ColumnId],
      ctx: OptimizerContext
  ): Option[RuntimePlan] =
    val kept = inputs.indices.filter(i => inputs(i).output.exists(c => needed.contains(c.id)))
    if kept.isEmpty || kept.size == inputs.size then
      None
    else
      val owner = inputs.indices.flatMap(i => inputs(i).output.map(_.id -> i)).toMap
      def owners(e: BExpr): Set[Int] = e.refs.map(owner)
      val inS                        = kept.toSet
      val others                     = inputs.indices.filterNot(inS)
      // Group the other inputs that a condition reads together
      val group             = mutable.Map.from(others.map(i => i -> i))
      def find(i: Int): Int =
        if group(i) == i then
          i
        else
          find(group(i))
      val condOwners = conds.map(c => c -> owners(c.cond))
      condOwners.foreach { (_, o) =>
        val t = o.filterNot(inS).toSeq
        t.drop(1).foreach(i => group(find(i)) = find(t.head))
      }
      val groups = others.groupBy(find).values.toSeq.sortBy(_.head)
      val sConds = condOwners.collect {
        case (c, o) if o.subsetOf(inS) =>
          c
      }
      val sPlan = joinOf(kept.map(inputs), sConds)
      // One semi join per group: its own conditions inside, the ones it shares with S as keys
      val semis: Seq[Option[RuntimePlan => RuntimePlan]] = groups.map { g =>
        val gs    = g.toSet
        val inner = condOwners.collect {
          case (c, o) if o.nonEmpty && o.subsetOf(gs) =>
            c
        }
        val shared = condOwners.collect {
          case (c, o) if o.exists(gs) && o.exists(inS) =>
            c
        }
        val keys = shared.flatMap { c =>
          c.cond.expr match
            case Compare(CmpOp.Eq, a, b) =>
              val (oa, ob) = (owners(BExpr(a)), owners(BExpr(b)))
              if oa.nonEmpty && oa.subsetOf(inS) && ob.nonEmpty && ob.subsetOf(gs) then
                Some(c -> (BExpr(a), BExpr(b)))
              else if ob.nonEmpty && ob.subsetOf(inS) && oa.nonEmpty && oa.subsetOf(gs) then
                Some(c -> (BExpr(b), BExpr(a)))
              else
                None
            case _ =>
              None
        }
        if keys.isEmpty then
          None
        else
          val keyConds = keys.map(_._1).toSet
          val residual = shared.filterNot(keyConds).map(_.cond).reduceOption(and)
          val right    = joinOf(g.map(inputs), inner)
          Some((left: RuntimePlan) =>
            // Build on the smaller side, as the binder does for IN: a much larger group probes
            // a table of the kept rows (a RIGHT SEMI join)
            Join(
              left,
              right,
              JoinKind.Semi,
              keys.map(_._2._1),
              keys.map(_._2._2),
              residual,
              buildLeft = JoinOrderOptimizer.swapBuild(
                ctx.options,
                Stats.rows(right),
                Stats.rows(left)
              )
            )
          )
      }
      if semis.exists(_.isEmpty) || !connected(kept, sConds.map(c => (c, owners(c.cond)))) then
        None
      else
        Some(semis.flatten.foldLeft(sPlan)((acc, f) => f(acc)))
    end if

  end splitInnerJoins

  /**
    * Whether equalities among `conds` connect every input of `kept`, so that their inner joins need
    * no cross product (which the joins with the other inputs may have avoided)
    */
  private def connected(kept: Seq[Int], conds: Seq[(JoinCondition, Set[Int])]): Boolean =
    val reached = mutable.Set(kept.head)
    var grown   = true
    while grown do
      grown = false
      conds.foreach { (c, o) =>
        c.cond.expr match
          case Compare(CmpOp.Eq, _, _) if o.size > 1 && o.exists(reached) && !o.subsetOf(reached) =>
            reached ++= o
            grown = true
          case _ =>
      }
    kept.forall(reached)

  /** The inner joins of `inputs` under `conds` (a filter over a single input) */
  private def joinOf(
      inputs: IndexedSeq[RuntimePlan],
      conds: IndexedSeq[JoinCondition]
  ): RuntimePlan =
    if inputs.size > 1 then
      InnerJoins(inputs, conds)
    else if conds.isEmpty then
      inputs.head
    else
      Filter(inputs.head, conds.map(_.cond).reduce(and), conds.flatMap(_.literalCounts).toMap)

  private def and(a: BExpr, b: BExpr): BExpr = BExpr(AndExpr(a.expr, b.expr))

end ReduceSemiJoins

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
import wvlet.lang.engine.expr.IsNullExpr
import wvlet.lang.engine.expr.Kernels
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.RowFn
import wvlet.lang.engine.expr.ScalarFunctions
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.PType

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Pushes filter conjuncts down the plan, in one top-down walk after [[Normalize]] and before
  * [[ReorderJoins]]:
  *
  *   - through a projection, substituting the projected expressions (when they are deterministic),
  *     a sort without a limit, and an aggregation with grouping keys when the conjunct reads only
  *     the keys;
  *   - into the preserved input of a LEFT or RIGHT join, when the conjunct reads only that input,
  *     and into the left input of a semi or anti join. With `join_key_filters`, a conjunct over
  *     that input's hash join keys also filters the other input, over the matching keys there;
  *   - with `join_key_filters`, into either input of an inner join the binder already ordered (an
  *     aggregation or subquery input), and across its keys to the other input;
  *   - into an inner join tree, as a condition that [[ReorderJoins]] places. There, `a = b` and
  *     `a = constant` add `b = constant` for columns `a` and `b` of the same type;
  *   - into every input of a UNION, INTERSECT, or EXCEPT, by position and cast to the output type,
  *     and below a DISTINCT;
  *   - below a window, when the conjunct reads only columns that every window call partitions by.
  *
  * It stops at filters (whose own conjuncts push down from there), limits, and full joins. A
  * conjunct keeps its literal count (selectivity) as it moves.
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
    * p when the pass is disabled. With `throughFilters`, they also move below the filters directly
    * over aggregations in p (HAVING), along with those filters' own conjuncts (for a plan already
    * optimized, such as a shared CTE body), instead of staying above them
    */
  def filter(
      p: RuntimePlan,
      conds: Seq[JoinCondition],
      ctx: OptimizerContext,
      throughFilters: Boolean = false
  ): RuntimePlan =
    if conds.isEmpty then
      p
    else if ctx.options.isEnabled(Optimization.PushDownFilters) then
      Walk(ctx, enabled = true, throughFilters).push(
        p,
        conds.map(c => Conjunct(c.cond.expr, c.literalCounts)).toList
      )
    else
      Filter(p, BExpr(conds.map(_.cond.expr).reduce(AndExpr(_, _))))

  private class Walk(ctx: OptimizerContext, enabled: Boolean, throughFilters: Boolean = false):
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
            else if throughFilters && conds.nonEmpty && child.isInstanceOf[Aggregate] then
              // A HAVING filter: the conditions read the grouping keys, which every input row of
              // a group shares, so they can't fail on rows the filter would have dropped
              moved(conds)
              push(child, own ++ conds)
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
          val ids   = (q: RuntimePlan) => q.output.map(_.id).toSet
          keyed.kind match
            case JoinKind.Left | JoinKind.Semi | JoinKind.Anti | JoinKind.Right =>
              // A semi or anti join outputs its left input's columns, so every conjunct reads
              // only them. The conjuncts over the preserved input's keys also filter the other
              // input: a row there that fails one only matches preserved rows the conjunct drops
              val left      = keyed.kind != JoinKind.Right
              val (in, out) = conds.partition(
                _.refs
                  .subsetOf(
                    ids(
                      if left then
                        keyed.left
                      else
                        keyed.right
                    )
                  )
              )
              moved(in)
              val carried = carry(in, keyed, fromLeft = left)
              val l       = push(
                keyed.left,
                if left then
                  in
                else
                  carried
              )
              val r = push(
                keyed.right,
                if left then
                  carried
                else
                  in
              )
              filter(keyed.withChildren(Seq(l, r)), out)
            case JoinKind.Inner | JoinKind.Cross if carryKeys =>
              // A join the binder already ordered (an aggregation or subquery input): each
              // conjunct over one input filters that input, and, over its keys, the other one
              val (inL, rest) = conds.partition(_.refs.subsetOf(ids(keyed.left)))
              val (inR, out)  = rest.partition(_.refs.subsetOf(ids(keyed.right)))
              moved(inL ++ inR)
              val l = push(keyed.left, inL ++ carry(inR, keyed, fromLeft = false))
              val r = push(keyed.right, inR ++ carry(inL, keyed, fromLeft = true))
              filter(keyed.withChildren(Seq(l, r)), out)
            case _ =>
              filter(keyed.withChildren(keyed.children.map(push(_, Nil))), conds)
          end match
        case j @ AsOfJoin(left, right, _, _, _, _, _, _) =>
          // Each left row matches the same right row whatever the other left rows are, so a
          // conjunct over left columns keeps the same rows below. A conjunct over right columns
          // stays above: below, it would change which right row is the closest
          val ids       = left.output.map(_.id).toSet
          val (in, out) = conds.partition(_.refs.subsetOf(ids))
          moved(in)
          filter(j.withChildren(Seq(push(left, in), push(right, Nil))), out)
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
        case Union(_, _) | SetOp(_, _, _, _, _) =>
          // For INTERSECT and EXCEPT, rows equal under the set comparison give a conjunct the
          // same value, so filtering both inputs keeps the result. An input narrower than the
          // output keeps the conjuncts above
          val columns   = p.output
          val pos       = columns.indices.map(i => columns(i).id.value -> i).toMap
          val (in, out) =
            if p.children.forall(_.output.size >= columns.size) then
              conds.partition(_.refs.forall(id => pos.contains(id.value)))
            else
              (Nil, conds)
          moved(in)
          filter(
            p.withChildren(p.children.map(i => push(i, in.map(toInput(_, columns, pos, i))))),
            out
          )
        case d @ Distinct(child) =>
          moved(conds)
          withChild(d, push(child, conds))
        case w @ Window(child, calls, _) =>
          // A conjunct over columns every call partitions by removes whole partitions, so the
          // window values of the other rows stay the same
          val keys = calls.map(_.partitionColumns).reduceOption(_ intersect _).getOrElse(Set.empty)
          val (in, out) = conds.partition(_.refs.subsetOf(keys))
          moved(in)
          filter(withChild(w, push(child, in)), out)
        case u @ Unnest(child, _, keepInput, _, _) =>
          // Each row of the child repeats its columns in every row it unnests to (one, for an
          // outer unnest without elements), so a conjunct over them keeps the same rows below
          val ids =
            if keepInput then
              child.output.map(_.id).toSet
            else
              Set.empty[ColumnId]
          val (in, out) = conds.partition(_.refs.subsetOf(ids))
          moved(in)
          filter(withChild(u, push(child, in)), out)
        case Scan(_, _, _, _) | Values(_, _) | CTERef(_, _) | Sort(_, _, _) | Limit(_, _, _) |
            Aggregate(_, _, _, _) | Sample(_, _) =>
          filter(p.withChildren(p.children.map(push(_, Nil))), conds)

    /**
      * A conjunct over the output `columns` of a set operation as a conjunct over its input: each
      * column becomes the input column at its position (`pos`, by column id), cast to the output
      * type
      */
    private def toInput(
        c: Conjunct,
        columns: IndexedSeq[Column],
        pos: Map[Int, Int],
        input: RuntimePlan
    ): Conjunct =
      val in = input.output
      c.substitute { r =>
        val col = in(pos(r.index))
        castTo(ColRef(col.id.value, col.ptype, col.name), columns(pos(r.index)).ptype)
      }

    /** Whether conjuncts carry across join keys (`join_key_filters`) */
    private val carryKeys = enabled && ctx.options.isEnabled(Optimization.JoinKeyFilters)

    /**
      * The conjuncts among `conds` that read only key columns of one input of a hash join (the left
      * one with `fromLeft`), over the matching key columns of the other input instead. A row of the
      * other input matches a row of this one only when their keys are equal and not NULL, so the
      * conjunct has the same value on both rows. Only key pairs of plain columns of the same type
      * count, and not floating-point ones, whose key equality differs from `=` (`-0.0`, NaN)
      */
    private def carry(conds: List[Conjunct], j: Join, fromLeft: Boolean): List[Conjunct] =
      if !carryKeys || conds.isEmpty then
        Nil
      else
        val (from, to) =
          if fromLeft then
            (j.leftKeys, j.rightKeys)
          else
            (j.rightKeys, j.leftKeys)
        val partner = mutable.Map.empty[Int, ColRef]
        from
          .lazyZip(to)
          .foreach { (a, b) =>
            (a.expr, b.expr) match
              case (x: ColRef, y: ColRef)
                  if x.ptype == y.ptype && carriesKey(x.ptype) && !partner.contains(x.index) =>
                partner(x.index) = y
              case _ =>
          }
        if partner.isEmpty then
          Nil
        else
          val carried = conds
            .filter(c =>
              c.refs.nonEmpty && c.refs.forall(id => partner.contains(id.value)) &&
                isDeterministic(c.expr) && !isNotNullKey(c.expr)
            )
            .map(_.substitute(r => partner(r.index)))
          if carried.nonEmpty then
            ctx.fired(Optimization.JoinKeyFilters.name)
          carried

    private def withChild(p: RuntimePlan, child: RuntimePlan): RuntimePlan = p.withChildren(
      Seq(child)
    )

    /**
      * Rows of p for which the conjuncts are true. A conjunct that is true whatever the row (a
      * comparison of constants a projection substituted) is dropped
      */
    private def filter(p: RuntimePlan, conds: List[Conjunct]): RuntimePlan =
      // A deterministic conjunct the filter p already applies, or that comes twice, is dropped: a
      // conjunct carried across join keys may repeat one. Two random() conjuncts both stay. The
      // comparison is structural, since a signature omits what a RowFn closes over (tuple_in)
      val seen = mutable.ListBuffer.empty[VExpr]
      p match
        case Filter(_, cond, _) if enabled =>
          seen ++= conjuncts(cond.expr)
        case _ =>
      val kept = conds.filterNot { c =>
        val repeated =
          enabled && isDeterministic(c.expr) && {
            val r = seen.contains(c.expr)
            if !r then
              seen += c.expr
            r
          }
        (enabled && isTrue(c.expr)) || repeated
      }
      if kept.isEmpty then
        p
      else
        val counts = kept.foldLeft(Map.empty[String, Int])(_ ++ _.counts)
        Filter(p, BExpr(kept.map(_.expr).reduce(AndExpr(_, _))), counts)

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

  /** `key is not null`, which a hash join already implies for both inputs' keys */
  private def isNotNullKey(e: VExpr): Boolean =
    e match
      case IsNullExpr(_: ColRef, true) =>
        true
      case _ =>
        false

  /** Whether a join key of type t can carry conjuncts: equal keys are equal values under `=` */
  private def carriesKey(t: PType): Boolean =
    t match
      case PType.Float64 | PType.AnyT | PType.Null =>
        false
      case _ =>
        true

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

  /** Whether a row-independent conjunct is true; one that fails to evaluate is kept as a filter */
  private def isTrue(e: VExpr): Boolean =
    e.isConstant && isDeterministic(e) && (
      try
        Kernels.scalarOf(e).contains(true)
      catch
        case NonFatal(_) =>
          false
    )

  /**
    * The conditions implied by every one of `filters`, the conditions of the readers of a shared
    * CTE over its body's columns, which the body can apply for all of them:
    *
    *   - the conjuncts every filter has, and
    *   - for each column, the `or` of each filter's conjuncts over that column alone, when every
    *     filter has one (`d_year = 2001` and `d_year = 2002` give
    *     `d_year = 2001 or d_year = 2002`).
    *
    * Conjuncts over several columns that not every filter has stay with their readers. Each reader
    * keeps its own filter, so the body only needs to keep the rows some reader keeps.
    */
  def shared(filters: Seq[JoinCondition]): Seq[JoinCondition] =
    if filters.isEmpty then
      Nil
    else
      // Each filter's deterministic conjuncts, with their literal counts
      val branches = filters.map { f =>
        conjuncts(f.cond.expr)
          .filter(isDeterministic)
          .map(c => JoinCondition(BExpr(c), f.literalCounts))
      }
      // Conjuncts compare structurally: a signature omits what a RowFn closes over (tuple_in's
      // subquery rows), so two readers' different conjuncts can share one
      val common    = branches.map(_.map(_.cond.expr).toSet).reduce(_ intersect _)
      val inAll     = branches.head.filter(c => common.contains(c.cond.expr))
      val columns   = branches.flatMap(_.flatMap(_.cond.refs)).distinct.sortBy(_.value)
      val perColumn = columns.flatMap { col =>
        val parts = branches.map(
          _.filter(c => !common.contains(c.cond.expr) && c.cond.refs == Set(col))
        )
        if parts.exists(_.isEmpty) then
          None
        else
          // One disjunct per distinct conjunction, in reader order
          val disjuncts = parts
            .map(cs => cs.map(_.cond.expr).reduce(AndExpr(_, _)) -> cs)
            .distinctBy(_._1)
          val e      = disjuncts.map(_._1).reduce(OrExpr(_, _))
          val counts = disjuncts.flatMap(_._2.flatMap(_.literalCounts)).toMap
          Some(JoinCondition(BExpr(e), counts))
      }
      inAll ++ perColumn

  private[runtime] def isDeterministic(e: VExpr): Boolean =
    e match
      case f: RowFn if ScalarFunctions.volatileFunctions.contains(f.name) =>
        false
      case other =>
        other.children.forall(isDeterministic)

  /** The conjuncts of a condition, as [[FactorOr]] splits them */
  private def conjuncts(e: VExpr): List[VExpr] = FactorOr.conjuncts(e)

  /**
    * The literal counts of a rewritten condition: the counts of `from`'s `and`/`or` sub-conditions,
    * keyed by the signatures of the matching sub-conditions of `to`
    */
  private[runtime] def remapCounts(
      from: VExpr,
      to: VExpr,
      counts: Map[String, Int]
  ): Map[String, Int] =
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

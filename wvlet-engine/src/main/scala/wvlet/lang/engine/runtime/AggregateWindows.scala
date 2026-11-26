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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.VExprs

import scala.collection.mutable

/**
  * A join of rows O with a per-key aggregate of the same rows becomes a window aggregate over O
  * (`aggregate_windows`, the WinMagic rewrite of a correlated subquery). TPC-H q17 compares each
  * `lineitem ⋈ part` row with `avg(l_quantity)` over the lineitem rows of its part, and q2 each
  * `part ⋈ partsupp ⋈ supplier ⋈ nation ⋈ region` row with `min(ps_supplycost)` over the
  * `partsupp ⋈ supplier ⋈ nation ⋈ region` rows of its part. Decorrelated, the subquery reads the
  * table again, aggregates it by the key, and joins back. Here the aggregate is computed over O's
  * own rows, partitioned by the key, and the table is read once.
  *
  * The rewrite applies when O is the aggregation's input I joined with other relations E only by
  * equalities between I's group key g and E's columns, on top of E's own conditions: O's rows of a
  * key v are then I's rows of v, each repeated once per row of E of key v. Every row of a key is
  * repeated alike, so a min, max, or avg over them is the aggregate over I's rows of v. Each I leaf
  * (a scan, or a subplan without joins) must match an O leaf reading the same rows, and every
  * condition of I must be a condition of O, so that O doesn't keep fewer of I's rows; the other
  * conditions of O read only E, or equate g with E. Every row of O then has a group in the
  * aggregation, so the join, inner or left, keeps each row once; a semi join filtering I by a
  * relation R ([[GroupKeySemiJoins]]) drops only groups whose key no row of O has, when R is a
  * subplan of O and its key equals g in O.
  *
  * A join pair on an aggregated value (q2's `ps_supplycost = min(ps_supplycost)`) becomes a filter
  * over the window; with such pairs only an inner join is rewritten. g must be equated in O, so
  * that no row of O has a NULL key, which the join would drop.
  */
object AggregateWindows extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.AggregateWindows.name

  /** Aggregates whose value over a group doesn't change when every row is repeated alike */
  private val repeatInsensitive = Set("min", "max", "avg")

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if !ctx.options.isEnabled(Optimization.AggregateWindows) then
      p
    else
      ctx.newColumnId match
        case Some(newId) =>
          walk(p, newId, ctx)
        case None =>
          p

  private def walk(p: RuntimePlan, newId: () => ColumnId, ctx: OptimizerContext): RuntimePlan =
    val children = p.children
    val next     = children.map(walk(_, newId, ctx))
    val node     =
      if next.corresponds(children)(_ eq _) then
        p
      else
        p.withChildren(next)
    node match
      case j @ Join(left, right, kind, lk, rk, None, _)
          if lk.nonEmpty && (kind == JoinKind.Inner || kind == JoinKind.Left) =>
        val rewritten = rewrite(left, right, lk, rk, kind == JoinKind.Inner, newId)
          .map(w => (w, left.output ++ right.output))
          .orElse(
            if kind == JoinKind.Inner then
              rewrite(right, left, rk, lk, inner = true, newId).map(w =>
                (w, left.output ++ right.output)
              )
            else
              None
          )
        rewritten match
          case Some((w, out)) =>
            ctx.fired(name)
            Project(w, out.map(c => BExpr(ref(c))), out)
          case None =>
            node
      case other =>
        other

  private def ref(c: Column): ColRef = ColRef(c.id.value, c.ptype, c.name)

  /** A condition of an inner join tree: an equality of two columns, or any other condition */
  private def eqPair(e: VExpr): Option[(Int, Int)] =
    e match
      case Compare(CmpOp.Eq, a: ColRef, b: ColRef) =>
        Some((a.index, b.index))
      case _ =>
        None

  /** Whether two conditions are the same, equalities in either orientation */
  private def sameCond(a: VExpr, b: VExpr): Boolean =
    (eqPair(a), eqPair(b)) match
      case (Some((a1, a2)), Some((b1, b2))) =>
        (a1 == b1 && a2 == b2) || (a1 == b2 && a2 == b1)
      case _ =>
        a == b

  /**
    * The leaves of an inner join tree (subplans without inner joins or filters on top) and its
    * conditions
    */
  private def flatten(
      p: RuntimePlan,
      leaves: mutable.ArrayBuffer[RuntimePlan],
      conds: mutable.ArrayBuffer[VExpr]
  ): Boolean =
    p match
      case Join(l, r, JoinKind.Inner, lk, rk, residual, _) =>
        lk.indices.foreach(i => conds += Compare(CmpOp.Eq, lk(i).expr, rk(i).expr))
        residual.foreach(e => conds ++= FactorOr.conjuncts(e.expr))
        flatten(l, leaves, conds) && flatten(r, leaves, conds)
      case InnerJoins(inputs, cs) =>
        cs.foreach(c => conds ++= FactorOr.conjuncts(c.cond.expr))
        inputs.forall(flatten(_, leaves, conds))
      case Filter(child, cond, _) =>
        if !PushDownFilters.isDeterministic(cond.expr) then
          false
        else
          conds ++= FactorOr.conjuncts(cond.expr)
          flatten(child, leaves, conds)
      case other =>
        leaves += other
        true

  /**
    * The ids of O leaf `o`'s columns that read the values of I leaf `i`'s columns, when `o` reads
    * every row `i` reads (and the same rows)
    */
  private def matchLeaf(i: RuntimePlan, o: RuntimePlan): Option[Map[Int, Int]] =
    (i, o) match
      case (Scan(is, icols, iread, ifilter), Scan(os, ocols, oread, ofilter))
          if is.key == os.key && ifilter == ofilter =>
        val oPos = ocols.indices.map(k => oread.map(_(k)).getOrElse(k) -> ocols(k)).toMap
        val m    = icols
          .indices
          .map { k =>
            val pos = iread.map(_(k)).getOrElse(k)
            oPos
              .get(pos)
              .filter(_.ptype == icols(k).ptype)
              .map(oc => icols(k).id.value -> oc.id.value)
          }
        Option.when(m.forall(_.nonEmpty))(m.flatten.toMap)
      case _ =>
        for
          ik <- ShareSubplans.key(i)
          ok <- ShareSubplans.key(o)
          if ik == ok && i.output.size == o.output.size
        yield i.output.indices.map(k => i.output(k).id.value -> o.output(k).id.value).toMap

  /** An assignment of distinct O leaves to the I leaves, with the column id map it gives */
  private def assign(
      is: List[RuntimePlan],
      os: IndexedSeq[RuntimePlan],
      used: Set[Int],
      acc: Map[Int, Int]
  ): Option[(Map[Int, Int], Set[Int])] =
    is match
      case Nil =>
        Some((acc, used))
      case i :: rest =>
        os.indices
          .iterator
          .filterNot(used.contains)
          .flatMap(k => matchLeaf(i, os(k)).flatMap(m => assign(rest, os, used + k, acc ++ m)))
          .nextOption()

  private def rename(e: VExpr, m: ColRef => Option[VExpr]): Option[VExpr] =
    var ok = true
    val r  = VExprs.mapRefs(
      e,
      c =>
        m(c) match
          case Some(x) =>
            x
          case None =>
            ok = false
            c
    )
    Option.when(ok)(r)

  /**
    * The aggregation under the projections of b, with b's output columns as expressions over the
    * aggregation's columns
    */
  private def peel(b: RuntimePlan): Option[(Aggregate, Map[Int, VExpr])] =
    b match
      case a: Aggregate =>
        Some((a, a.columns.map(c => c.id.value -> (ref(c): VExpr)).toMap))
      case Project(child, exprs, cols) =>
        peel(child).flatMap { (a, m) =>
          val es = exprs.map(e => rename(e.expr, r => m.get(r.index)))
          Option.when(es.forall(_.nonEmpty))(
            (a, cols.indices.map(k => cols(k).id.value -> es(k).get).toMap)
          )
        }
      case _ =>
        None

  /** The column of `p` that semi join key `c` reads, under p's projections, and the node below */
  private def underProjections(p: RuntimePlan, c: Int): Option[(RuntimePlan, Int)] =
    p match
      case Project(child, exprs, cols) =>
        val k = cols.indexWhere(_.id.value == c)
        if k < 0 then
          None
        else
          exprs(k).expr match
            case r: ColRef =>
              underProjections(child, r.index)
            case _ =>
              None
      case other =>
        Option.when(other.output.exists(_.id.value == c))((other, c))

  private def contains(p: RuntimePlan, node: RuntimePlan): Boolean =
    p == node || p.children.exists(contains(_, node))

  /**
    * A window over o replacing its join with b on `oKeys` = `bKeys`, with b's output columns
    * computed over it (o's columns, then b's), or None when the rewrite doesn't apply
    */
  private def rewrite(
      o: RuntimePlan,
      b: RuntimePlan,
      oKeys: IndexedSeq[BExpr],
      bKeys: IndexedSeq[BExpr],
      inner: Boolean,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    for
      (agg, bOut) <- peel(b)
      if agg.keys.nonEmpty && agg.keys.forall(_.expr.isInstanceOf[ColRef])
      if agg
        .calls
        .forall(c =>
          repeatInsensitive.contains(c.name.toLowerCase) && !c.distinct && c.filter.isEmpty &&
            c.order.isEmpty && c.args.forall(a => PushDownFilters.isDeterministic(a.expr))
        )
      groupKeys = agg.keys.map(_.expr.asInstanceOf[ColRef].index)
      (input, semi) <-
        agg.child match
          case Join(i, r, JoinKind.Semi, lk, rk, None, _) =>
            val cols = lk
              .indices
              .flatMap(k =>
                (lk(k).expr, rk(k).expr) match
                  case (l: ColRef, s: ColRef) if groupKeys.contains(l.index) =>
                    Some((l.index, s.index))
                  case _ =>
                    None
              )
            Option.when(cols.size == lk.size)((i, Some((cols, r))))
          case other =>
            Some((other, None))
      iLeaves = mutable.ArrayBuffer.empty[RuntimePlan]
      iConds  = mutable.ArrayBuffer.empty[VExpr]
      if flatten(input, iLeaves, iConds)
      oLeaves = mutable.ArrayBuffer.empty[RuntimePlan]
      oConds  = mutable.ArrayBuffer.empty[VExpr]
      if flatten(o, oLeaves, oConds)
      (idMap, used) <- assign(iLeaves.toList, oLeaves.toIndexedSeq, Set.empty, Map.empty)
      mapped        <-
        iConds
          .map(c => rename(c, r => idMap.get(r.index).map(oi => r.copy(index = oi))))
          .foldLeft(Option(List.empty[VExpr]))((acc, c) => acc.flatMap(l => c.map(_ :: l)))
      result <- build(
        o,
        oLeaves.toIndexedSeq,
        oConds.toIndexedSeq,
        used,
        idMap,
        mapped,
        agg,
        groupKeys,
        semi,
        bOut,
        b.output,
        oKeys,
        bKeys,
        inner,
        newId
      )
    yield result

  private def build(
      o: RuntimePlan,
      oLeaves: IndexedSeq[RuntimePlan],
      oConds: IndexedSeq[VExpr],
      used: Set[Int],
      idMap: Map[Int, Int],
      mapped: List[VExpr],
      agg: Aggregate,
      groupKeys: IndexedSeq[Int],
      semi: Option[(IndexedSeq[(Int, Int)], RuntimePlan)],
      bOut: Map[Int, VExpr],
      bColumns: IndexedSeq[Column],
      oKeys: IndexedSeq[BExpr],
      bKeys: IndexedSeq[BExpr],
      inner: Boolean,
      newId: () => ColumnId
  ): Option[RuntimePlan] =
    // Every condition of I is a condition of O
    val matched  = mutable.BitSet.empty
    val allFound = mapped.forall { c =>
      oConds.indices.find(k => !matched.contains(k) && sameCond(oConds(k), c)) match
        case Some(k) =>
          matched += k
          true
        case None =>
          false
    }
    if !allFound then
      return None
    val iIds = used.iterator.flatMap(k => oLeaves(k).output.map(_.id.value)).toSet
    val eIds =
      oLeaves.indices.filterNot(used.contains).flatMap(k => oLeaves(k).output.map(_.id.value)).toSet
    val keyIds = groupKeys.map(g => idMap.get(g)).flatten
    if keyIds.size != groupKeys.size then
      return None

    def refs(e: VExpr): Set[Int] =
      val acc = mutable.BitSet.empty
      VExprs.collectRefs(e, acc)
      acc.toSet

    // The columns equated with each group key in O
    val equal    = keyIds.map(k => mutable.Set(k))
    val othersOk = oConds
      .indices
      .filterNot(matched.contains)
      .forall { k =>
        val c = oConds(k)
        val r = refs(c)
        if r.subsetOf(eIds) then
          true
        else
          eqPair(c) match
            case Some((a, x)) if keyIds.contains(a) && eIds.contains(x) =>
              equal(keyIds.indexOf(a)) += x
              true
            case Some((x, a)) if keyIds.contains(a) && eIds.contains(x) =>
              equal(keyIds.indexOf(a)) += x
              true
            case _ =>
              false
      }
    if !othersOk then
      return None
    // A NULL key would be dropped by the join: every key must be equated in O, by a condition of
    // I or one equating it with E
    val equated = keyIds
      .indices
      .forall(j =>
        equal(j).size > 1 ||
          oConds.exists(c => eqPair(c).exists((a, x) => a == keyIds(j) || x == keyIds(j)))
      )
    if !equated then
      return None
    // The semi join drops only keys O doesn't have
    val semiOk = semi.forall { (pairs, r) =>
      pairs.forall { (g, s) =>
        val j = groupKeys.indexOf(g)
        underProjections(r, s).exists((node, sc) => equal(j).contains(sc) && contains(o, node))
      }
    }
    if !semiOk then
      return None
    // The join pairs: on a group key, or on an aggregated value (a filter over the window)
    val aggKeyCols = agg.columns.take(agg.keys.size).map(_.id.value)
    val pairs      = oKeys
      .indices
      .map { k =>
        val keyOf =
          bKeys(k).expr match
            case c: ColRef =>
              bOut
                .get(c.index)
                .collect { case r: ColRef =>
                  aggKeyCols.indexOf(r.index)
                }
                .filter(_ >= 0)
            case _ =>
              None
        (k, keyOf)
      }
    val keyCovered = keyIds
      .indices
      .forall(j =>
        pairs.exists((k, keyOf) =>
          keyOf.contains(j) && (
            oKeys(k).expr match
              case c: ColRef =>
                equal(j).contains(c.index)
              case _ =>
                false
          )
        )
      )
    val valuePairs = pairs.filter(_._2.isEmpty).map(_._1)
    if !keyCovered || (!inner && valuePairs.nonEmpty) then
      return None

    // The window calls, over O's columns
    val keyRefs = keyIds.indices.map(j => BExpr(ColRef(keyIds(j), agg.keys(j).ptype, "")))
    val calls   = agg
      .calls
      .map(c =>
        c.args.map(a => rename(a.expr, r => idMap.get(r.index).map(oi => r.copy(index = oi))))
      )
    if calls.exists(_.exists(_.isEmpty)) then
      return None
    val callCols = agg.columns.drop(agg.keys.size)
    val winCols  = callCols.map(c => Column(newId(), c.name, c.ptype, c.qualifiers))
    val window   = Window(
      o,
      agg
        .calls
        .indices
        .map(k =>
          WindowFunction(
            agg.calls(k).name,
            calls(k).map(a => BExpr(a.get)),
            keyRefs,
            Nil,
            None,
            ignoreNulls = false,
            agg.calls(k).ptype
          )
        ),
      winCols
    )
    // The aggregation's columns over the window's
    val aggToWin: Map[Int, VExpr] = (
      aggKeyCols.indices.map(j => aggKeyCols(j) -> (keyRefs(j).expr: VExpr)) ++
        callCols.indices.map(k => callCols(k).id.value -> (ref(winCols(k)): VExpr))
    ).toMap
    val bExprs = bColumns.map(c =>
      bOut.get(c.id.value).flatMap(e => rename(e, r => aggToWin.get(r.index)))
    )
    if bExprs.exists(_.isEmpty) then
      return None
    val withB = Project(
      window,
      o.output.map(c => BExpr(ref(c))) ++ bExprs.map(e => BExpr(e.get)),
      o.output ++ bColumns
    )
    val filtered =
      if valuePairs.isEmpty then
        withB
      else
        val conds = valuePairs.map(k => Compare(CmpOp.Eq, oKeys(k).expr, bKeys(k).expr): VExpr)
        Filter(withB, BExpr(conds.reduce((a, b) => AndExpr(a, b))))
    Some(filtered)

  end build

end AggregateWindows

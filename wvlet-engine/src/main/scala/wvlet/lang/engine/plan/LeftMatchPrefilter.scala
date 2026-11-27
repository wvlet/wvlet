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
package wvlet.lang.engine.plan

import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Filter an aggregation's input by the builds of the LEFT joins its output probes, when the filter
  * above those joins keeps only rows that one of them matched (`left_match_prefilter`).
  *
  * The shape: `Filter(c) ← LEFT joins J1..Jn ← Project* ← HashAgg`, where a conjunct of `c` is an
  * OR whose every part reads the build columns of one join only and is not true when they are all
  * NULL (TPC-DS q78: `coalesce(ws_qty, 0) > 0 or coalesce(cs_qty, 0) > 0`). A row none of those
  * joins matched then fails `c`. When their probe keys are group keys of the aggregation, a group
  * whose key no such build has is dropped as a whole, so its input rows can be dropped before the
  * aggregation: a [[FilterOp]] over [[BuildKeyMatch]] goes between the aggregation and its input.
  * The joins and `c` stay, so the result is the same; the joins also get a Bloom filter of their
  * keys (with `probe_miss_filter`), since the new filter's probes mostly miss.
  *
  * Runs right after [[ColumnPruner]], which makes the operators it rebuilds here from their
  * constructors, before the passes that set their options. It rebuilds the operators above a
  * rewrite only through the types it knows (filters, projections, sorts, limits, hash joins and
  * aggregations), and leaves other subtrees as they are.
  */
object LeftMatchPrefilter:

  /** The plan with the prefilters added */
  def apply(op: Operator): Operator = rewrite(op)

  /** Make the prefilters of a plan leave their aggregations' presizing (no `prefilter_presize`) */
  def disablePresize(op: Operator): Unit =
    op match
      case f: FilterOp =>
        f.cond match
          case m: BuildKeyMatch =>
            m.sizesAggregation = false
          case _ =>
      case _ =>
    op.children.foreach(disablePresize)
    op.sharedInputs.foreach(disablePresize)

  /** Whether a join's probes feed a prefilter, so they mostly miss */
  def feedsPrefilter(j: HashJoinOp): Boolean = j.prefilters.nonEmpty

  private def rewrite(op: Operator): Operator =
    op match
      case f: FilterOp =>
        prefilter(f).getOrElse {
          val c = rewrite(f.child)
          if c eq f.child then
            f
          else
            FilterOp(c, f.cond, f.keep)
        }
      case p: ProjectOp =>
        val c = rewrite(p.child)
        if c eq p.child then
          p
        else
          ProjectOp(c, p.exprs, p.schema)
      case s: SortOp =>
        val c = rewrite(s.child)
        if c eq s.child then
          s
        else
          SortOp(c, s.keys, s.limit)
      case l: LimitOp =>
        val c = rewrite(l.child)
        if c eq l.child then
          l
        else
          LimitOp(c, l.limit, l.offset)
      case a: HashAggOp =>
        val c = rewrite(a.child)
        if c eq a.child then
          a
        else
          HashAggOp(c, a.keys, a.aggs, a.schema, a.expectedGroups)
      case j: HashJoinOp =>
        val l = rewrite(j.left)
        val r = rewrite(j.right)
        if (l eq j.left) && (r eq j.right) then
          j
        else
          copyJoin(j, l, r)
      case other =>
        other

  private def copyJoin(j: HashJoinOp, left: Operator, right: Operator): HashJoinOp = HashJoinOp(
    left,
    right,
    j.leftKeys,
    j.rightKeys,
    j.kind,
    j.residual,
    j.schema,
    runtimeFilters = j.runtimeFilters,
    nullsEqual = j.nullsEqual,
    exclude = j.exclude
  )

  /** A LEFT join whose output is its probe columns then its build columns */
  private def plainLeft(op: Operator): Option[HashJoinOp] =
    op match
      case j: HashJoinOp
          if j.kind == JoinKind.Left && j.residual.isEmpty && j.exclude.isEmpty && !j.nullsEqual &&
            j.prefilters.isEmpty && j.schema.size == j.left.schema.size + j.right.schema.size =>
        Some(j)
      case _ =>
        None

  private def prefilter(f: FilterOp): Option[Operator] =
    // The LEFT joins under the filter, top first, down to the probe input they share
    val joins = List.unfold(f.child)(op => plainLeft(op).map(j => (j, j.left)))
    if joins.isEmpty then
      return None
    val base  = joins.last.left
    val width = base.schema.size
    // The joins whose probe keys are columns of the base, with their build columns' positions in
    // the filter's input
    val candidates = joins.flatMap { j =>
      val keys = j
        .leftKeys
        .collect {
          case c: ColRef if c.index < width =>
            c.index
        }
      Option.when(keys.size == j.leftKeys.size) {
        (j, keys, j.left.schema.size until j.schema.size)
      }
    }
    if candidates.isEmpty then
      return None
    val nulls = nullRow(f.child.schema)
    // The joins one conjunct needs a match of, if any
    val matched = conjuncts(f.cond)
      .iterator
      .flatMap { c =>
        val parts = disjuncts(c).map { d =>
          val refs = mutable.BitSet()
          VExprs.collectRefs(d, refs)
          candidates
            .find((_, _, cols) => refs.nonEmpty && refs.forall(cols.contains))
            .filter(_ => notTrueOnNulls(d, nulls))
        }
        Option.when(parts.forall(_.isDefined))(parts.flatten.distinctBy(_._1))
      }
    if !matched.hasNext then
      return None
    val needed = matched.next()
    pushBelowAgg(base, needed.map(_._2)).map { (newBase, sources) =>
      // Rebuild the joins bottom up over the new base, binding the needed ones to their sources
      val bySource = needed.map(_._1).zip(sources).toMap
      val newChild =
        joins
          .reverse
          .foldLeft(newBase) { (left, j) =>
            val nj = copyJoin(j, left, rewrite(j.right))
            bySource.get(j).foreach(s => BuildKeyMatch.bind(nj, Seq(s)))
            nj
          }
      FilterOp(newChild, f.cond, f.keep)
    }

  end prefilter

  /**
    * The probe input with a [[BuildKeyMatch]] filter below its aggregation over the probe keys
    * `keys` (positions of the input's columns), and the filter's sources in the order of `keys`;
    * None unless the input is projections of columns over an aggregation whose group keys hold the
    * probe keys
    */
  private def pushBelowAgg(
      op: Operator,
      keys: List[IndexedSeq[Int]]
  ): Option[(Operator, List[BuildKeyMatch.Source])] =
    op match
      case p: ProjectOp =>
        val cols = keys.map(_.map(p.exprs(_)))
        if cols.forall(_.forall(_.isInstanceOf[ColRef])) then
          pushBelowAgg(p.child, cols.map(_.map(_.asInstanceOf[ColRef].index))).map { (c, s) =>
            (ProjectOp(c, p.exprs, p.schema), s)
          }
        else
          None
      case a: HashAggOp if a.keys.nonEmpty && keys.forall(_.forall(_ < a.keys.size)) =>
        val sources = keys.map(k => BuildKeyMatch.Source(k.map(a.keys(_))))
        val input   = FilterOp(rewrite(a.child), BuildKeyMatch(sources.toIndexedSeq))
        Some((HashAggOp(input, a.keys, a.aggs, a.schema, a.expectedGroups), sources))
      case _ =>
        None

  private def conjuncts(e: VExpr): List[VExpr] =
    e match
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        List(other)

  private def disjuncts(e: VExpr): List[VExpr] =
    e match
      case OrExpr(l, r) =>
        disjuncts(l) ++ disjuncts(r)
      case other =>
        List(other)

  /** One row of NULLs of a schema */
  private def nullRow(schema: Schema): Batch = Batch(
    schema.fields.map(f => Vec.constant(f.ptype, null, 1)).toArray,
    1
  )

  /** Whether `e` is NULL or false over a row of NULLs (false when it can't be evaluated) */
  private def notTrueOnNulls(e: VExpr, nulls: Batch): Boolean =
    try
      e.eval(nulls).get(0) != true
    catch
      case NonFatal(_) =>
        false

end LeftMatchPrefilter

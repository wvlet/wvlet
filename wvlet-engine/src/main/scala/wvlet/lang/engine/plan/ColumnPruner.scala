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

/**
  * Removes columns that no operator above needs, so that scans read only the referenced columns.
  *
  * `prune(op, required)` returns an operator whose output is the columns of `op` at the returned
  * (ascending) positions, a superset of `required`. Expressions of the operators above are
  * rewritten through the position mapping. Operators this pass doesn't know keep all their input
  * columns, so pruning stops below them.
  */
object ColumnPruner:

  /**
    * What the pass does: drop unused columns (`pruneColumns`), and attach the filters directly over
    * Parquet scans for row-group skipping (`scanFilters`), also on decimal and floating-point
    * columns with `numericScanFilters`
    */
  private case class Mode(pruneColumns: Boolean, scanFilters: Boolean, numericScanFilters: Boolean)

  /**
    * Prune a whole plan. The output schema is unchanged. With `pruneColumns = false`, every column
    * is kept and only the scan filters are attached
    */
  def apply(
      op: Operator,
      pruneColumns: Boolean = true,
      scanFilters: Boolean = true,
      numericScanFilters: Boolean = true
  ): Operator =
    given Mode = Mode(pruneColumns, scanFilters, numericScanFilters)
    val all    = (0 until op.schema.size).toArray
    try
      if !pruneColumns && !scanFilters then
        op
      else
        prune(op, all)._1
    catch
      // An expression this pass can't rewrite (VExprs.remap): run the plan unpruned
      case e: IllegalStateException =>
        op

  private def all(op: Operator): Array[Int] = (0 until op.schema.size).toArray

  private def identity(op: Operator): (Operator, Array[Int]) = (op, all(op))

  /** old position -> new position (-1: dropped) */
  private def mapping(kept: Array[Int], width: Int): Array[Int] =
    val m = Array.fill(width)(-1)
    kept.indices.foreach(i => m(kept(i)) = i)
    m

  private def sorted(cols: mutable.BitSet): Array[Int] = cols.toArray.sorted

  private def prune(op: Operator, needed: Array[Int])(using mode: Mode): (Operator, Array[Int]) =
    val required =
      if mode.pruneColumns then
        needed
      else
        all(op)
    op match
      case s: ScanOp =>
        if required.length == s.schema.size then
          identity(s)
        else
          // Keep at least one column so that the scan still carries the row count: one its row
          // filter decodes anyway, if any
          val keep =
            if required.isEmpty then
              val filtered = s.rowFilter.map(_.columnNames.toSet).getOrElse(Set.empty)
              Array(s.schema.fields.indices.find(i => filtered(s.sourceColumnName(i))).getOrElse(0))
            else
              required
          if s.schema.size == 0 then
            identity(s)
          else
            (s.project(keep.toIndexedSeq), keep)

      case f: FilterOp =>
        // Input position of each output column
        val src: Int => Int =
          if f.keep == null then
            i => i
          else
            f.keep(_)
        val need = mutable.BitSet(required.map(src)*)
        VExprs.collectRefs(f.cond, need)
        val input =
          f.child match
            // A scan of the runtime plan has its filter already (PushScanFilters)
            case s: ScanOp if mode.scanFilters && s.source.supportsFilter && s.filter.isEmpty =>
              ScanFilters
                .toParquet(
                  f.cond,
                  i => Some(s.sourceColumnName(i)).filter(s.source.canFilterOn),
                  mode.numericScanFilters
                )
                .map(s.withFilter)
                .getOrElse(s)
            case other =>
              other
        val (child, kept) = prune(input, sorted(need))
        val m             = mapping(kept, f.child.schema.size)
        val cond          = VExprs.remap(f.cond, m)
        // Only the required columns are output: those only the condition reads aren't copied
        val keep = required.map(r => m(src(r)))
        if keep.length == kept.length then
          (FilterOp(child, cond), required)
        else
          (FilterOp(child, cond, keep), required)

      case p: ProjectOp =>
        val keep = required
        val need = mutable.BitSet()
        keep.foreach(i => VExprs.collectRefs(p.exprs(i), need))
        val (child, kept) = prune(p.child, sorted(need))
        val m             = mapping(kept, p.child.schema.size)
        val exprs         = keep.map(i => VExprs.remap(p.exprs(i), m)).toIndexedSeq
        (ProjectOp(child, exprs, Schema(keep.map(p.schema(_)).toIndexedSeq)), keep)

      case l: LimitOp =>
        val (child, kept) = prune(l.child, required)
        (LimitOp(child, l.limit, l.offset), kept)

      case s: SortOp =>
        val need = mutable.BitSet(required*)
        s.keys.foreach(k => VExprs.collectRefs(k.expr, need))
        val (child, kept) = prune(s.child, sorted(need))
        val m             = mapping(kept, s.child.schema.size)
        val keys          = s.keys.map(k => k.copy(expr = VExprs.remap(k.expr, m)))
        (SortOp(child, keys, s.limit), kept)

      case a: HashAggOp =>
        val need = mutable.BitSet()
        a.keys.foreach(VExprs.collectRefs(_, need))
        a.aggs
          .foreach { c =>
            c.args.foreach(VExprs.collectRefs(_, need))
            c.filter.foreach(VExprs.collectRefs(_, need))
          }
        val (child, kept) = prune(a.child, sorted(need))
        val m             = mapping(kept, a.child.schema.size)
        val keys          = a.keys.map(VExprs.remap(_, m))
        val aggs          = a
          .aggs
          .map(c =>
            c.copy(args = c.args.map(VExprs.remap(_, m)), filter = c.filter.map(VExprs.remap(_, m)))
          )
        identity(HashAggOp(child, keys, aggs, a.schema, a.expectedGroups))

      case d: DistinctOp =>
        val (child, _) = prune(d.child, all(d.child))
        identity(DistinctOp(child))

      case u: UnionAllOp =>
        // Output position i is position i of every input: each input outputs exactly `required`
        val inputs = u
          .inputs
          .map { i =>
            val (child, kept) = prune(i, required)
            if kept.sameElements(required) then
              child
            else
              val m = mapping(kept, i.schema.size)
              ProjectOp(
                child,
                required.map(r => ColRef(m(r), i.schema(r).ptype, i.schema(r).name)).toIndexedSeq,
                Schema(required.map(i.schema(_)).toIndexedSeq)
              )
          }
        (UnionAllOp(inputs, Schema(required.map(u.schema(_)).toIndexedSeq)), required)

      case s: SetOpOp =>
        // INTERSECT / EXCEPT compare whole rows: every column is needed, but pruning continues below
        identity(
          SetOpOp(
            prune(s.left, all(s.left))._1,
            prune(s.right, all(s.right))._1,
            s.isIntersect,
            s.all,
            s.schema
          )
        )

      case w: WindowOp =>
        // Output: the input columns, then one column per call (all kept)
        val width = w.child.schema.size
        val need  = mutable.BitSet(required.filter(_ < width)*)
        w.calls
          .foreach { c =>
            c.args.foreach(VExprs.collectRefs(_, need))
            c.partitionBy.foreach(VExprs.collectRefs(_, need))
            c.orderBy.foreach(k => VExprs.collectRefs(k.expr, need))
          }
        val (child, kept) = prune(w.child, sorted(need))
        val m             = mapping(kept, width)
        val calls         = w
          .calls
          .map(c =>
            c.copy(
              args = c.args.map(VExprs.remap(_, m)),
              partitionBy = c.partitionBy.map(VExprs.remap(_, m)),
              orderBy = c.orderBy.map(k => k.copy(expr = VExprs.remap(k.expr, m)))
            )
          )
        val schema = Schema(kept.map(w.child.schema(_)).toIndexedSeq ++ w.schema.fields.drop(width))
        (
          WindowOp(
            child,
            calls,
            schema,
            topN = w.topN,
            runningKernels = w.runningKernels,
            sortKernels = w.sortKernels,
            sharePartitions = w.sharePartitions,
            streamTopN = w.streamTopN
          ),
          kept ++ (width until w.schema.size)
        )

      case j: HashJoinOp =>
        pruneHashJoin(j, required)

      case j: NestedLoopJoinOp =>
        pruneNestedLoopJoin(j, required)

      case other =>
        identity(other)
    end match

  end prune

  private def leftOnly(kind: JoinKind): Boolean = kind.outputsProbeOnly

  /** Split join output positions into the left and right input positions */
  private def splitRequired(
      required: Iterable[Int],
      leftWidth: Int,
      leftNeed: mutable.BitSet,
      rightNeed: mutable.BitSet
  ): Unit = required.foreach { i =>
    if i < leftWidth then
      leftNeed += i
    else
      rightNeed += (i - leftWidth)
  }

  private def outputWidthMatches(op: Operator, left: Operator, right: Operator, kind: JoinKind) =
    val expected =
      if leftOnly(kind) then
        left.schema.size
      else if kind.outputsBuildOnly then
        right.schema.size
      else
        left.schema.size + right.schema.size
    op.schema.size == expected

  private def pruneHashJoin(j: HashJoinOp, required: Array[Int])(using
      Mode
  ): (Operator, Array[Int]) =
    if !outputWidthMatches(j, j.left, j.right, j.kind) then
      return identity(j)
    val lw        = j.left.schema.size
    val leftNeed  = mutable.BitSet()
    val rightNeed = mutable.BitSet()
    if j.kind.outputsBuildOnly then
      // Output positions are build (right) positions
      rightNeed ++= required
    else
      splitRequired(required, lw, leftNeed, rightNeed)
    j.leftKeys.foreach(VExprs.collectRefs(_, leftNeed))
    j.rightKeys.foreach(VExprs.collectRefs(_, rightNeed))
    (j.residual ++ j.exclude).foreach { r =>
      val refs = mutable.BitSet()
      VExprs.collectRefs(r, refs)
      splitRequired(refs, lw, leftNeed, rightNeed)
    }
    val (left, lKept)  = prune(j.left, sorted(leftNeed))
    val (right, rKept) = prune(j.right, sorted(rightNeed))
    val lm             = mapping(lKept, lw)
    val rm             = mapping(rKept, j.right.schema.size)
    // Positions of the joined row (left ++ right) used by the residual condition
    val joined = combinedMapping(lm, rm, lKept.length)
    val kept   =
      if leftOnly(j.kind) then
        lKept
      else if j.kind.outputsBuildOnly then
        rKept
      else
        lKept ++ rKept.map(_ + lw)
    val schema = Schema(kept.map(j.schema(_)).toIndexedSeq)
    val op     = HashJoinOp(
      left,
      right,
      j.leftKeys.map(VExprs.remap(_, lm)),
      j.rightKeys.map(VExprs.remap(_, rm)),
      j.kind,
      j.residual.map(VExprs.remap(_, joined)),
      schema,
      runtimeFilters = j.runtimeFilters,
      exclude = j.exclude.map(VExprs.remap(_, joined))
    )
    (op, kept)

  end pruneHashJoin

  private def pruneNestedLoopJoin(j: NestedLoopJoinOp, required: Array[Int])(using
      Mode
  ): (Operator, Array[Int]) =
    if !outputWidthMatches(j, j.left, j.right, j.kind) then
      return identity(j)
    val lw        = j.left.schema.size
    val leftNeed  = mutable.BitSet()
    val rightNeed = mutable.BitSet()
    splitRequired(required, lw, leftNeed, rightNeed)
    j.cond
      .foreach { c =>
        val refs = mutable.BitSet()
        VExprs.collectRefs(c, refs)
        splitRequired(refs, lw, leftNeed, rightNeed)
      }
    val (left, lKept)  = prune(j.left, sorted(leftNeed))
    val (right, rKept) = prune(j.right, sorted(rightNeed))
    val lm             = mapping(lKept, lw)
    val rm             = mapping(rKept, j.right.schema.size)
    val joined         = combinedMapping(lm, rm, lKept.length)
    val kept           =
      if leftOnly(j.kind) then
        lKept
      else
        lKept ++ rKept.map(_ + lw)
    val schema = Schema(kept.map(j.schema(_)).toIndexedSeq)
    (NestedLoopJoinOp(left, right, j.cond.map(VExprs.remap(_, joined)), j.kind, schema), kept)

  end pruneNestedLoopJoin

  private def combinedMapping(lm: Array[Int], rm: Array[Int], newLeftWidth: Int): Array[Int] =
    lm ++
      rm.map { r =>
        if r < 0 then
          -1
        else
          r + newLeftWidth
      }

end ColumnPruner

/** Column reference utilities over bound expressions */
object VExprs:
  /** Add the input positions an expression reads */
  def collectRefs(e: VExpr, acc: mutable.BitSet): Unit =
    e match
      case c: ColRef =>
        acc += c.index
      case other =>
        other.children.foreach(collectRefs(_, acc))

  /** Rewrite column positions through `m` (old -> new) */
  def remap(e: VExpr, m: Array[Int]): VExpr = mapRefs(
    e,
    c =>
      val i = m(c.index)
      if i < 0 then
        throw IllegalStateException(s"Column ${c.name} (#${c.index}) was pruned")
      if i == c.index then
        c
      else
        c.copy(index = i)
  )

  /** Replace each column reference of an expression with `f` of it */
  def mapRefs(e: VExpr, f: ColRef => VExpr): VExpr =
    e match
      case c: ColRef =>
        f(c)
      case other =>
        mapChildren(other, mapRefs(_, f))

  /**
    * The expression with each child replaced by `f` of it (leaves as they are). Throws
    * IllegalStateException for an expression type it doesn't know
    */
  def mapChildren(e: VExpr, f: VExpr => VExpr): VExpr =
    e match
      case _ if e.children.isEmpty =>
        e
      case x: RowFn =>
        x.copy(args = x.args.map(f))
      case x: CastExpr =>
        x.copy(child = f(x.child))
      case x: Arith =>
        x.copy(left = f(x.left), right = f(x.right))
      case x: FusedDecimalMul =>
        x.mapChildren(f)
      case x: Negate =>
        x.copy(child = f(x.child))
      case x: Compare =>
        x.copy(left = f(x.left), right = f(x.right))
      case x: NullSafeDistinct =>
        x.copy(left = f(x.left), right = f(x.right))
      case x: AndExpr =>
        x.copy(left = f(x.left), right = f(x.right))
      case x: OrExpr =>
        x.copy(left = f(x.left), right = f(x.right))
      case x: NotExpr =>
        x.copy(child = f(x.child))
      case x: IsNullExpr =>
        x.copy(child = f(x.child))
      case x: InList =>
        x.copy(child = f(x.child))
      case x: InExprs =>
        x.copy(child = f(x.child), list = x.list.map(f))
      case x: CaseWhen =>
        x.copy(whens = x.whens.map((c, v) => (f(c), f(v))), elseExpr = x.elseExpr.map(f))
      case x: Coalesce =>
        x.copy(args = x.args.map(f))
      case x: LikeExpr =>
        x.copy(child = f(x.child))
      case other =>
        throw IllegalStateException(s"Unsupported expression for column remapping: ${other}")
    end match

  end mapChildren

end VExprs

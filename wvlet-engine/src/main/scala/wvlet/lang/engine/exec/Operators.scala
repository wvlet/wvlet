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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import scala.collection.mutable

/**
  * A pull-based physical operator. `next()` returns the next non-empty batch, or null at the end
  */
trait Operator:
  def schema: Schema
  def next(): Batch
  def children: Seq[Operator] = Nil
  def close(): Unit           = children.foreach(_.close())
  def name: String            = getClass.getSimpleName.stripSuffix("Op")

  /**
    * The physical plan for EXPLAIN, in flow style as the logical plan printer shows it: data flows
    * down, from the scans to this operator. The first child continues the flow (`↓ op`); the other
    * inputs (join build sides, shared CTE bodies) are nested above the operator that reads them (`↙
    * op`). Adding an operator to a plan thus adds one line instead of re-indenting its inputs
    */
  def explain(indent: Int = 0): String =
    val self =
      s"${name}${details} -> [${schema.fields.map(f => s"${f.name}:${f.ptype}").mkString(", ")}]"
    val inputs = children
    val flow   = inputs.headOption.map(_.explain(indent))
    val nested = inputs.drop(1).map(_.explain(indent + 1)) ++ extraInputs(indent + 1)
    val marker =
      if nested.nonEmpty then
        "↙ "
      else if flow.nonEmpty then
        "↓ "
      else
        ""
    (flow.toSeq ++ nested :+ s"${"  " * indent}${marker}${self}").mkString("\n")

  /**
    * The EXPLAIN text, at `indent`, of inputs that are not children (e.g. a shared CTE body that
    * runs once for every reader)
    */
  protected def extraInputs(indent: Int): Seq[String] = Nil

  /**
    * Inputs that are not children because they are computed once for several readers (e.g. the
    * right input of a parallel cross join), so that a walk over a plan can reach every operator
    */
  def sharedInputs: Seq[Operator] = Nil

  protected def details: String = ""

  /** Drain every remaining batch */
  def drain(): Seq[Batch] =
    val b = Seq.newBuilder[Batch]
    var x = next()
    while x != null do
      b += x
      x = next()
    b.result()

end Operator

object Operator:
  /** Pull every row of the operator into one batch */
  def materialize(op: Operator): Batch =
    val batches = op.drain()
    if batches.isEmpty then
      Batch.empty(op.schema)
    else
      Batch.concat(op.schema, batches)

/** Batches computed up front (VALUES, materialized subqueries, in-memory tables) */
class BatchesOp(val schema: Schema, batches: Seq[Batch], label: String = "Batches")
    extends Operator:
  private val it    = batches.iterator.filter(_.size > 0)
  def next(): Batch =
    if it.hasNext then
      it.next()
    else
      null

  override def name: String = label

/** Batches produced lazily (file scans) */
class IteratorOp(val schema: Schema, mk: () => Iterator[Batch], label: String) extends Operator:
  private lazy val it       = mk()
  override def name: String = label
  def next(): Batch         =
    var b: Batch = null
    while b == null && it.hasNext do
      val x = it.next()
      if x.size > 0 then
        b = x
    b

/** One row without columns: the input of a FROM-less SELECT */
class SingleRowOp extends Operator:
  private var done   = false
  val schema: Schema = Schema.empty
  def next(): Batch  =
    if done then
      null
    else
      done = true
      Batch.ofRows(1)

/**
  * Rows of the input where `cond` is true. With `keep`, only those input columns are output (in
  * that order), so columns only the condition reads aren't copied
  */
class FilterOp(val child: Operator, val cond: VExpr, val keep: Array[Int] = null) extends Operator:
  val schema: Schema =
    if keep == null then
      child.schema
    else
      Schema(keep.toIndexedSeq.map(child.schema(_)))

  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(${cond.sig}${
        if keep == null then
          ""
        else
          s", keep=${keep.mkString(",")}"
      })"

  def next(): Batch =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val mask = Kernels.trueMask(cond.eval(b))
      val in   =
        if keep == null then
          b
        else
          b.select(keep)
      val r = in.filter(mask)
      if r.size > 0 then
        out = r
    out

end FilterOp

class ProjectOp(val child: Operator, val exprs: IndexedSeq[VExpr], val schema: Schema)
    extends Operator:
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${exprs.map(_.sig).mkString(", ")})"
  def next(): Batch                    =
    val b = child.next()
    if b == null then
      null
    else
      Batch(exprs.map(_.eval(b)).toArray, b.size)

class LimitOp(val child: Operator, val limit: Long, val offset: Long) extends Operator:
  private var skipped                  = 0L
  private var emitted                  = 0L
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${limit}, offset=${offset})"
  def next(): Batch                    =
    var out: Batch = null
    while out == null && emitted < limit do
      val b = child.next()
      if b == null then
        return null
      var from = 0
      if skipped < offset then
        val skip = math.min(offset - skipped, b.size.toLong).toInt
        skipped += skip
        from = skip
      if from < b.size then
        val take = math.min(limit - emitted, (b.size - from).toLong).toInt
        emitted += take
        out = b.slice(from, from + take)
    out

case class SortKey(expr: VExpr, ascending: Boolean, nullsFirst: Boolean)

/**
  * Full sort; with a limit only the top rows are kept. Inputs of at least `minParallelRows` rows
  * are sorted in parallel on `runner`: each of `runner.parallelism` contiguous chunks of the input
  * is sorted (and cut to the limit) as a task. The merge of the sorted runs is split at evenly
  * spaced output ranks ([[SortOp.splitRanks]]), and each range is merged and gathered as a task.
  * Ties in the merge go to the earlier chunk, so the output equals the sequential (stable) sort.
  */
class SortOp(
    val child: Operator,
    val keys: Seq[SortKey],
    val limit: Option[Long] = None,
    runner: TaskRunner = TaskRunner.Sequential,
    minParallelRows: Int = SortOp.MinParallelRows
) extends Operator:
  private var result: Iterator[Batch]  = null
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override def name: String            =
    if runner.parallelism > 1 then
      s"Sort[parallelism=${runner.parallelism}]"
    else
      "Sort"

  override protected def details =
    s"(${keys
        .map(k =>
          s"${k.expr.sig}${
              if k.ascending then
                ""
              else
                " desc"
            }"
        )
        .mkString(", ")}${limit.map(l => s", limit=${l}").getOrElse("")})"

  def next(): Batch =
    if result == null then
      result = sortAll()
    if result.hasNext then
      result.next()
    else
      null

  private def sortAll(): Iterator[Batch] =
    val all = Operator.materialize(child)
    if all.size == 0 then
      return Iterator.empty
    val keyVecs = keys.map(k => k.expr.eval(all)).toArray
    val cmp     = SortOp.comparator(keyVecs, keys)
    val n       = limit.map(l => math.min(l, all.size.toLong).toInt).getOrElse(all.size)
    if all.size < minParallelRows || runner.parallelism <= 1 then
      val sorted = SortOp.sortRange(cmp, 0, all.size, n)
      val out    = all.gather(sorted, n)
      Iterator.range(0, n, Batch.DefaultSize).map(from => out.slice(from, from + Batch.DefaultSize))
    else
      val chunks = math.min(runner.parallelism, all.size)
      val runs   = (0 until chunks).map { c =>
        val lo = (all.size.toLong * c / chunks).toInt
        val hi = (all.size.toLong * (c + 1) / chunks).toInt
        runner.submit(() => SortOp.sortRange(cmp, lo, hi, n))
      }
      val sortedRuns = runs.map(_.get()).toArray
      // Split the merge into independent output ranges (one task each) that merge and gather
      val ranges = SortOp.mergeRangeCount(n, chunks, minParallelRows)
      val cuts   = SortOp.splitRanks(sortedRuns, cmp, n, ranges)
      val tasks  = (0 until ranges).map { t =>
        runner.submit { () =>
          val part = SortOp.mergeRange(sortedRuns, cmp, cuts(t), cuts(t + 1))
          Iterator
            .range(0, part.length, Batch.DefaultSize)
            .map { from =>
              val to = math.min(part.length, from + Batch.DefaultSize)
              all.gather(java.util.Arrays.copyOfRange(part, from, to), to - from)
            }
            .toIndexedSeq
        }
      }
      tasks.iterator.flatMap(_.get())
    end if

  end sortAll

end SortOp

object SortOp:
  /**
    * Below this many input rows a sort runs on the calling thread. The
    * wvlet.engine.parallelSortMinRows system property overrides it (tests use it to exercise the
    * parallel path on small data)
    */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.parallelSortMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

  /**
    * Row indexes lo until hi in (stable) sorted order, cut to the first `limit`. A limit of at most
    * 1/8 of the range keeps a bounded heap of the top rows instead of sorting the whole range
    */
  def sortRange(cmp: SortOp.RowComparator, lo: Int, hi: Int, limit: Int): Array[Int] =
    if limit <= 0 || hi <= lo then
      Array.emptyIntArray
    else if limit.toLong * 8 <= hi - lo then
      topRange(cmp, lo, hi, limit)
    else
      val idx = new Array[Int](hi - lo)
      var i   = 0
      while i < idx.length do
        idx(i) = lo + i
        i += 1
      IndexSort.sort(idx, cmp)
      if limit < idx.length then
        java.util.Arrays.copyOf(idx, limit)
      else
        idx

  /**
    * The first `limit` (< hi - lo) row indexes of lo until hi in stable sorted order, using a
    * max-heap of the `limit` smallest rows by (key, row index). A later row replaces the heap top
    * only when its key is strictly smaller, so ties keep the input order
    */
  private[exec] def topRange(cmp: SortOp.RowComparator, lo: Int, hi: Int, limit: Int): Array[Int] =
    val heap = new Array[Int](limit)
    var size = limit
    // a ranks after b in the stable order
    def after(a: Int, b: Int): Boolean =
      val c = cmp.compareRows(a, b)
      c > 0 || (c == 0 && a > b)
    def siftDown(start: Int): Unit =
      var i    = start
      var done = false
      while !done do
        val l = 2 * i + 1
        if l >= size then
          done = true
        else
          val r = l + 1
          val m =
            if r < size && after(heap(r), heap(l)) then
              r
            else
              l
          if after(heap(m), heap(i)) then
            val t = heap(i)
            heap(i) = heap(m)
            heap(m) = t
            i = m
          else
            done = true
    var i = 0
    while i < limit do
      heap(i) = lo + i
      i += 1
    var k = size / 2 - 1
    while k >= 0 do
      siftDown(k)
      k -= 1
    var j = lo + limit
    while j < hi do
      if cmp.compareRows(j, heap(0)) < 0 then
        heap(0) = j
        siftDown(0)
      j += 1
    // Pop the largest remaining row into the back of the output
    val out = new Array[Int](limit)
    var o   = limit - 1
    while o >= 0 do
      out(o) = heap(0)
      size -= 1
      heap(0) = heap(size)
      siftDown(0)
      o -= 1
    out

  end topRange

  /**
    * The number of independent output ranges to split the merge of `n` rows into: one (a merge on
    * the calling thread's task) below `minParallelRows` rows, else one per chunk
    */
  def mergeRangeCount(n: Int, chunks: Int, minParallelRows: Int): Int =
    if n < minParallelRows then
      1
    else
      math.max(1, math.min(chunks, n))

  /**
    * Cuts of sorted runs at `ranges + 1` evenly spaced ranks 0 .. n of their merge: `cuts(t)(r)` is
    * how many rows of run r come before rank `n * t / ranges`. Rows are ordered by (key, run,
    * position), so ties go to the earlier run as in [[merge]]
    */
  def splitRanks(
      runs: Array[Array[Int]],
      cmp: SortOp.RowComparator,
      n: Int,
      ranges: Int
  ): Array[Array[Int]] =
    val cuts = new Array[Array[Int]](ranges + 1)
    var t    = 0
    while t <= ranges do
      cuts(t) = cutAt(runs, cmp, (n.toLong * t / ranges).toInt)
      t += 1
    cuts

  /** How many rows of each run come before rank `k` of the merge of sorted `runs` */
  def cutAt(runs: Array[Array[Int]], cmp: SortOp.RowComparator, k: Int): Array[Int] =
    val p   = runs.length
    val lo  = new Array[Int](p)
    val hi  = new Array[Int](p)
    val cnt = new Array[Int](p)
    var j   = 0
    while j < p do
      hi(j) = runs(j).length
      j += 1
    // Invariant: lo(j) <= cut(j) <= hi(j). Each step halves the widest range with a pivot
    var done = false
    while !done do
      var r = 0
      j = 1
      while j < p do
        if hi(j) - lo(j) > hi(r) - lo(r) then
          r = j
        j += 1
      if hi(r) == lo(r) then
        done = true
      else
        val m = (lo(r) + hi(r)) >>> 1
        val x = runs(r)(m)
        // Rank of the pivot: rows ordered before (x, r, m) in each run
        var rank = 0L
        j = 0
        while j < p do
          cnt(j) =
            if j == r then
              m
            else
              countBefore(runs(j), cmp, x, inclusive = j < r)
          rank += cnt(j)
          j += 1
        if rank < k then
          // The pivot is among the first k rows, so are all rows before it
          j = 0
          while j < p do
            lo(j) = math.max(lo(j), cnt(j))
            j += 1
          lo(r) = m + 1
        else
          j = 0
          while j < p do
            hi(j) = math.min(hi(j), cnt(j))
            j += 1
        end if
      end if
    end while
    lo

  end cutAt

  /** Rows of a sorted run with a key below `x` (or at most `x` when `inclusive`) */
  private def countBefore(
      run: Array[Int],
      cmp: SortOp.RowComparator,
      x: Int,
      inclusive: Boolean
  ): Int =
    var lo = 0
    var hi = run.length
    while lo < hi do
      val mid = (lo + hi) >>> 1
      val c   = cmp.compareRows(run(mid), x)
      if c < 0 || (inclusive && c == 0) then
        lo = mid + 1
      else
        hi = mid
    lo

  /**
    * The first `limit` rows of sorted runs, merged with a binary heap of the runs' heads. Ties go
    * to the earlier run, so merging stable sorts of consecutive chunks gives the stable sort of all
    */
  def merge(runs: Array[Array[Int]], cmp: SortOp.RowComparator, limit: Int): Array[Int] =
    val to = new Array[Int](runs.length)
    var r  = 0
    while r < runs.length do
      to(r) = runs(r).length
      r += 1
    mergeRange(runs, cmp, new Array[Int](runs.length), to, limit)

  /**
    * Merges the rows `from(r)` until `to(r)` of each sorted run r (up to `limit` rows), ties going
    * to the earlier run
    */
  def mergeRange(
      runs: Array[Array[Int]],
      cmp: SortOp.RowComparator,
      from: Array[Int],
      to: Array[Int],
      limit: Int = Int.MaxValue
  ): Array[Int] =
    var total = 0L
    var r     = 0
    while r < runs.length do
      total += to(r) - from(r)
      r += 1
    val out = new Array[Int](math.min(limit.toLong, total).toInt)
    val pos = from.clone()
    // Min-heap of run ids by their current head row
    val heap                          = new Array[Int](runs.length)
    var size                          = 0
    def less(a: Int, b: Int): Boolean =
      val c = cmp.compareRows(runs(a)(pos(a)), runs(b)(pos(b)))
      c < 0 || (c == 0 && a < b)
    def siftDown(start: Int): Unit =
      var i    = start
      var done = false
      while !done do
        val l = 2 * i + 1
        if l >= size then
          done = true
        else
          val r = l + 1
          val m =
            if r < size && less(heap(r), heap(l)) then
              r
            else
              l
          if less(heap(m), heap(i)) then
            val t = heap(i)
            heap(i) = heap(m)
            heap(m) = t
            i = m
          else
            done = true
    r = 0
    while r < runs.length do
      if pos(r) < to(r) then
        heap(size) = r
        size += 1
      r += 1
    var k = size / 2 - 1
    while k >= 0 do
      siftDown(k)
      k -= 1
    var o = 0
    while o < out.length do
      val top = heap(0)
      out(o) = runs(top)(pos(top))
      o += 1
      pos(top) += 1
      if pos(top) == to(top) then
        size -= 1
        heap(0) = heap(size)
      siftDown(0)
    out

  end mergeRange

  /** Compares rows by index; `compareRows` avoids boxing the indexes */
  abstract class RowComparator extends java.util.Comparator[Integer]:
    def compareRows(i: Int, j: Int): Int
    final def compare(a: Integer, b: Integer): Int = compareRows(a.intValue(), b.intValue())

  def comparator(keyVecs: Array[Vec], keys: Seq[SortKey]): RowComparator =
    val asc        = keys.map(_.ascending).toArray
    val nullsFirst = keys.map(_.nullsFirst).toArray
    new RowComparator:
      def compareRows(i: Int, j: Int): Int =
        var c = 0
        var k = 0
        while c == 0 && k < keyVecs.length do
          val v  = keyVecs(k)
          val ni = v.isNull(i)
          val nj = v.isNull(j)
          c =
            if ni && nj then
              0
            else if ni then
              if nullsFirst(k) then
                -1
              else
                1
            else if nj then
              if nullsFirst(k) then
                1
              else
                -1
            else
              val r =
                v match
                  case l: LongVec =>
                    java.lang.Long.compare(l.values(i), l.values(j))
                  case x: IntVec =>
                    Integer.compare(x.values(i), x.values(j))
                  case d: DoubleVec =>
                    java.lang.Double.compare(d.values(i), d.values(j))
                  case s: StrVec =>
                    s.values(i).compareTo(s.values(j))
                  case other =>
                    ValueOps.compare(other.get(i), other.get(j))
              if asc(k) then
                r
              else
                -r
          k += 1
        end while
        c
      end compareRows
    end new

  end comparator

end SortOp

/**
  * Hash aggregation. Output columns: grouping keys, then one column per aggregate. Without keys,
  * exactly one row is produced, even for an empty input
  */
class HashAggOp(
    val child: Operator,
    val keys: IndexedSeq[VExpr],
    val aggs: IndexedSeq[AggCall],
    val schema: Schema
) extends Operator:
  private var done                     = false
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(keys=[${keys.map(_.sig).mkString(", ")}], aggs=[${aggs.map(_.sig).mkString(", ")}])"

  def next(): Batch =
    if done then
      return null
    done = true
    val table = GroupTable(keys.map(_.ptype))
    val accs  = aggs.map(_.newAccumulator())
    var b     = child.next()
    while b != null do
      val groups =
        if keys.isEmpty then
          new Array[Int](b.size)
        else
          table.groupIds(keys.map(_.eval(b)).toArray, b.size)
      val numGroups =
        if keys.isEmpty then
          1
        else
          table.numGroups
      var a = 0
      while a < accs.size do
        val agg = aggs(a)
        val acc = accs(a)
        acc.ensureGroups(numGroups)
        val argVecs = agg.args.map(_.eval(b)).toArray
        agg.filter match
          case Some(f) =>
            val mask = Kernels.trueMask(f.eval(b))
            var cnt  = 0
            var i    = 0
            while i < b.size do
              if mask(i) then
                cnt += 1
              i += 1
            val idx = new Array[Int](cnt)
            val gs  = new Array[Int](cnt)
            var j   = 0
            i = 0
            while i < b.size do
              if mask(i) then
                idx(j) = i
                gs(j) = groups(i)
                j += 1
              i += 1
            acc.update(gs, cnt, argVecs.map(_.gather(idx, cnt)))
          case None =>
            acc.update(groups, b.size, argVecs)
        a += 1
      b = child.next()
    end while
    val numGroups =
      if keys.isEmpty then
        1
      else
        table.numGroups
    if numGroups == 0 then
      return null
    accs.foreach(_.ensureGroups(numGroups))
    val keyVecs =
      if keys.isEmpty then
        Array.empty[Vec]
      else
        table.keyVectors()
    val aggVecs = accs.map(_.result(numGroups))
    Batch(keyVecs ++ aggVecs, numGroups)

  end next

end HashAggOp

/** Concatenation of inputs with the same column count; values are cast to the output types */
class UnionAllOp(val inputs: Seq[Operator], val schema: Schema) extends Operator:
  private var current                  = 0
  override def children: Seq[Operator] = inputs
  def next(): Batch                    =
    while current < inputs.size do
      val b = inputs(current).next()
      if b != null then
        return Batch(
          b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype)),
          b.size
        )
      current += 1
    null

/** Removes duplicate rows */
class DistinctOp(val child: Operator) extends Operator:
  private val table                    = GroupTable(child.schema.types)
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  def next(): Batch                    =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val before = table.numGroups
      val ids    = table.groupIds(b.columns, b.size)
      // Keep the first row of every new group
      val keep = new Array[Int](b.size)
      var k    = 0
      var next = before
      var i    = 0
      while i < b.size do
        if ids(i) == next then
          keep(k) = i
          k += 1
          next += 1
        i += 1
      if k > 0 then
        out = b.gather(keep, k)
    out

/** INTERSECT / EXCEPT with set (distinct) or bag (ALL) semantics */
class SetOpOp(
    val left: Operator,
    val right: Operator,
    val isIntersect: Boolean,
    val all: Boolean,
    val schema: Schema
) extends Operator:
  private var done                     = false
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            =
    if isIntersect then
      "Intersect"
    else
      "Except"

  def next(): Batch =
    if done then
      return null
    done = true
    val counts = mutable.HashMap.empty[AnyRef, Int]
    var b      = right.next()
    while b != null do
      val cols = b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < b.size do
        val k = GroupKey.of(cols, i)
        counts(k) = counts.getOrElse(k, 0) + 1
        i += 1
      b = right.next()
    val emitted = mutable.HashMap.empty[AnyRef, Int]
    val out     = schema.types.map(t => VecBuilder(t)).toArray
    var lb      = left.next()
    while lb != null do
      val cols = lb.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < lb.size do
        val k       = GroupKey.of(cols, i)
        val inRight = counts.getOrElse(k, 0)
        val already = emitted.getOrElse(k, 0)
        val emit    =
          if isIntersect then
            if all then
              already < inRight
            else
              inRight > 0 && already == 0
          else if all then
            // EXCEPT ALL: emit occurrences beyond the right count
            val seenLeft = emitted.getOrElse(k, 0)
            emitted(k) = seenLeft + 1
            seenLeft + 1 > inRight
          else
            inRight == 0 && already == 0
        if emit then
          if isIntersect || !all then
            emitted(k) = already + 1
          var c = 0
          while c < cols.length do
            out(c).add(cols(c).get(i))
            c += 1
        else if isIntersect && !all then
          ()
        i += 1
      lb = left.next()
    end while
    val vecs = out.map(_.result())
    val n    =
      if vecs.isEmpty then
        0
      else
        vecs(0).size
    if n == 0 then
      null
    else
      Batch(vecs, n)

  end next

end SetOpOp

enum JoinKind:
  case Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
    /**
      * Build (right) rows with a match in the probe input, each once: a semi join with swapped
      * inputs
      */
    RightSemi,
    /** Build (right) rows without a match in the probe input: an anti join with swapped inputs */
    RightAnti

  /** Whether the join tracks which build rows matched, and emits build rows at the end */
  def tracksBuildMatches: Boolean =
    this match
      case Right | Full | RightSemi | RightAnti =>
        true
      case _ =>
        false

  /** Whether the output has only the build (right) columns */
  def outputsBuildOnly: Boolean = this == RightSemi || this == RightAnti

  /** Whether the output has only the probe (left) columns */
  def outputsProbeOnly: Boolean = this == Semi || this == Anti

/**
  * The hash table of a join's build (right) input: its rows, and for each distinct key the chain of
  * rows holding it. Built once on first use, then only read, so probes may share it across threads
  */
final class JoinBuild(
    right: Operator,
    rightKeys: IndexedSeq[VExpr],
    trackMatches: Boolean = false,
    runner: TaskRunner = TaskRunner.Sequential
):
  @volatile
  private var built = false

  var build: Batch = null

  // Rows are partitioned by the hash of their key, one hash table per partition; a single
  // partition for small builds. `heads(p)(g)` is the first row of group g of partition p, and
  // `chain(row)` the next row with the same key
  private var parts: Int                = 1
  private var hasher: GroupTable        = null
  private var tables: Array[GroupTable] = null
  private var heads: Array[Array[Int]]  = null
  private var chain: Array[Int]         = null

  /** The row with the same key after `row`, or -1 */
  inline def nextMatch(row: Int): Int = chain(row)

  /**
    * Build rows some probe matched, shared by the probe copies of a right / full join. Copies only
    * ever set entries to true, and the unmatched rows are read after every copy has finished
    */
  var matched: Array[Boolean] = null

  def ensureBuilt(): Unit =
    if !built then
      synchronized {
        if !built then
          buildTable()
          built = true
      }

  private def buildTable(): Unit =
    build = Operator.materialize(right)
    val keyVecs = rightKeys.map(_.eval(build)).toArray
    val types   = rightKeys.map(_.ptype)
    // Rows with NULL keys never match
    val valid = new Array[Int](build.size)
    var nv    = 0
    var i     = 0
    while i < build.size do
      if !GroupKey.hasNull(keyVecs, i) then
        valid(nv) = i
        nv += 1
      i += 1
    chain = Vec.filledInts(build.size, -1)
    if runner.parallelism > 1 && nv >= JoinBuild.MinParallelRows then
      parts = GroupTable.partitionCount(runner.parallelism)
    // Rows of each partition, in input order
    val rowsOf: Array[Array[Int]] =
      if parts == 1 then
        Array(java.util.Arrays.copyOf(valid, nv))
      else
        hasher = GroupTable(types)
        val h     = hasher.keyHashes(keyVecs.map(_.gather(valid, nv)), nv)
        val byPos = GroupTable.partitionRows(h, nv, parts)
        // Positions among the valid rows -> build rows (a loop: Array.map would box every Int)
        byPos.foreach { r =>
          var j = 0
          while j < r.length do
            r(j) = valid(r(j))
            j += 1
        }
        byPos
    tables = new Array[GroupTable](parts)
    heads = new Array[Array[Int]](parts)
    def buildPart(p: Int): Unit =
      val rows  = rowsOf(p)
      val n     = rows.length
      val table = GroupTable(types)
      val ids   = table.groupIds(keyVecs.map(_.gather(rows, n)), n)
      val head  = Vec.filledInts(table.numGroups, -1)
      // Insert in reverse so chains enumerate rows in input order. Partitions hold disjoint rows,
      // so they write disjoint chain entries
      var j = n - 1
      while j >= 0 do
        val row = rows(j)
        val g   = ids(j)
        chain(row) = head(g)
        head(g) = row
        j -= 1
      tables(p) = table
      heads(p) = head
    if parts == 1 then
      buildPart(0)
    else
      (0 until parts).map(p => runner.submit(() => buildPart(p))).foreach(_.get())
    if trackMatches then
      matched = new Array[Boolean](build.size)

  end buildTable

  /**
    * The first build row with the key of each probe row, or -1 when none has it (or the probe key
    * has a NULL). Safe to call from several threads once built
    */
  def firstMatches(keys: Array[Vec], n: Int): Array[Int] =
    val out = new Array[Int](n)
    if parts == 1 then
      val ids  = tables(0).lookup(keys, n)
      val head = heads(0)
      var i    = 0
      while i < n do
        val g = ids(i)
        out(i) =
          if g >= 0 then
            head(g)
          else
            -1
        i += 1
    else
      val rowsOf = GroupTable.partitionRows(hasher.keyHashes(keys, n), n, parts)
      var p      = 0
      while p < parts do
        val rows = rowsOf(p)
        if rows.nonEmpty then
          val ids  = tables(p).lookup(keys.map(_.gather(rows, rows.length)), rows.length)
          val head = heads(p)
          var j    = 0
          while j < rows.length do
            val g = ids(j)
            out(rows(j)) =
              if g >= 0 then
                head(g)
              else
                -1
            j += 1
        p += 1
    end if
    out

  end firstMatches

end JoinBuild

object JoinBuild:
  /** Builds with fewer rows use one hash table, built on the calling thread */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.partitionedJoinMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

/** How a right / full join tracks and emits build rows no probe row matched */
enum OuterMode:
  /** Tracks its own matches and emits the unmatched rows at the end of its input */
  case Own

  /** One part of a split probe: marks the shared build's matches, emits no unmatched rows */
  case Part

  /** The last run after every part: marks the shared matches, then emits the unmatched rows */
  case Tail

/**
  * Equi hash join building on the right input. Output: left columns then right columns (Semi /
  * Anti: left columns only). `residual` is an extra condition over the joined row.
  *
  * `shared` is a build already made for this join elsewhere: copies of the join that probe parts of
  * the left input in parallel share one build
  */
class HashJoinOp(
    val left: Operator,
    val right: Operator,
    val leftKeys: IndexedSeq[VExpr],
    val rightKeys: IndexedSeq[VExpr],
    val kind: JoinKind,
    val residual: Option[VExpr],
    val schema: Schema,
    shared: JoinBuild = null,
    outerMode: OuterMode = OuterMode.Own,
    buildRunner: TaskRunner = TaskRunner.Sequential
) extends Operator:
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            = s"HashJoin[${kind}]"
  override protected def details       =
    s"(${leftKeys.map(_.sig).mkString(",")} = ${rightKeys.map(_.sig).mkString(",")}${residual
        .map(r => s", residual=${r.sig}")
        .getOrElse("")})"

  /** The build this join probes */
  val joinBuild: JoinBuild =
    if shared != null then
      shared
    else
      JoinBuild(right, rightKeys, trackMatches = kind.tracksBuildMatches, runner = buildRunner)

  /** The same join over another left input, sharing this join's build */
  def withLeft(newLeft: Operator, mode: OuterMode = OuterMode.Own): HashJoinOp = HashJoinOp(
    newLeft,
    right,
    leftKeys,
    rightKeys,
    kind,
    residual,
    schema,
    joinBuild,
    mode
  )

  override def close(): Unit =
    // A shared build's right input belongs to the join that made the build
    if shared == null then
      super.close()
    else
      left.close()

  private var built                        = false
  private var build: Batch                 = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false
  private val leftWidth                    = left.schema.size

  private def buildTable(): Unit =
    joinBuild.ensureBuilt()
    build = joinBuild.build
    if kind.tracksBuildMatches then
      buildMatched =
        outerMode match
          case OuterMode.Own if shared == null =>
            // This join's own build: its matched array is this join's alone
            joinBuild.matched
          case OuterMode.Own =>
            new Array[Boolean](build.size)
          case _ =>
            joinBuild.matched
    built = true

  def next(): Batch =
    if !built then
      buildTable()
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched || outerMode == OuterMode.Part then
      return null
    emittedUnmatched = true
    // RIGHT SEMI emits the matched build rows; the other kinds the unmatched ones
    val wanted = kind == JoinKind.RightSemi
    val idx    = (0 until build.size).filter(i => buildMatched(i) == wanted).toArray
    if idx.isEmpty then
      return null
    val n = idx.length
    if kind.outputsBuildOnly then
      build.gather(idx, n)
    else
      val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
      Batch(leftPad ++ build.gather(idx, n).columns, n)

  private def probeBatch(probe: Batch): Batch =
    val keyVecs = leftKeys.map(_.eval(probe)).toArray
    val first   = joinBuild.firstMatches(keyVecs, probe.size)
    val li      = mutable.ArrayBuilder.make[Int]
    val ri      = mutable.ArrayBuilder.make[Int]
    var i       = 0
    while i < probe.size do
      var r = first(i)
      while r >= 0 do
        li += i
        ri += r
        r = joinBuild.nextMatch(r)
      i += 1
    var lIdx = li.result()
    var rIdx = ri.result()
    // Apply the residual condition on candidate pairs
    residual.foreach { cond =>
      if lIdx.nonEmpty then
        val joined = Batch(
          probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
          lIdx.length
        )
        val mask = Kernels.trueMask(cond.eval(joined))
        val keep = (0 until lIdx.length).filter(mask(_)).toArray
        lIdx = keep.map(lIdx)
        rIdx = keep.map(rIdx)
    }
    kind match
      case JoinKind.Inner | JoinKind.Cross =>
        if lIdx.isEmpty then
          Batch(probe.gather(lIdx, 0).columns ++ build.gather(rIdx, 0).columns, 0) match
            case _ =>
              null
        else
          Batch(
            probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
            lIdx.length
          )
      case JoinKind.Left | JoinKind.Full | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if kind == JoinKind.Right then
          if lIdx.isEmpty then
            null
          else
            Batch(
              probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
              lIdx.length
            )
        else
          val matched = new Array[Boolean](probe.size)
          lIdx.foreach(l => matched(l) = true)
          val unmatched = (0 until probe.size).filter(!matched(_)).toArray
          val allL      = lIdx ++ unmatched
          val allR      = rIdx ++ Vec.filledInts(unmatched.length, -1)
          if allL.isEmpty then
            null
          else
            Batch(
              probe.gather(allL, allL.length).columns ++ build.gather(allR, allR.length).columns,
              allL.length
            )
      case JoinKind.Semi =>
        val matched = new Array[Boolean](probe.size)
        lIdx.foreach(l => matched(l) = true)
        val r = probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = new Array[Boolean](probe.size)
        java.util.Arrays.fill(keep, true)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only mark the matched build rows; they are emitted at the end of the input
        rIdx.foreach(r => buildMatched(r) = true)
        null
    end match

  end probeBatch

end HashJoinOp

/** Nested-loop join for non-equi conditions and cross joins (right input materialized) */
class NestedLoopJoinOp(
    val left: Operator,
    val right: Operator,
    val cond: Option[VExpr],
    val kind: JoinKind,
    val schema: Schema
) extends Operator:
  override def children: Seq[Operator]     = Seq(left, right)
  override def name: String                = s"NestedLoopJoin[${kind}]"
  override protected def details           = cond.map(c => s"(${c.sig})").getOrElse("")
  private var build: Batch                 = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false

  def next(): Batch =
    if build == null then
      build = Operator.materialize(right)
      if kind == JoinKind.Right || kind == JoinKind.Full then
        buildMatched = new Array[Boolean](build.size)
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched then
      return null
    emittedUnmatched = true
    val idx = (0 until build.size).filter(i => !buildMatched(i)).toArray
    if idx.isEmpty then
      return null
    val n       = idx.length
    val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
    Batch(leftPad ++ build.gather(idx, n).columns, n)

  private def probeBatch(probe: Batch): Batch =
    val nl = probe.size
    val nr = build.size
    // Process the cross product in chunks of left rows to bound memory
    val li    = mutable.ArrayBuilder.make[Int]
    val ri    = mutable.ArrayBuilder.make[Int]
    val chunk = math.max(1, 65536 / math.max(1, nr))
    var start = 0
    while start < nl do
      val end = math.min(nl, start + chunk)
      val n   = (end - start) * nr
      if n > 0 then
        val lIdx = new Array[Int](n)
        val rIdx = new Array[Int](n)
        var k    = 0
        var i    = start
        while i < end do
          var j = 0
          while j < nr do
            lIdx(k) = i
            rIdx(k) = j
            k += 1
            j += 1
          i += 1
        cond match
          case Some(c) =>
            val joined = Batch(probe.gather(lIdx, n).columns ++ build.gather(rIdx, n).columns, n)
            val mask   = Kernels.trueMask(c.eval(joined))
            var x      = 0
            while x < n do
              if mask(x) then
                li += lIdx(x)
                ri += rIdx(x)
              x += 1
          case None =>
            li ++= lIdx
            ri ++= rIdx
      start = end
    end while
    val lIdx = li.result()
    val rIdx = ri.result()
    kind match
      case JoinKind.Inner | JoinKind.Cross | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if lIdx.isEmpty then
          null
        else
          Batch(
            probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
            lIdx.length
          )
      case JoinKind.Left | JoinKind.Full =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val unmatched = (0 until nl).filter(!matched(_)).toArray
        val allL      = lIdx ++ unmatched
        val allR      = rIdx ++ Vec.filledInts(unmatched.length, -1)
        if allL.isEmpty then
          null
        else
          Batch(
            probe.gather(allL, allL.length).columns ++ build.gather(allR, allR.length).columns,
            allL.length
          )
      case JoinKind.Semi =>
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val r = probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = Vec.allTrue(nl)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only hash joins swap semi / anti inputs
        throw IllegalStateException(s"Unsupported nested-loop join kind: ${kind}")
    end match

  end probeBatch

end NestedLoopJoinOp

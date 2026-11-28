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
      // Done: the buffer is dropped, so its reservation returns before the query ends
      result = Iterator.empty
      memory.release()
      null

  /**
    * Whether an input past the query's spill threshold is sorted in runs written to spill files and
    * merged (`spill_sort`, [[SortSpill]])
    */
  var spill: Boolean = true

  /** Set [[spill]] */
  def withSpill(enabled: Boolean): this.type =
    spill = enabled
    this

  // The query's memory budget, captured where the sort is planned
  private val memory                     = MemoryContext.current.child()
  private def sortAll(): Iterator[Batch] =
    if spill && memory.canSpill && SpillBlocks.spillable(child.schema) then
      SortSpill(this, memory).sort()
    else
      sortBatch(memory.materialize(child), limit)

  /** The rows of `all` in sorted order, cut to `cut`, as batches of [[Batch.DefaultSize]] rows */
  private[exec] def sortBatch(all: Batch, cut: Option[Long]): Iterator[Batch] =
    sortedRanges(all, cut) { part =>
      Iterator
        .range(0, part.length, Batch.DefaultSize)
        .map { from =>
          val to = math.min(part.length, from + Batch.DefaultSize)
          all.gather(java.util.Arrays.copyOfRange(part, from, to), to - from)
        }
        .toIndexedSeq
    }.flatten

  /**
    * The sorted row indexes of `all` (cut to `cut`) as consecutive output ranges, each passed to
    * `emit`: one range on the calling thread for small inputs, else ranges emitted as tasks of
    * `runner` (in order)
    */
  private[exec] def sortedRanges[A](all: Batch, cut: Option[Long])(
      emit: Array[Int] => A
  ): Iterator[A] =
    if all.size == 0 then
      return Iterator.empty
    val keyVecs = keys.map(k => k.expr.eval(all)).toArray
    val cmp     = SortOp.comparator(keyVecs, keys)
    val n       = cut.map(l => math.min(l, all.size.toLong).toInt).getOrElse(all.size)
    if all.size < minParallelRows || runner.parallelism <= 1 then
      Iterator.single(emit(SortOp.sortRange(cmp, 0, all.size, n)))
    else
      val chunks     = math.min(runner.parallelism, all.size)
      val sortedRuns =
        TaskRunner
          .runAll(runner, chunks) { c =>
            val lo = (all.size.toLong * c / chunks).toInt
            val hi = (all.size.toLong * (c + 1) / chunks).toInt
            SortOp.sortRange(cmp, lo, hi, n)
          }
          .toArray
      // Split the merge into independent output ranges (one task each) that merge and emit
      val ranges = SortOp.mergeRangeCount(n, chunks, minParallelRows)
      val cuts   = SortOp.splitRanks(sortedRuns, cmp, n, ranges)
      val tasks  = (0 until ranges).map { t =>
        runner.submit(() => emit(SortOp.mergeRange(sortedRuns, cmp, cuts(t), cuts(t + 1))))
      }
      tasks.iterator.map(_.get())
    end if

  end sortedRanges

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

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

import wvlet.lang.engine.vector.*

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
  * The external merge sort of a [[SortOp]] (`spill_sort`): the input is buffered with
  * [[MemoryContext.tryReserve]] (each batch twice, it and its concatenated copy, plus index bytes);
  * past the query's spill threshold the buffer is sorted ([[SortOp.sortedRanges]], in parallel) and
  * written as one sorted run file, and the next run starts. With a limit, a full buffer is first
  * cut to its top `limit` rows and kept in memory, as long as that halves it. At the end, the last
  * buffer sorts in memory as the newest run, and the runs merge as the sort emits its rows
  * ([[SortSpill.Merge]]), more than [[SortSpill.MaxFanIn]] runs in passes over consecutive groups.
  * Ties go to the earlier run, so the output equals the stable in-memory sort. Used by the thread
  * that drains the sort
  */
final class SortSpill(op: SortOp, memory: MemoryContext):
  import SortSpill.*

  private val schema        = op.child.schema
  private lazy val space    = memory.spillSpace
  private val runs          = mutable.ArrayBuffer.empty[SpillFile]
  private val buffer        = mutable.ArrayBuffer.empty[Batch]
  private var bufferBytes   = 0L
  private var bufferMemory  = memory.child()
  private val minRunBytes   = math.max(1L, memory.spillThresholdBytes / 16)
  private val cutLimit      = op.limit
  private val limitRows     = op.limit.getOrElse(Long.MaxValue)
  private def keys          = op.keys.toIndexedSeq
  private def resetBuffer() =
    buffer.clear()
    bufferBytes = 0L
    bufferMemory.release()
    bufferMemory = memory.child()

  /** Sort the input, spilling runs past the threshold */
  def sort(): Iterator[Batch] =
    try
      var x = op.child.next()
      while x != null do
        if x.size > 0 then
          add(x.compact())
        x = op.child.next()
      val last = op.sortBatch(concatBuffer(), cutLimit)
      if runs.isEmpty then
        last
      else
        mergeRuns(last)
    catch
      case e: Throwable =>
        runs.foreach(_.delete())
        throw e

  private def add(c: Batch): Unit =
    val bytes = reserveBytes(c)
    if !bufferMemory.tryReserve(bytes) then
      // Runs of at least a sixteenth of the threshold, even when other holders took the rest
      if bufferBytes >= minRunBytes then
        flush()
      if !bufferMemory.tryReserve(bytes) then
        bufferMemory.reserve(bytes)
    buffer += c
    bufferBytes += bytes

  private def concatBuffer(): Batch =
    val all =
      if buffer.isEmpty then
        Batch.empty(schema)
      else if buffer.size == 1 then
        buffer.head
      else
        Batch.concat(schema, buffer.toSeq)
    buffer.clear()
    all

  // Cut the buffer to its top rows, or write it as a sorted run
  private def flush(): Unit =
    val all = concatBuffer()
    if cutLimit.exists(_ <= all.size / 2) then
      val top = op.sortBatch(all, cutLimit).toSeq
      resetBuffer()
      topCuts.incrementAndGet()
      // At most `limit` rows: kept even past the threshold
      top.foreach { b =>
        val bytes = reserveBytes(b)
        bufferMemory.reserve(bytes)
        buffer += b
        bufferBytes += bytes
      }
    else
      val f = space.newFile(schema)
      op.sortedRanges(all, cutLimit)(encodeBlocks(all))
        .foreach(_.foreach(b => f.appendBlock(b.bytes, b.rows, b.estimatedBytes)))
      f.finish()
      runs += f
      runsWritten.incrementAndGet()
      resetBuffer()

  // One output range of a sorted run, as encoded blocks of Batch.DefaultSize rows
  private def encodeBlocks(all: Batch)(part: Array[Int]): IndexedSeq[AggSpillRouter.Block] =
    Iterator
      .range(0, part.length, Batch.DefaultSize)
      .map { from =>
        val to = math.min(part.length, from + Batch.DefaultSize)
        val b  = all.gather(java.util.Arrays.copyOfRange(part, from, to), to - from)
        AggSpillRouter.Block(SpillBlocks.encode(b), b.size, MemoryContext.estimatedBytes(b))
      }
      .toIndexedSeq

  // Merge the written runs and the last (in-memory) one, in passes of at most MaxFanIn runs
  private def mergeRuns(last: Iterator[Batch]): Iterator[Batch] =
    // `runs` holds every file not yet deleted, so a failure in a pass deletes them all
    var files = runs.toIndexedSeq
    while files.size + 1 > MaxFanIn do
      files =
        files
          .grouped(MaxFanIn)
          .map { group =>
            if group.size == 1 then
              group.head
            else
              val f = space.newFile(schema)
              runs += f
              Merge(schema, keys, group.map(_.read()), limitRows).foreach(f.append)
              f.finish()
              group.foreach(_.delete())
              runs --= group
              mergePasses.incrementAndGet()
              f
          }
          .toIndexedSeq
    // Read back until the merge ends; the files go once it does (or with the query)
    val merge = Merge(schema, keys, files.map(_.read()) :+ last, limitRows)
    new Iterator[Batch]:
      def hasNext: Boolean =
        val h = merge.hasNext
        if !h then
          runs.foreach(_.delete())
          runs.clear()
        h
      def next(): Batch = merge.next()

  end mergeRuns

end SortSpill

object SortSpill:
  /** The most runs merged at once: each holds one block in memory while merging */
  val MaxFanIn = 64

  /** Sorted runs written to spill files (for tests and stats) */
  private[engine] val runsWritten = AtomicLong(0L)

  /** Buffers cut to their top rows instead of written, with a limit (for tests and stats) */
  private[engine] val topCuts = AtomicLong(0L)

  /** Intermediate merges of more than [[MaxFanIn]] runs (for tests and stats) */
  private[engine] val mergePasses = AtomicLong(0L)

  /**
    * The bytes a spillable sort reserves for a batch of its input: the batch, its concatenated
    * copy, and 8 bytes per row of row indexes
    */
  def reserveBytes(b: Batch): Long = 2 * MemoryContext.estimatedBytes(b) + 8L * b.size

  /**
    * The first `limit` rows of sorted `runs` (each an iterator of sorted batches), merged with a
    * binary heap of the runs' head rows, ties going to the earlier run. Output batches hold up to
    * [[Batch.DefaultSize]] rows, gathered per source batch and put in merge order
    */
  final class Merge(
      schema: Schema,
      keys: IndexedSeq[SortKey],
      runs: IndexedSeq[Iterator[Batch]],
      limit: Long
  ) extends Iterator[Batch]:
    private val k          = runs.size
    private val asc        = keys.map(_.ascending).toArray
    private val nullsFirst = keys.map(_.nullsFirst).toArray
    private val heads      = new Array[Batch](k)
    private val headKeys   = new Array[Array[Vec]](k)
    private val pos        = new Array[Int](k)
    private val heap       = new Array[Int](k)
    private var live       = 0
    private var emitted    = 0L
    // The source (index into `sources`) of each run's head batch, for the current output batch
    private val headSource = new Array[Int](k)
    private val sources    = mutable.ArrayBuffer.empty[Batch]
    private val sourceRows = mutable.ArrayBuffer.empty[mutable.ArrayBuilder.ofInt]

    // Load run r's next non-empty batch; false at its end
    private def load(r: Int): Boolean =
      heads(r) = null
      headKeys(r) = null
      var found = false
      while !found && runs(r).hasNext do
        val b = runs(r).next()
        if b.size > 0 then
          heads(r) = b
          headKeys(r) = keys.map(_.expr.eval(b)).toArray
          pos(r) = 0
          headSource(r) = -1
          found = true
      found

    private def less(a: Int, b: Int): Boolean =
      val c = compareAcross(headKeys(a), pos(a), headKeys(b), pos(b), asc, nullsFirst)
      c < 0 || (c == 0 && a < b)

    private def siftDown(start: Int): Unit =
      var i    = start
      var done = false
      while !done do
        val l = 2 * i + 1
        if l >= live then
          done = true
        else
          val r = l + 1
          val m =
            if r < live && less(heap(r), heap(l)) then
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

    locally {
      var r = 0
      while r < k do
        if load(r) then
          heap(live) = r
          live += 1
        r += 1
      var i = live / 2 - 1
      while i >= 0 do
        siftDown(i)
        i -= 1
    }

    def hasNext: Boolean = live > 0 && emitted < limit

    def next(): Batch =
      if !hasNext then
        throw java.util.NoSuchElementException("No more sorted rows")
      val n    = math.min(Batch.DefaultSize.toLong, limit - emitted).toInt
      val pick = new Array[Int](n)
      var o    = 0
      while o < n && live > 0 do
        val top = heap(0)
        if headSource(top) < 0 then
          headSource(top) = sources.size
          sources += heads(top)
          sourceRows += mutable.ArrayBuilder.ofInt()
        pick(o) = headSource(top)
        sourceRows(headSource(top)) += pos(top)
        o += 1
        pos(top) += 1
        if pos(top) == heads(top).size && !load(top) then
          live -= 1
          heap(0) = heap(live)
        siftDown(0)
      emitted += o
      val out = gatherPicks(pick, o)
      // The next output batch starts new sources
      sources.clear()
      sourceRows.clear()
      var r = 0
      while r < k do
        headSource(r) = -1
        r += 1
      out

    end next

    private def gatherPicks(pick: Array[Int], n: Int): Batch =
      val rows = sourceRows.map(_.result())
      if sources.size == 1 then
        val idx = rows.head
        if idx(idx.length - 1) - idx(0) == idx.length - 1 then
          sources.head.slice(idx(0), idx(0) + idx.length)
        else
          sources.head.gather(idx, idx.length)
      else
        val pieces = sources.indices.map(s => sources(s).gather(rows(s), rows(s).length))
        val all    = Batch.concat(schema, pieces)
        // Each pick's position in `all`: its source's offset plus its rank in the source
        val next = new Array[Int](sources.size)
        var off  = 0
        var s    = 0
        while s < sources.size do
          next(s) = off
          off += rows(s).length
          s += 1
        val perm = new Array[Int](n)
        var i    = 0
        while i < n do
          perm(i) = next(pick(i))
          next(pick(i)) += 1
          i += 1
        all.gather(perm, n)

  end Merge

  /** Compares row i of key columns `a` with row j of key columns `b` (of the same types) */
  def compareAcross(
      a: Array[Vec],
      i: Int,
      b: Array[Vec],
      j: Int,
      asc: Array[Boolean],
      nullsFirst: Array[Boolean]
  ): Int =
    var c = 0
    var k = 0
    while c == 0 && k < a.length do
      val va = a(k)
      val vb = b(k)
      val ni = va.isNull(i)
      val nj = vb.isNull(j)
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
            (va, vb) match
              case (x: LongVec, y: LongVec) =>
                java.lang.Long.compare(x.values(i), y.values(j))
              case (x: IntVec, y: IntVec) =>
                Integer.compare(x.values(i), y.values(j))
              case (x: DoubleVec, y: DoubleVec) =>
                java.lang.Double.compare(x.values(i), y.values(j))
              case (x: StrVec, y: StrVec) =>
                x.values(i).compareTo(y.values(j))
              case _ =>
                ValueOps.compare(va.get(i), vb.get(j))
          if asc(k) then
            r
          else
            -r
      k += 1
    end while
    c

  end compareAcross

end SortSpill

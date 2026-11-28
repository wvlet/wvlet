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

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
  * Routes an aggregation's input into [[AggSpill.Partitions]] spill files, with the work as tasks
  * of `runner`: the input is cut into chunks of about `chunkBytes`, and each task hashes, gathers,
  * and encodes one chunk's rows per partition. The caller appends the encoded blocks to the
  * partitions' files in chunk order, so a partition's rows keep the input order. At most
  * `runner.parallelism` chunks are in flight, their rows reserved until their blocks are written.
  * Used by one thread; [[finish]] and [[abort]] wait for every task, so no task runs after the
  * query's spill files close
  */
final class AggSpillRouter(
    schema: Schema,
    keys: IndexedSeq[VExpr],
    longDecimalKeys: Boolean,
    memory: MemoryContext,
    runner: TaskRunner,
    seed: Int = 0
):
  import AggSpillRouter.Block

  private val space      = memory.spillSpace
  private val files      = new Array[SpillFile](AggSpill.Partitions)
  private val chunkBytes = math.min(
    64L << 20,
    math.max(4L << 20, memory.spillThresholdBytes / (4L * math.max(1, runner.parallelism)))
  )

  private val inFlight    = mutable.Queue.empty[(TaskRunner.Handle[Array[Block]], MemoryContext)]
  private var chunk       = mutable.ArrayBuffer.empty[Batch]
  private var chunkMemory = memory.child()
  private var chunkSize   = 0L

  /** Add the rows of `b` (routed once its chunk is full) */
  def add(b: Batch): Unit =
    val bytes = MemoryContext.estimatedBytes(b)
    chunkMemory.reserve(bytes)
    chunk += b
    chunkSize += bytes
    if chunkSize >= chunkBytes then
      submit()

  private def submit(): Unit =
    if chunk.nonEmpty then
      while inFlight.size >= math.max(1, runner.parallelism) do
        write()
      val bs = chunk.toIndexedSeq
      val h  = runner.submit(() => AggSpillRouter.route(schema, keys, longDecimalKeys, bs, seed))
      inFlight.enqueue((h, chunkMemory))
      chunk = mutable.ArrayBuffer.empty[Batch]
      chunkMemory = memory.child()
      chunkSize = 0L

  // Append the oldest chunk's blocks, then return its reservation
  private def write(): Unit =
    val (h, mem) = inFlight.dequeue()
    try
      val blocks = h.get()
      var p      = 0
      while p < blocks.length do
        val b = blocks(p)
        if b != null then
          if files(p) == null then
            files(p) = space.newFile(schema)
            AggSpill.filesWritten.incrementAndGet()
          files(p).appendBlock(b.bytes, b.rows, b.estimatedBytes)
        p += 1
    finally
      mem.release()

  /** Route the last chunk and write every block: the partitions that hold rows */
  def finish(): IndexedSeq[SpillFile] =
    try
      submit()
      while inFlight.nonEmpty do
        write()
    catch
      case e: Throwable =>
        abort()
        throw e
    files.foreach { f =>
      if f != null then
        f.finish()
    }
    files.filter(_ != null).toIndexedSeq

  /** Wait for every routing task without writing more (after a failure) */
  def abort(): Unit =
    while inFlight.nonEmpty do
      val (h, mem) = inFlight.dequeue()
      try
        h.get()
      catch
        case _: Throwable =>
      mem.release()
    chunkMemory.release()

end AggSpillRouter

object AggSpillRouter:
  /** Remix the first `n` key hashes for another seed, so a partition's rows split again */
  def reseed(h: Array[Int], n: Int, seed: Int): Unit =
    if seed != 0 then
      var i = 0
      while i < n do
        h(i) = GroupTable.fmix(h(i) ^ (seed * 0x9e3779b9))
        i += 1

  /** One partition's rows of a chunk, encoded */
  final class Block(val bytes: Array[Byte], val rows: Int, val estimatedBytes: Long)

  /** The rows of `batches` per partition, as one block each (null for none) */
  private def route(
      schema: Schema,
      keys: IndexedSeq[VExpr],
      longDecimalKeys: Boolean,
      batches: IndexedSeq[Batch],
      seed: Int
  ): Array[Block] =
    val parts  = AggSpill.Partitions
    val hasher = GroupTable(keys.map(_.ptype), 0, longDecimalKeys)
    val pieces = Array.fill(parts)(mutable.ArrayBuffer.empty[Batch])
    batches.foreach { deferred =>
      val c = deferred.compact()
      if c.size > 0 then
        val h = hasher.keyHashes(keys.map(_.eval(c)).toArray, c.size)
        reseed(h, c.size, seed)
        val idx = GroupTable.partitionRows(h, c.size, parts)
        var p   = 0
        while p < parts do
          if idx(p).length > 0 then
            pieces(p) += c.gather(idx(p), idx(p).length)
          p += 1
    }
    pieces.map { ps =>
      if ps.isEmpty then
        null
      else
        val b = Batch.concat(schema, ps.toSeq)
        Block(SpillBlocks.encode(b), b.size, MemoryContext.estimatedBytes(b))
    }

end AggSpillRouter

/**
  * Aggregation of spilled partitions: read back in waves that fit under the query's spill
  * threshold, aggregated in parallel within a wave, each file deleted once read. A wave's
  * reservations (its rows and groups) return when the next wave starts, after the consumer took its
  * groups
  */
object AggSpill:
  /** Spill partitions of an aggregation's input */
  val Partitions = 64

  /** Spill files aggregations of this process created (for tests and stats) */
  private[engine] val filesWritten = AtomicLong(0L)

  /** Spill files re-partitioned because they did not fit alone (for tests and stats) */
  private[engine] val filesSplit = AtomicLong(0L)

  /** Bytes a partition needs in memory: its rows, and about as much for its groups */
  private def need(f: SpillFile): Long = 2 * f.estimatedBytes

  /**
    * The groups of every partition in `files`. `aggregatePartition` runs one partition's rows under
    * the given context (in parallel when its wave holds it alone); `split` re-partitions a file
    * needing more than half the spill threshold (at `depth` 0 only; deeper, such a partition runs
    * anyway and meets the query's limit)
    */
  def aggregate(
      files: IndexedSeq[SpillFile],
      memory: MemoryContext,
      runner: TaskRunner,
      depth: Int,
      split: SpillFile => IndexedSeq[SpillFile],
      aggregatePartition: (Seq[Batch], MemoryContext, Boolean) => Seq[Batch]
  ): Iterator[Batch] =
    new Iterator[Batch]:
      private var queue                     = files.toList
      private var current: Iterator[Batch]  = Iterator.empty
      private var waveMemory: MemoryContext = null

      private def releaseWave(): Unit =
        if waveMemory != null then
          waveMemory.release()
          waveMemory = null

      def hasNext: Boolean =
        while !current.hasNext && queue.nonEmpty do
          releaseWave()
          current = nextWave()
        if !current.hasNext then
          releaseWave()
        current.hasNext

      def next(): Batch =
        if !hasNext then
          throw java.util.NoSuchElementException("No more groups")
        current.next()

      // A file needing more than half the threshold is split: alone it would take the query far
      // past the threshold, or leave no room for other partitions in its wave
      private def tooLarge(f: SpillFile): Boolean =
        depth == 0 && f.rows > 1 && need(f) > memory.spillThresholdBytes / 2

      private def nextWave(): Iterator[Batch] =
        // At least a quarter of the threshold per wave, even when other holders took the rest:
        // waves of one partition each would run the aggregation on one thread
        val headroom = math.max(memory.spillHeadroom, memory.spillThresholdBytes / 4)
        val head     = queue.head
        if tooLarge(head) then
          queue = queue.tail
          filesSplit.incrementAndGet()
          return AggSpill.aggregate(
            split(head),
            memory,
            runner,
            depth + 1,
            split,
            aggregatePartition
          )
        val wave  = mutable.ArrayBuffer(head)
        var bytes = need(head)
        queue = queue.tail
        while queue.nonEmpty && !tooLarge(queue.head) && bytes + need(queue.head) <= headroom do
          bytes += need(queue.head)
          wave += queue.head
          queue = queue.tail
        val ctx = memory.child()
        waveMemory = ctx
        // A wave of one partition aggregates it in parallel; a wider one runs a task per partition
        val parallel                      = wave.size > 1 && runner.parallelism > 1
        def run(f: SpillFile): Seq[Batch] =
          val rows =
            f.read()
              .map { b =>
                ctx.reserve(b)
                b
              }
              .toIndexedSeq
          f.delete()
          MemoryContext.withCurrent(ctx)(aggregatePartition(rows, ctx, !parallel))
        val groups =
          if parallel then
            val handles = wave.map(f => runner.submit(() => run(f)))
            // On a failure, wait for the other partitions: none may read after the query closes
            var failure: Throwable = null
            val out                = handles.map { h =>
              try
                h.get()
              catch
                case e: Throwable =>
                  if failure == null then
                    failure = e
                    handles.foreach(_.cancel())
                  Nil
            }
            if failure != null then
              throw failure
            out
          else
            wave.map(run)
        groups.iterator.flatten

      end nextWave

end AggSpill

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
  * Window spilling (`spill_window`): a window's rows depend only on their window partition, so its
  * input can be split by the PARTITION BY expressions every call shares ([[keysOf]]) and each split
  * evaluated alone. [[readInput]] buffers the input with [[MemoryContext.tryReserve]]; past the
  * query's spill threshold it routes the buffered and remaining rows by those keys into
  * [[AggSpill.Partitions]] files ([[AggSpillRouter]], reseeded per depth so that a file's rows
  * spread again over the in-memory window's parallel parts). [[evaluate]] then runs a fresh window
  * over one file at a time, deleting each once read. A window over a file splits once more when it
  * doesn't fit (depth 1); deeper, it holds its input and meets the query's limit.
  */
object WindowSpill:
  private val SeedBase = 0x2f6b

  /** Window inputs routed to spill files (for tests and stats) */
  private[engine] val inputsSpilled = AtomicLong(0L)

  /**
    * The PARTITION BY expressions shared by every call, by which the input can be split: none (no
    * spilling) when a call has no PARTITION BY or the calls share none, or past depth 1
    */
  def keysOf(calls: IndexedSeq[WindowCall], depth: Int): IndexedSeq[VExpr] =
    if depth > 1 || calls.isEmpty then
      IndexedSeq.empty
    else
      calls
        .head
        .partitionBy
        .distinct
        .filter(e => calls.forall(_.partitionBy.contains(e)))
        .toIndexedSeq

  /**
    * The bytes a spillable window reserves for a batch of its input: the batch, plus per row 8
    * bytes per call's output value and 16 bytes of row indexes and hashes
    */
  def reserveBytes(b: Batch, calls: Int): Long =
    MemoryContext.estimatedBytes(b) + b.size.toLong * (8L * calls + 16L)

  /**
    * The window's whole input (`Right`), reserved in `memory`, or, past the spill threshold, the
    * spill files of its rows split by `keys` (`Left`)
    */
  def readInput(
      child: Operator,
      keys: IndexedSeq[VExpr],
      calls: Int,
      memory: MemoryContext,
      runner: TaskRunner,
      depth: Int
  ): Either[IndexedSeq[SpillFile], Batch] =
    val input                  = memory.child()
    val buffered               = mutable.ArrayBuffer.empty[Batch]
    var router: AggSpillRouter = null
    try
      var x = child.next()
      while x != null do
        if x.size > 0 then
          if router != null then
            router.add(x)
          else
            val c = x.compact()
            if input.tryReserve(reserveBytes(c, calls)) then
              buffered += c
            else
              router = AggSpillRouter(child.schema, keys, false, memory, runner, SeedBase + depth)
              inputsSpilled.incrementAndGet()
              // The router reserves the buffered rows again until they are written
              input.release()
              var i = 0
              while i < buffered.size do
                router.add(buffered(i))
                buffered(i) = null
                i += 1
              buffered.clear()
              router.add(c)
        x = child.next()
    catch
      case e: Throwable =>
        // No routing task may write after the query's spill files close
        if router != null then
          router.abort()
        throw e
    end try
    if router != null then
      Left(router.finish())
    else if buffered.isEmpty then
      Right(Batch.empty(child.schema))
    else if buffered.size == 1 then
      Right(buffered.head)
    else
      Right(Batch.concat(child.schema, buffered.toSeq))

  end readInput

  /**
    * The rows of a window over each file, one file at a time: `windowOver(f)` makes the window over
    * file `f`, under `memory`, so that its reservations count there and return once it ends
    */
  def evaluate(
      files: IndexedSeq[SpillFile],
      memory: MemoryContext,
      windowOver: SpillFile => Operator
  ): Iterator[Batch] =
    new Iterator[Batch]:
      private var queue                  = files.toList
      private var current: Operator      = null
      private var currentFile: SpillFile = null
      private var nextBatch: Batch       = null

      def hasNext: Boolean =
        while nextBatch == null && (current != null || queue.nonEmpty) do
          if current == null then
            currentFile = queue.head
            queue = queue.tail
            current = MemoryContext.withCurrent(memory)(windowOver(currentFile))
          val b = current.next()
          if b == null then
            current.close()
            current = null
            currentFile.delete()
            currentFile = null
          else if b.size > 0 then
            nextBatch = b
        nextBatch != null

      def next(): Batch =
        if !hasNext then
          throw java.util.NoSuchElementException("No more window rows")
        val b = nextBatch
        nextBatch = null
        b

end WindowSpill

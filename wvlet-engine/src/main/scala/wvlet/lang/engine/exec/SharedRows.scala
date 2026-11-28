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
  * The rows of a result computed once and read by several readers (a shared CTE result): batches
  * held in memory, or the blocks of one spill file once they would take the query past its spill
  * threshold (`spill_cte_results`)
  */
sealed trait SharedRows:
  /** The rows held */
  def rows: Long

  /** Every row, in order */
  def iterator: Iterator[Batch]

  /** Independently readable non-empty parts, in order, each with its row count */
  def parts: IndexedSeq[(() => Iterator[Batch], Long)]

  /** The batches, when held in memory */
  def inMemory: Option[Seq[Batch]]

  /** Whether the rows are in a spill file */
  def isSpilled: Boolean = inMemory.isEmpty

  /** Drop the spill file, if any. Idempotent; reads afterwards return nothing */
  def free(): Unit

object SharedRows:
  /** Batches held in memory */
  final case class InMemory(batches: Seq[Batch]) extends SharedRows:
    def rows: Long                                       = batches.iterator.map(_.size.toLong).sum
    def iterator: Iterator[Batch]                        = batches.iterator
    def parts: IndexedSeq[(() => Iterator[Batch], Long)] =
      batches
        .iterator
        .filter(_.size > 0)
        .map(b => (() => Iterator.single(b), b.size.toLong))
        .toIndexedSeq

    def inMemory: Option[Seq[Batch]] = Some(batches)
    def free(): Unit                 = ()

  /** The blocks of a finished spill file, each a part */
  final class Spilled(val file: SpillFile) extends SharedRows:
    private lazy val blockRows = file.blockRows

    def rows: Long                                       = file.rows
    def iterator: Iterator[Batch]                        = file.readBlocks(0, blockRows.size)
    def parts: IndexedSeq[(() => Iterator[Batch], Long)] = blockRows
      .indices
      .filter(blockRows(_) > 0)
      .map(i => (() => file.readBlocks(i, i + 1), blockRows(i).toLong))

    def inMemory: Option[Seq[Batch]] = None
    def free(): Unit                 = file.delete()
    override def toString: String    = s"SharedRows.Spilled(${file})"

  /** The estimated in-memory bytes of a spill block: small batches are appended together */
  private[exec] val BlockBytes: Long = 4L << 20

  /** Results written to spill files, for tests */
  val resultsSpilled = AtomicLong(0L)

  /**
    * Drain every batch of `op`, compacted, reserving each in `ctx` ([[MemoryContext.tryReserve]]
    * with `spill`). Once a batch would take the query past its spill threshold, the batches held so
    * far and every later one go to one spill file, and `ctx` is released. A result of unspillable
    * columns (`AnyT`), or a query that can't spill, reserves as [[MemoryContext.drain]]
    */
  def drain(ctx: MemoryContext, op: Operator, spill: Boolean): SharedRows =
    if !spill || !ctx.canSpill || !SpillBlocks.spillable(op.schema) then
      InMemory(ctx.drain(op))
    else
      val held            = mutable.ArrayBuffer.empty[Batch]
      var file: SpillFile = null
      var pendingBytes    = 0L
      def flush(): Unit   =
        if held.nonEmpty then
          file.append(
            if held.size == 1 then
              held.head
            else
              Batch.concat(op.schema, held.toSeq)
          )
          held.clear()
          pendingBytes = 0L
      try
        var x = op.next()
        while x != null do
          val c     = x.compact()
          val bytes = MemoryContext.estimatedBytes(c)
          if file == null then
            if ctx.tryReserve(bytes) then
              held += c
            else
              file = ctx.spillSpace.newFile(op.schema)
              resultsSpilled.incrementAndGet()
              held.foreach(b =>
                if b.size > 0 then
                  file.append(b)
              )
              held.clear()
              ctx.release()
              held += c
              pendingBytes = bytes
          else if c.size > 0 then
            held += c
            pendingBytes += bytes
          if file != null && pendingBytes >= BlockBytes then
            flush()
          x = op.next()
        if file == null then
          InMemory(held.toSeq)
        else
          flush()
          file.finish()
          Spilled(file)
      catch
        case e: Throwable =>
          if file != null then
            file.delete()
          throw e
      end try

    end if

  end drain

end SharedRows

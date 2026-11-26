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
package wvlet.lang.engine.catalog

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*

/**
  * A table generated while it's scanned (TPC-H, TPC-DS): a sequence of blocks, each generating on
  * its own. Each block is a part of the scan, so the engine reads the parts in parallel and a scan
  * never holds the whole table.
  */
abstract class GeneratedSource extends TableSource:
  /** The number of blocks */
  protected def numBlocks: Int

  /** Block `b` with every column */
  protected def generateBlock(b: Int): Batch

  /** The rows of block `b`, or None when only generating it tells */
  protected def blockRowCount(b: Int): Option[Long]

  // The block's rows as batches of the engine's batch size, with the requested columns
  private def blockBatches(b: Int, columns: Option[Array[Int]]): Iterator[Batch] =
    val full = generateBlock(b)
    slices(columns.fold(full)(full.select))

  private def slices(b: Batch): Iterator[Batch] = Iterator
    .range(0, b.size, Batch.DefaultSize)
    .map(from => b.slice(from, math.min(from + Batch.DefaultSize, b.size)))

  private def columnIndexes(spec: ScanSpec): Option[Array[Int]] = spec
    .columns
    .map(_.map(c => schema.names.indexOf(c)).toArray)

  def scan(spec: ScanSpec): Iterator[Batch] =
    val cols = columnIndexes(spec)
    Iterator.range(0, numBlocks).flatMap(blockBatches(_, cols))

  override def canSplit: Boolean = numBlocks > 1

  override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    if numBlocks <= 1 then
      None
    else
      val cols   = columnIndexes(spec)
      val counts = IndexedSeq.tabulate(numBlocks)(blockRowCount)
      Some(
        ScanSplits(
          IndexedSeq.tabulate(numBlocks)(b => () => blockBatches(b, cols)),
          () => (),
          if counts.forall(_.isDefined) then
            counts.flatten
          else
            IndexedSeq.empty
        )
      )

  /** All rows of the table, in memory */
  def materialize: MemTable = MemTable(name, schema, scan(ScanSpec()).toSeq)

  /** The table in memory from its already generated blocks, with every column */
  private[catalog] def materialize(blocks: Seq[Batch]): MemTable = MemTable(
    name,
    schema,
    blocks.flatMap(slices)
  )

end GeneratedSource

/** Schema and column builders shared by the table generators (TPC-H, TPC-DS) */
private[catalog] object GenColumns:
  def schema(fields: (String, PType)*): Schema = Schema(
    fields.map((n, t) => Field(n, t)).toIndexedSeq
  )

  def longs(n: Int)(f: Int => Long): Vec  = LongVec(PType.Int64, Array.tabulate(n)(f), null)
  def ints(n: Int)(f: Int => Int): Vec    = IntVec(PType.Int32, Array.tabulate(n)(f), null)
  def strs(n: Int)(f: Int => String): Vec = StrVec(Array.tabulate(n)(f), null)

  /** Throws unless `sf` is in (0, max] */
  def checkScaleFactor(benchmark: String, sf: Double, max: Double): Unit =
    if !(sf > 0 && sf <= max) then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"${benchmark} scale factor must be in (0, ${max}]: ${sf}")

end GenColumns

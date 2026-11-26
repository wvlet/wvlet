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

import wvlet.lang.engine.vector.*

/**
  * A TPC-H table generated while it's scanned ([[TpchGen]]). Each block of the table is a part of
  * the scan that generates on its own, so the engine reads the parts in parallel and a scan never
  * holds the whole table. Row counts, key ranges, and distinct counts come from the specification,
  * so planning needs no generated data.
  */
class TpchSource private[catalog] (val table: String, gen: TpchGen) extends TableSource:
  import TpchGen.*

  val name: String   = table
  val schema: Schema = schemas(table)

  private val numBlocks = gen.numBlocks(table)

  override def estimatedRows: Option[Long] = Some(gen.estimatedRows(table))

  // The block's rows as batches of the engine's batch size, with the requested columns
  private def blockBatches(b: Int, columns: Option[Array[Int]]): Iterator[Batch] =
    val full = gen.block(table, b)
    val out  = columns.fold(full)(full.select)
    Iterator
      .range(0, out.size, Batch.DefaultSize)
      .map(from => out.slice(from, math.min(from + Batch.DefaultSize, out.size)))

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
      val counts = IndexedSeq.tabulate(numBlocks)(gen.blockRowCount(table, _))
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

  // Columns whose values grow with the block: the table's own key, numbered in block order
  private def blockKeyRange(column: String, b: Int): Option[(Long, Long)] =
    val start = gen.blockStart(b)
    val last  = start + gen.blockUnits(table, b) - 1
    (table, column) match
      case ("supplier", "s_suppkey") | ("customer", "c_custkey") | ("part", "p_partkey") |
          ("partsupp", "ps_partkey") =>
        Some((start + 1L, last + 1L))
      case ("orders", "o_orderkey") | ("lineitem", "l_orderkey") =>
        Some((gen.orderKey(start), gen.orderKey(last)))
      case _ =>
        None

  override def columnRange(column: String): Option[(Long, Long)] =
    blockKeyRange(column, 0) match
      case Some((lo, _)) =>
        blockKeyRange(column, numBlocks - 1).map((_, hi) => (lo, hi))
      case None =>
        column match
          case "r_regionkey" | "n_regionkey" =>
            Some((0L, 4L))
          case "n_nationkey" | "s_nationkey" | "c_nationkey" =>
            Some((0L, 24L))
          case "ps_suppkey" | "l_suppkey" =>
            Some((1L, gen.numSuppliers.toLong))
          case "o_custkey" =>
            Some((1L, gen.numCustomers.toLong))
          case "l_partkey" =>
            Some((1L, gen.numParts.toLong))
          case "p_size" =>
            Some((1L, 50L))
          case "ps_availqty" =>
            Some((1L, 9999L))
          case "o_shippriority" =>
            Some((0L, 0L))
          case "l_linenumber" =>
            Some((1L, 7L))
          case "o_orderdate" =>
            Some((StartDate.toLong, LastOrderDate.toLong))
          case "l_shipdate" =>
            Some((StartDate + 1L, LastOrderDate + 121L))
          case "l_commitdate" =>
            Some((StartDate + 30L, LastOrderDate + 90L))
          case "l_receiptdate" =>
            Some((StartDate + 2L, LastOrderDate + 151L))
          case _ =>
            None

  override def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] = blockKeyRange(
    column,
    0
  ).map { _ =>
    IndexedSeq.tabulate(numBlocks) { b =>
      val (lo, hi) = blockKeyRange(column, b).get
      // A lineitem block holds 4 line items per order on average
      val rows = gen.blockRowCount(table, b).getOrElse(gen.blockUnits(table, b) * 4L)
      ColumnPartRange(lo, hi, rows)
    }
  }

  override def estimatedDistinct(column: String): Option[Double] =
    val rows              = gen.estimatedRows(table).toDouble
    val n: Option[Double] =
      column match
        case "r_regionkey" | "r_name" | "n_regionkey" | "p_mfgr" | "c_mktsegment" |
            "o_orderpriority" =>
          Some(5)
        case "n_nationkey" | "n_name" | "s_nationkey" | "c_nationkey" | "p_brand" =>
          Some(25)
        case "s_suppkey" | "ps_suppkey" | "l_suppkey" =>
          Some(gen.numSuppliers)
        case "c_custkey" =>
          Some(gen.numCustomers)
        // Customers with a key divisible by 3 place no orders
        case "o_custkey" =>
          Some(gen.numCustomers * 2.0 / 3)
        case "p_partkey" | "ps_partkey" | "l_partkey" =>
          Some(gen.numParts)
        case "o_orderkey" | "l_orderkey" =>
          Some(gen.numOrders)
        case "p_size" =>
          Some(50)
        case "p_type" =>
          Some(150)
        case "p_container" =>
          Some(40)
        case "o_orderstatus" | "l_returnflag" =>
          Some(3)
        case "l_linestatus" =>
          Some(2)
        case "o_shippriority" =>
          Some(1)
        case "l_linenumber" | "l_shipmode" =>
          Some(7)
        case "l_shipinstruct" =>
          Some(4)
        case "o_clerk" =>
          Some(gen.numClerks)
        case _ =>
          None
    n.map(math.min(_, rows))

  end estimatedDistinct

  // Tables of the same name at the same scale factor hold the same rows
  override def key: Any = ("tpch", gen.sf, table)

  /** All rows of the table, in memory */
  def materialize: MemTable = MemTable(name, schema, scan(ScanSpec()).toSeq)

end TpchSource

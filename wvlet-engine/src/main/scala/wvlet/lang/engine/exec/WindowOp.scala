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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/** A frame bound: None means unbounded; Some(0) is the current row */
case class FrameSpec(rows: Boolean, start: Option[Long], end: Option[Long])

/**
  * One window function call: the function, its arguments, and the window definition. `frame` is
  * None for the SQL default frame (whole partition without ORDER BY, otherwise from the partition
  * start to the last peer of the current row)
  */
case class WindowCall(
    name: String,
    args: Seq[VExpr],
    partitionBy: Seq[VExpr],
    orderBy: Seq[SortKey],
    frame: Option[FrameSpec],
    ignoreNulls: Boolean,
    ptype: PType
):
  def sig: String =
    s"win:${name}(${args.map(_.sig).mkString(",")}) over(${partitionBy
        .map(_.sig)
        .mkString(",")};${orderBy
        .map(k =>
          s"${k.expr.sig}${
              if k.ascending then
                ""
              else
                " desc"
            }"
        )
        .mkString(",")};${frame})"

object WindowCall:
  val rankingFunctions: Set[String] = Set(
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value"
  )

  def resultType(name: String, argTypes: Seq[PType]): PType =
    name match
      case "row_number" | "rank" | "dense_rank" | "ntile" =>
        PType.Int64
      case "percent_rank" | "cume_dist" =>
        PType.Float64
      case "lag" | "lead" | "first_value" | "last_value" | "nth_value" =>
        argTypes.headOption.getOrElse(PType.Null)
      case other =>
        Aggregates.resultType(other, argTypes)

/** Appends one column per window call to the input rows (input order is not preserved) */
class WindowOp(val child: Operator, val calls: IndexedSeq[WindowCall], val schema: Schema)
    extends Operator:
  private var result: Iterator[Batch]  = null
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${calls.map(_.sig).mkString(", ")})"

  def next(): Batch =
    if result == null then
      result = compute()
    if result.hasNext then
      result.next()
    else
      null

  private def compute(): Iterator[Batch] =
    val all = Operator.materialize(child)
    val n   = all.size
    if n == 0 then
      return Iterator.empty
    val outCols = calls.map(c => evaluate(c, all))
    val out     = Batch(all.columns ++ outCols, n)
    Iterator.range(0, n, Batch.DefaultSize).map(from => out.slice(from, from + Batch.DefaultSize))

  /** Rows of each partition, sorted by the ORDER BY keys */
  private def partitions(c: WindowCall, all: Batch): Seq[Array[Int]] =
    val n                  = all.size
    val groups: Array[Int] =
      if c.partitionBy.isEmpty then
        new Array[Int](n)
      else
        val table = GroupTable(c.partitionBy.map(_.ptype).toIndexedSeq)
        table.groupIds(c.partitionBy.map(_.eval(all)).toArray, n)
    val numGroups =
      if n == 0 then
        0
      else
        groups.max + 1
    val buckets = Array.fill(numGroups)(scala.collection.mutable.ArrayBuilder.make[Int])
    var i       = 0
    while i < n do
      buckets(groups(i)) += i
      i += 1
    val keyVecs = c.orderBy.map(_.expr.eval(all)).toArray
    val cmp     = SortOp.comparator(keyVecs, c.orderBy)
    buckets
      .toSeq
      .map { b =>
        val rows = b.result().map(Integer.valueOf)
        if c.orderBy.nonEmpty then
          java.util.Arrays.sort(rows, cmp)
        rows.map(_.intValue())
      }

  private def evaluate(c: WindowCall, all: Batch): Vec =
    val n                              = all.size
    val out                            = new Array[Any](n)
    val args                           = c.args.map(_.eval(all)).toArray
    val keyVecs                        = c.orderBy.map(_.expr.eval(all)).toArray
    val cmp                            = SortOp.comparator(keyVecs, c.orderBy)
    def peers(a: Int, b: Int): Boolean = c.orderBy.isEmpty || cmp.compare(a, b) == 0

    partitions(c, all).foreach { rows =>
      val size = rows.length
      c.name match
        case "row_number" =>
          var k = 0
          while k < size do
            out(rows(k)) = (k + 1).toLong
            k += 1
        case "rank" | "dense_rank" | "percent_rank" =>
          var rank  = 1L
          var dense = 1L
          var k     = 0
          while k < size do
            if k > 0 && !peers(rows(k - 1), rows(k)) then
              rank = k + 1
              dense += 1
            out(rows(k)) =
              c.name match
                case "rank" =>
                  rank
                case "dense_rank" =>
                  dense
                case _ =>
                  if size <= 1 then
                    0.0
                  else
                    (rank - 1).toDouble / (size - 1)
            k += 1
        case "cume_dist" =>
          var k = 0
          while k < size do
            var e = k
            while e + 1 < size && peers(rows(k), rows(e + 1)) do
              e += 1
            val v = (e + 1).toDouble / size
            var x = k
            while x <= e do
              out(rows(x)) = v
              x += 1
            k = e + 1
        case "ntile" =>
          val buckets = ValueOps.toLong(args(0).get(rows.headOption.getOrElse(0)))
          var k       = 0
          val base    = size / buckets
          val extra   = size % buckets
          while k < size do
            // The first `extra` buckets have one more row
            val bucket =
              if k < extra * (base + 1) then
                k / (base + 1)
              else
                extra + (k - extra * (base + 1)) / math.max(1, base)
            out(rows(k)) = bucket + 1
            k += 1
        case "lag" | "lead" =>
          val offset =
            if args.length > 1 then
              ValueOps.toLong(args(1).get(rows.headOption.getOrElse(0))).toInt
            else
              1
          val default =
            if args.length > 2 then
              args(2)
            else
              null
          var k = 0
          while k < size do
            val j =
              if c.name == "lag" then
                k - offset
              else
                k + offset
            out(rows(k)) =
              if j >= 0 && j < size then
                args(0).get(rows(j))
              else if default != null then
                default.get(rows(k))
              else
                null
            k += 1
        case "first_value" | "last_value" | "nth_value" =>
          var k = 0
          while k < size do
            val (s, e) = frameRange(c, rows, k, peers)
            val nth    =
              if c.name == "nth_value" then
                ValueOps.toLong(args(1).get(rows(k))).toInt
              else
                1
            val candidates = (s to e).filter(x => !c.ignoreNulls || !args(0).isNull(rows(x)))
            out(rows(k)) =
              if candidates.isEmpty then
                null
              else if c.name == "first_value" then
                args(0).get(rows(candidates.head))
              else if c.name == "last_value" then
                args(0).get(rows(candidates.last))
              else if nth >= 1 && nth <= candidates.size then
                args(0).get(rows(candidates(nth - 1)))
              else
                null
            k += 1
        case agg =>
          aggregateOverFrames(c, rows, args, out, peers)
      end match
    }
    Vec.fromValues(c.ptype, out.toSeq)

  end evaluate

  /** Inclusive positions of the frame of the k-th row of a partition */
  private def frameRange(
      c: WindowCall,
      rows: Array[Int],
      k: Int,
      peers: (Int, Int) => Boolean
  ): (Int, Int) =
    val size = rows.length
    c.frame match
      case None =>
        if c.orderBy.isEmpty then
          (0, size - 1)
        else
          var e = k
          while e + 1 < size && peers(rows(k), rows(e + 1)) do
            e += 1
          (0, e)
      case Some(f) if f.rows =>
        val s = f.start.map(x => math.max(0L, k + x).toInt).getOrElse(0)
        val e = f.end.map(x => math.min(size - 1L, k + x).toInt).getOrElse(size - 1)
        (s, e)
      case Some(f) =>
        // RANGE frames: only UNBOUNDED / CURRENT ROW bounds (peer groups)
        var s = k
        while s > 0 && peers(rows(k), rows(s - 1)) do
          s -= 1
        var e = k
        while e + 1 < size && peers(rows(k), rows(e + 1)) do
          e += 1
        (
          if f.start.isEmpty then
            0
          else
            s
          ,
          if f.end.isEmpty then
            size - 1
          else
            e
        )
    end match

  end frameRange

  private def aggregateOverFrames(
      c: WindowCall,
      rows: Array[Int],
      args: Array[Vec],
      out: Array[Any],
      peers: (Int, Int) => Boolean
  ): Unit =
    val size = rows.length
    // Whole-partition frame: one aggregate for all rows
    val wholePartition = c.frame.isEmpty && c.orderBy.isEmpty
    if wholePartition then
      val v = aggregate(c, rows, 0, size - 1, args)
      rows.foreach(r => out(r) = v)
    else
      var k = 0
      while k < size do
        val (s, e) = frameRange(c, rows, k, peers)
        // Peers share the same value under the default frame
        out(rows(k)) = aggregate(c, rows, s, e, args)
        k += 1

  private def aggregate(c: WindowCall, rows: Array[Int], s: Int, e: Int, args: Array[Vec]): Any =
    val acc = Aggregates.accumulator(c.name, c.args.map(_.ptype), c.ptype)
    acc.ensureGroups(1)
    if s <= e then
      val idx = (s to e).map(rows).toArray
      acc.update(new Array[Int](idx.length), idx.length, args.map(_.gather(idx, idx.length)))
    acc.result(1).get(0)

end WindowOp

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

/**
  * Appends one column per window call to the input rows (input order is not preserved). For calls
  * with PARTITION BY over inputs of at least `minParallelRows` rows, the rows are split by the hash
  * of the partition key, so that every window partition falls in one part, and the parts are
  * evaluated as tasks of `runner`. Each row's value doesn't depend on how partitions are grouped
  * into parts, so the output equals the sequential one. Aggregates over frames that start at the
  * partition start are folded incrementally (see `aggregateRunning`); `runningFrames = false`
  * recomputes every frame instead, which tests use as the reference.
  */
class WindowOp(
    val child: Operator,
    val calls: IndexedSeq[WindowCall],
    val schema: Schema,
    runner: TaskRunner = TaskRunner.Sequential,
    minParallelRows: Int = WindowOp.MinParallelRows,
    runningFrames: Boolean = true
) extends Operator:
  private var result: Iterator[Batch]  = null
  override def children: Seq[Operator] = Seq(child)
  override def name: String            =
    if runner.parallelism > 1 then
      s"Window[parallelism=${runner.parallelism}]"
    else
      "Window"

  override protected def details = s"(${calls.map(_.sig).mkString(", ")})"

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

  /**
    * The window partitions among `rows` (ascending row indexes of the input), each sorted by the
    * ORDER BY keys. `partKeys` are the PARTITION BY values of every input row. `hashes` are their
    * key hashes when `rows` is one part of the input, or null when `rows` is the whole input
    */
  private def partitions(
      c: WindowCall,
      partKeys: Array[Vec],
      rows: Array[Int],
      cmp: SortOp.RowComparator,
      hashes: Array[Int]
  ): Seq[Array[Int]] =
    val n                  = rows.length
    val groups: Array[Int] =
      if c.partitionBy.isEmpty then
        new Array[Int](n)
      else
        val table = GroupTable(c.partitionBy.map(_.ptype).toIndexedSeq)
        if hashes == null then
          table.groupIds(partKeys, n)
        else if n == hashes.length then
          // One part holds every row, in input order
          table.groupIds(partKeys, n, hashes)
        else
          // A part of the rows: reuse the hashes that split the rows into parts
          val keys = new Array[Vec](partKeys.length)
          var k    = 0
          while k < keys.length do
            keys(k) = partKeys(k).gather(rows, n)
            k += 1
          val h = new Array[Int](n)
          var i = 0
          while i < n do
            h(i) = hashes(rows(i))
            i += 1
          table.groupIds(keys, n, h)
    val numGroups =
      if n == 0 then
        0
      else
        groups.max + 1
    // Counting sort of the rows by group, keeping the row order within a group
    val start = new Array[Int](numGroups + 1)
    var i     = 0
    while i < n do
      start(groups(i) + 1) += 1
      i += 1
    var g = 0
    while g < numGroups do
      start(g + 1) += start(g)
      g += 1
    val pos     = java.util.Arrays.copyOf(start, numGroups)
    val ordered = new Array[Int](n)
    i = 0
    while i < n do
      val gi = groups(i)
      ordered(pos(gi)) = rows(i)
      pos(gi) += 1
      i += 1
    val out = new Array[Array[Int]](numGroups)
    g = 0
    while g < numGroups do
      val part = java.util.Arrays.copyOfRange(ordered, start(g), start(g + 1))
      if c.orderBy.nonEmpty then
        IndexSort.sort(part, cmp)
      out(g) = part
      g += 1
    out.toSeq

  end partitions

  private def evaluate(c: WindowCall, all: Batch): Vec =
    val n                              = all.size
    val out                            = WindowOp.OutColumn(c.ptype, n)
    val args                           = c.args.map(_.eval(all)).toArray
    val keyVecs                        = c.orderBy.map(_.expr.eval(all)).toArray
    val cmp                            = SortOp.comparator(keyVecs, c.orderBy)
    def peers(a: Int, b: Int): Boolean = c.orderBy.isEmpty || cmp.compareRows(a, b) == 0
    val partKeys                       = c.partitionBy.map(_.eval(all)).toArray

    // Rows of distinct partitions write distinct slots of `out`, so parts can run concurrently
    def evalPart(rows: Array[Int], hashes: Array[Int]): Unit = partitions(
      c,
      partKeys,
      rows,
      cmp,
      hashes
    ).foreach(evalRows)

    def evalRows(rows: Array[Int]): Unit =
      val size = rows.length
      c.name match
        case "row_number" =>
          var k = 0
          while k < size do
            out.setLong(rows(k), k + 1)
            k += 1
        case "rank" | "dense_rank" | "percent_rank" =>
          var rank  = 1L
          var dense = 1L
          var k     = 0
          while k < size do
            if k > 0 && !peers(rows(k - 1), rows(k)) then
              rank = k + 1
              dense += 1
            c.name match
              case "rank" =>
                out.setLong(rows(k), rank)
              case "dense_rank" =>
                out.setLong(rows(k), dense)
              case _ =>
                out.setDouble(
                  rows(k),
                  if size <= 1 then
                    0.0
                  else
                    (rank - 1).toDouble / (size - 1)
                )
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
              out.setDouble(rows(x), v)
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
            out.setLong(rows(k), bucket + 1)
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
            if j >= 0 && j < size then
              out.copyFrom(rows(k), args(0), rows(j))
            else if default != null then
              out.copyFrom(rows(k), default, rows(k))
            else
              out.setNull(rows(k))
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
            val v = args(0)
            // The position of the picked row in the frame [s, e], or -1 for none
            var picked = -1
            if c.name == "last_value" then
              var x = e
              while x >= s && picked < 0 do
                if !c.ignoreNulls || !v.isNull(rows(x)) then
                  picked = x
                x -= 1
            else if nth >= 1 then
              var remaining = nth
              var x         = s
              while x <= e && picked < 0 do
                if !c.ignoreNulls || !v.isNull(rows(x)) then
                  remaining -= 1
                  if remaining == 0 then
                    picked = x
                x += 1
            if picked >= 0 then
              out.copyFrom(rows(k), v, rows(picked))
            else
              out.setNull(rows(k))
            k += 1
          end while
        case agg =>
          aggregateOverFrames(c, rows, args, out, peers)
      end match
    end evalRows

    if c.partitionBy.isEmpty || n < minParallelRows || runner.parallelism <= 1 then
      val rows = new Array[Int](n)
      var i    = 0
      while i < n do
        rows(i) = i
        i += 1
      evalPart(rows, null)
    else
      val hashes = GroupTable(c.partitionBy.map(_.ptype).toIndexedSeq).keyHashes(partKeys, n)
      val parts  = WindowOp.partitionRows(
        hashes,
        n,
        GroupTable.partitionCount(runner.parallelism),
        runner
      )
      parts
        .filter(_.nonEmpty)
        .map(rows => runner.submit(() => evalPart(rows, hashes)))
        .foreach(_.get())
    out.result()

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
      out: WindowOp.OutColumn,
      peers: (Int, Int) => Boolean
  ): Unit =
    val size = rows.length
    // Whole-partition frame: one aggregate for all rows
    val wholePartition = c.frame.isEmpty && c.orderBy.isEmpty
    if wholePartition then
      val v = aggregate(c, rows, 0, size - 1, args)
      var k = 0
      while k < size do
        out.copyFrom(rows(k), v, 0)
        k += 1
    else if runningFrames && WindowOp.isRunningFrame(c) then
      aggregateRunning(c, rows, args, out, peers)
    else
      var k = 0
      while k < size do
        val (s, e) = frameRange(c, rows, k, peers)
        // Peers share the same value under the default frame
        out.copyFrom(rows(k), aggregate(c, rows, s, e, args), 0)
        k += 1

  /**
    * Frames that start at the partition start, whose end never moves backwards: one accumulator
    * folds the rows between the previous frame end and the current one, so a partition costs
    * O(size). The rows are folded in the order the per-frame path folds them, and `result` doesn't
    * change the accumulator's state, so each value equals the per-frame one, floating-point sums
    * included. Rows with the same frame end share one result vector
    */
  private def aggregateRunning(
      c: WindowCall,
      rows: Array[Int],
      args: Array[Vec],
      out: WindowOp.OutColumn,
      peers: (Int, Int) => Boolean
  ): Unit =
    val size = rows.length
    val acc  = Aggregates.accumulator(c.name, c.args.map(_.ptype), c.ptype)
    acc.ensureGroups(1)
    // Bounds as frameRange reads them: a RANGE end other than UNBOUNDED is the last peer
    val toEnd   = c.frame.exists(_.end.isEmpty)
    val byPeers = !toEnd && c.frame.forall(!_.rows)
    val rowsEnd =
      c.frame match
        case Some(f) if f.rows && f.end.nonEmpty =>
          f.end.get
        case _ =>
          0L
    val groups    = new Array[Int](size)
    val in        = new Array[Vec](args.length)
    val idx       = new Array[Int](size)
    var fed       = 0 // rows(0 until fed) are folded into acc
    var lastEnd   = Int.MinValue
    var last: Vec = null
    var k         = 0
    while k < size do
      // The inclusive frame end of the k-th row, possibly before the partition start
      val e =
        if toEnd then
          size - 1
        else if byPeers then
          if k <= lastEnd then
            lastEnd
          else
            var x = k
            while x + 1 < size && peers(rows(k), rows(x + 1)) do
              x += 1
            x
        else
          math.max(-1L, math.min(size - 1L, k + rowsEnd)).toInt
      if last == null || e != lastEnd then
        val upto = math.max(fed, e + 1)
        if upto > fed then
          // Gather the newly framed rows straight from the input
          val n = upto - fed
          System.arraycopy(rows, fed, idx, 0, n)
          var a = 0
          while a < in.length do
            in(a) = args(a).gather(idx, n)
            a += 1
          acc.update(groups, n, in)
          fed = upto
        last = acc.result(1)
        lastEnd = e
      out.copyFrom(rows(k), last, 0)
      k += 1
    end while

  end aggregateRunning

  /** The aggregate of the frame rows(s..e), as a one-row vector */
  private def aggregate(c: WindowCall, rows: Array[Int], s: Int, e: Int, args: Array[Vec]): Vec =
    val acc = Aggregates.accumulator(c.name, c.args.map(_.ptype), c.ptype)
    acc.ensureGroups(1)
    if s <= e then
      val len = e - s + 1
      val idx = java.util.Arrays.copyOfRange(rows, s, e + 1)
      val in  = new Array[Vec](args.length)
      var a   = 0
      while a < in.length do
        in(a) = args(a).gather(idx, len)
        a += 1
      acc.update(new Array[Int](len), len, in)
    acc.result(1)

end WindowOp

object WindowOp:

  /**
    * Aggregates whose accumulator folds rows one at a time in row order and whose `result` leaves
    * its state unchanged, with a result that costs O(1). Aggregates that collect their rows
    * (string_agg, array_agg, quantiles, mode, ...) recompute each frame instead
    */
  private val runningAggregates: Set[String] = Set(
    "count_star",
    "count",
    "count_if",
    "countif",
    "approx_count_distinct",
    "count_distinct",
    "sum",
    "sum_if",
    "product",
    "avg",
    "mean",
    "min",
    "max",
    "arbitrary",
    "any_value",
    "first",
    "last",
    "max_by",
    "arg_max",
    "min_by",
    "arg_min",
    "bool_and",
    "every",
    "bool_or",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop"
  )

  /**
    * True when every frame of `c` starts at the partition start and the frame end never moves
    * backwards, and the aggregate can be folded incrementally. The whole-partition frame (no ORDER
    * BY, no frame) is handled separately
    */
  def isRunningFrame(c: WindowCall): Boolean =
    val startsAtPartitionStart =
      c.frame match
        case None =>
          c.orderBy.nonEmpty
        case Some(f) =>
          f.start.isEmpty
    startsAtPartitionStart && runningAggregates.contains(c.name.toLowerCase)

  /**
    * One window call's output column, written by row position. Results are kept in a primitive
    * array of the result type's vector layout (Bool, Int32 / Date, Int64 / Timestamp, Decimal,
    * Float64, Str) without boxing; other types are kept as values. `result()` equals
    * `Vec.fromValues(ptype, values)` of the same values. Tasks write distinct positions, and every
    * array is allocated up front, so concurrent writes never change the column's storage
    */
  final class OutColumn(ptype: PType, n: Int):
    import OutColumn.*

    private val layout: Int =
      ptype match
        case PType.Bool =>
          BoolLayout
        case PType.Int32 | PType.Date =>
          IntLayout
        case PType.Int64 | PType.Timestamp =>
          LongLayout
        case _: PType.Decimal =>
          DecimalLayout
        case PType.Float64 =>
          DoubleLayout
        case PType.Str =>
          StrLayout
        case _ =>
          ValueLayout

    private val decimalScale: Int =
      ptype match
        case d: PType.Decimal =>
          d.scale
        case _ =>
          0

    private val bools: Array[Boolean] =
      if layout == BoolLayout then
        new Array[Boolean](n)
      else
        null

    private val ints: Array[Int] =
      if layout == IntLayout then
        new Array[Int](n)
      else
        null

    // Int64 / Timestamp values, or unscaled Decimal values
    private val longs: Array[Long] =
      if layout == LongLayout || layout == DecimalLayout then
        new Array[Long](n)
      else
        null

    // Decimal values that don't fit in an unscaled Long, already rescaled
    private val bigDecimals: Array[AnyRef] =
      if layout == DecimalLayout then
        new Array[AnyRef](n)
      else
        null

    private val doubles: Array[Double] =
      if layout == DoubleLayout then
        new Array[Double](n)
      else
        null

    private val strs: Array[String] =
      if layout == StrLayout then
        new Array[String](n)
      else
        null

    private val values: Array[Any] =
      if layout == ValueLayout then
        new Array[Any](n)
      else
        null

    private val nulls: Array[Boolean] =
      if layout != ValueLayout then
        new Array[Boolean](n)
      else
        null

    def setNull(i: Int): Unit =
      if values != null then
        values(i) = null
      else
        nulls(i) = true

    def setLong(i: Int, v: Long): Unit =
      if longs != null && layout == LongLayout then
        longs(i) = v
      else
        update(i, v)

    def setDouble(i: Int, v: Double): Unit =
      if doubles != null then
        doubles(i) = v
      else
        update(i, v)

    /** Row j of `src`, copied without boxing when `src` has this column's type */
    def copyFrom(i: Int, src: Vec, j: Int): Unit =
      if src.isNull(j) then
        setNull(i)
      else
        src match
          case v: LongVec if longs != null && v.ptype == ptype =>
            longs(i) = v.values(j)
          case v: IntVec if ints != null && v.ptype == ptype =>
            ints(i) = v.values(j)
          case v: DoubleVec if doubles != null =>
            doubles(i) = v.values(j)
          case v: StrVec if strs != null =>
            strs(i) = v.values(j)
          case v: BoolVec if bools != null =>
            bools(i) = v.values(j)
          case _ =>
            update(i, src.get(j))

    /** Sets one engine value, converted as [[VecBuilder]] of this type would */
    def update(i: Int, v: Any): Unit =
      if v == null then
        setNull(i)
      else
        layout match
          case BoolLayout =>
            bools(i) = ValueOps.toBoolean(v)
          case IntLayout =>
            ints(i) = ValueOps.toInt(v)
          case LongLayout =>
            longs(i) = ValueOps.toLong(v)
          case DecimalLayout =>
            val d = ValueOps.rescale(ValueOps.toBigDecimal(v), decimalScale)
            if d.unscaledValue().bitLength() < 64 then
              longs(i) = d.unscaledValue().longValue()
            else
              bigDecimals(i) = d
          case DoubleLayout =>
            doubles(i) = ValueOps.toDouble(v)
          case StrLayout =>
            strs(i) = ValueOps.toStr(v)
          case _ =>
            values(i) = v

    def result(): Vec =
      if values != null then
        return Vec.fromValues(ptype, values.toSeq)
      var anyNull = false
      var i       = 0
      while !anyNull && i < n do
        anyNull = nulls(i)
        i += 1
      val ns =
        if anyNull then
          nulls
        else
          null
      layout match
        case BoolLayout =>
          BoolVec(bools, ns)
        case IntLayout =>
          IntVec(ptype, ints, ns)
        case LongLayout =>
          LongVec(ptype, longs, ns)
        case DecimalLayout =>
          decimalResult(ns)
        case DoubleLayout =>
          DoubleVec(doubles, ns)
        case _ =>
          StrVec(strs, ns)

    private def decimalResult(ns: Array[Boolean]): Vec =
      var anyBig = false
      var i      = 0
      while !anyBig && i < n do
        anyBig = bigDecimals(i) != null
        i += 1
      if !anyBig then
        LongVec(ptype, longs, ns)
      else
        // Replay the values into a builder, which switches to BigDecimal values as it does today
        val b = VecBuilder.DecimalBuilder(ptype.asInstanceOf[PType.Decimal], n)
        i = 0
        while i < n do
          if nulls(i) then
            b.addNull()
          else if bigDecimals(i) != null then
            b.addBig(bigDecimals(i).asInstanceOf[java.math.BigDecimal])
          else
            b.addUnscaled(longs(i))
          i += 1
        b.result()

  end OutColumn

  object OutColumn:
    private final val BoolLayout    = 0
    private final val IntLayout     = 1
    private final val LongLayout    = 2
    private final val DecimalLayout = 3
    private final val DoubleLayout  = 4
    private final val StrLayout     = 5
    private final val ValueLayout   = 6

  /**
    * [[GroupTable.partitionRows]] in two parallel passes over chunks of the rows: count each
    * chunk's rows per partition, then scatter each chunk at its offsets. Rows stay in ascending
    * order within a partition. `parts` must be a power of two of at least 2
    */
  def partitionRows(h: Array[Int], n: Int, parts: Int, runner: TaskRunner): Array[Array[Int]] =
    val shift           = 32 - Integer.numberOfTrailingZeros(parts)
    val chunks          = math.max(1, math.min(runner.parallelism, n / 4096))
    def lo(c: Int): Int = (n.toLong * c / chunks).toInt
    val counts          = (0 until chunks)
      .map { c =>
        runner.submit { () =>
          val cnt = new Array[Int](parts)
          var i   = lo(c)
          val hi  = lo(c + 1)
          while i < hi do
            cnt(h(i) >>> shift) += 1
            i += 1
          cnt
        }
      }
      .map(_.get())
    // offsets(c)(p): where chunk c's rows of partition p start
    val offsets = Array.ofDim[Int](chunks, parts)
    val sizes   = new Array[Int](parts)
    var c       = 0
    while c < chunks do
      var p = 0
      while p < parts do
        offsets(c)(p) = sizes(p)
        sizes(p) += counts(c)(p)
        p += 1
      c += 1
    val out = new Array[Array[Int]](parts)
    var p   = 0
    while p < parts do
      out(p) = new Array[Int](sizes(p))
      p += 1
    (0 until chunks)
      .map { c =>
        runner.submit { () =>
          val pos = offsets(c)
          var i   = lo(c)
          val hi  = lo(c + 1)
          while i < hi do
            val q = h(i) >>> shift
            out(q)(pos(q)) = i
            pos(q) += 1
            i += 1
        }
      }
      .foreach(_.get())
    out

  end partitionRows

  /**
    * Below this many input rows, window partitions are evaluated on the calling thread. The
    * wvlet.engine.parallelWindowMinRows system property overrides it (tests use it to exercise the
    * parallel path on small data)
    */
  def MinParallelRows: Int = sys
    .props
    .get("wvlet.engine.parallelWindowMinRows")
    .flatMap(_.toIntOption)
    .getOrElse(65536)

end WindowOp

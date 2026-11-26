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
  * recomputes every frame instead, which tests use as the reference. With `sortKernels`, a
  * partition ordered by one integer or date key is sorted as packed (key, position) longs
  * ([[WindowOp.sortByPrimitiveKey]]) instead of through the row comparator. With `sharePartitions`,
  * calls with the same PARTITION BY and ORDER BY are partitioned and sorted once
  * ([[WindowOp.callGroups]]). With `streamTopN` and a `topN`, the input is read in chunks that keep
  * only their own top rows ([[WindowTopN.keepTop]]), in tasks of `runner`, instead of being
  * materialized in full.
  */
class WindowOp(
    val child: Operator,
    val calls: IndexedSeq[WindowCall],
    val schema: Schema,
    runner: TaskRunner = TaskRunner.Sequential,
    minParallelRows: Int = WindowOp.MinParallelRows,
    runningFrames: Boolean = true,
    val topN: Option[WindowOp.TopN] = None,
    val runningKernels: Boolean = false,
    val sortKernels: Boolean = false,
    val sharePartitions: Boolean = false,
    val streamTopN: Boolean = false,
    topNChunkRows: Int = WindowTopN.ChunkRows
) extends Operator:
  private var result: Iterator[Batch]  = null
  override def children: Seq[Operator] = Seq(child)
  override def name: String            =
    if runner.parallelism > 1 then
      s"Window[parallelism=${runner.parallelism}]"
    else
      "Window"

  override protected def details =
    val top = topN
      .map(t =>
        s", top=${t.limit}${
            if t.ties then
              " with ties"
            else
              ""
          }"
      )
      .getOrElse("")
    val streamed =
      if topN.isDefined && streamTopN then
        ", streamed"
      else
        ""
    s"(${calls.map(_.sig).mkString(", ")}${top}${streamed})"

  def next(): Batch =
    if result == null then
      result = compute()
    if result.hasNext then
      result.next()
    else
      null

  private def compute(): Iterator[Batch] =
    val all =
      topN match
        case Some(t) if streamTopN =>
          topInput(t)
        case _ =>
          Operator.materialize(child)
    val n = all.size
    if n == 0 then
      return Iterator.empty
    // With topN, the rows the first call ranks within the limit: the only rows output
    val kept =
      if topN.isDefined then
        new Array[Boolean](n)
      else
        null
    val outCols = new Array[Vec](calls.size)
    // One group at a time, so only one group's arguments and sort keys stay in memory
    WindowOp
      .callGroups(calls, sharePartitions)
      .foreach { g =>
        val orderBy = calls(g.head).orderBy
        val keyVecs = orderBy.map(_.expr.eval(all)).toArray
        val cmp     = SortOp.comparator(keyVecs, orderBy)
        val evals   = g.map(i =>
          CallEval(
            calls(i),
            all,
            if i == 0 then
              kept
            else
              null
            ,
            cmp
          )
        )
        evaluateGroup(evals, all, keyVecs, cmp)
        g.indices.foreach(j => outCols(g(j)) = evals(j).out.result())
      }
    val full = Batch(all.columns ++ outCols, n)
    val out  =
      if kept == null then
        full
      else
        var count = 0
        var i     = 0
        while i < n do
          if kept(i) then
            count += 1
          i += 1
        val rows = new Array[Int](count)
        var j    = 0
        i = 0
        while i < n do
          if kept(i) then
            rows(j) = i
            j += 1
          i += 1
        full.gather(rows, count)
    val size = out.size
    Iterator
      .range(0, size, Batch.DefaultSize)
      .map(from => out.slice(from, from + Batch.DefaultSize))

  end compute

  /**
    * The input rows that can rank within `t` (see [[WindowTopN.keepTop]]), in input order: chunks
    * of about `topNChunkRows` input rows keep their own top rows in tasks of `runner` while the
    * next chunk is read. A row outside its chunk's top is outside the top of the whole input, as
    * the rows ranked before it in the chunk are ranked before it in the input too. Once a chunk
    * keeps most of its rows (many small partitions), the later chunks are kept whole
    */
  private def topInput(t: WindowOp.TopN): Batch =
    val c       = calls.head
    val tasks   = Seq.newBuilder[TaskRunner.Handle[Seq[Batch]]]
    val pending = Seq.newBuilder[Batch]
    var rows    = 0
    // Set by the chunk tasks
    val pruning                             = java.util.concurrent.atomic.AtomicBoolean(true)
    def keep(chunk: Seq[Batch]): Seq[Batch] =
      if !pruning.get() then
        chunk
      else
        val in   = chunk.map(_.size).sum
        val tops = chunk.map(b => WindowTopN.keepTop(b, c, t))
        val top  =
          if tops.size > 1 then
            // Each batch's top rows, then their top together
            Seq(WindowTopN.keepTop(Batch.concat(child.schema, tops), c, t))
          else
            tops
        if top.map(_.size).sum * 2 > in then
          pruning.set(false)
        top
    def submit(): Unit =
      val chunk = pending.result()
      pending.clear()
      rows = 0
      tasks += runner.submit(() => keep(chunk))
    var b = child.next()
    while b != null do
      if b.size > 0 then
        pending += b.compact()
        rows += b.size
        if rows >= topNChunkRows then
          submit()
      b = child.next()
    if rows > 0 then
      submit()
    val kept = tasks.result().flatMap(_.get()).filter(_.size > 0)
    if kept.isEmpty then
      Batch.empty(child.schema)
    else if kept.size == 1 then
      kept.head
    else
      Batch.concat(child.schema, kept)

  end topInput

  /**
    * The window partitions among `rows` (ascending row indexes of the input), each sorted by the
    * ORDER BY keys. `partKeys` are the PARTITION BY values of every input row. `hashes` are their
    * key hashes when `rows` is one part of the input, or null when `rows` is the whole input
    */
  private def partitions(
      c: WindowCall,
      partKeys: Array[Vec],
      rows: Array[Int],
      keyVecs: Array[Vec],
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
    WindowTopN
      .groupRows(groups, rows, n)
      .map { all =>
        val part =
          topN match
            case Some(t) =>
              WindowOp.topRows(all, t, cmp)
            case None =>
              all
        if c.orderBy.nonEmpty && !(
            sortKernels && keyVecs.length == 1 &&
              WindowOp.sortByPrimitiveKey(part, keyVecs(0), c.orderBy.head)
          )
        then
          IndexSort.sort(part, cmp)
        part
      }
      .toSeq

  end partitions

  /**
    * The state of one window call over the input `all`: its output column and its arguments. `cmp`
    * compares rows by the call's ORDER BY keys. `evalRows` writes the values of one sorted
    * partition, and marks its rows in `kept` unless null
    */
  private final class CallEval(
      val c: WindowCall,
      all: Batch,
      kept: Array[Boolean],
      cmp: SortOp.RowComparator
  ):
    val out: WindowOp.OutColumn        = WindowOp.OutColumn(c.ptype, all.size)
    val args: Array[Vec]               = c.args.map(_.eval(all)).toArray
    def peers(a: Int, b: Int): Boolean = c.orderBy.isEmpty || cmp.compareRows(a, b) == 0

    def evalRows(rows: Array[Int]): Unit =
      val size = rows.length
      if kept != null then
        // Rows of distinct partitions are distinct slots
        var k = 0
        while k < size do
          kept(rows(k)) = true
          k += 1
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

  end CallEval

  /**
    * Evaluates calls with the same PARTITION BY and ORDER BY (one call unless `sharePartitions`):
    * the rows are partitioned and sorted once, by `keyVecs` and `cmp`, and every call reads the
    * same sorted partitions. Each call's values equal the ones of evaluating it alone, as its
    * partitions are the same rows in the same order
    */
  private def evaluateGroup(
      evals: IndexedSeq[CallEval],
      all: Batch,
      keyVecs: Array[Vec],
      cmp: SortOp.RowComparator
  ): Unit =
    val first    = evals.head
    val c        = first.c
    val n        = all.size
    val partKeys = c.partitionBy.map(_.eval(all)).toArray

    // Rows of distinct partitions write distinct slots of `out`, so parts can run concurrently
    def evalPart(rows: Array[Int], hashes: Array[Int]): Unit = partitions(
      c,
      partKeys,
      rows,
      keyVecs,
      cmp,
      hashes
    ).foreach { part =>
      var e = 0
      while e < evals.length do
        evals(e).evalRows(part)
        e += 1
    }

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

  end evaluateGroup

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
    else if runningFrames && WindowOp.isSlidingFrame(c) && aggregateSliding(c, rows, args, out) then
      ()
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
    if runningKernels && WindowOp.runningPrimitive(c, rows, args, out, peers) then
      return
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

  /**
    * ROWS frames bounded on both sides. min / max / max_by / min_by go to
    * `aggregateSlidingExtreme`, which always applies. For counts and Int64 / Decimal sums, two
    * pointers add the rows entering the frame and remove the rows leaving it, so a partition costs
    * O(size) whatever the frame width. Sums wrap on overflow, which still gives the exact frame sum
    * when it fits in a Long. When the absolute values of the partition's summed rows add up to more
    * than a Long, some frame might overflow in the per-frame fold (failing an integer sum, or
    * switching a decimal sum to BigDecimal); this returns false without writing anything, and the
    * caller recomputes each frame as before
    */
  private def aggregateSliding(
      c: WindowCall,
      rows: Array[Int],
      args: Array[Vec],
      out: WindowOp.OutColumn
  ): Boolean =
    val name = c.name.toLowerCase
    if WindowOp.extremeAggregates.contains(name) then
      aggregateSlidingExtreme(c, rows, args, out)
      return true
    val size = rows.length
    val f    = c.frame.get
    // Bounds clamped to the partition size, so that k + bound can't overflow
    val lo    = math.max(-size.toLong, math.min(size.toLong, f.start.get))
    val hi    = math.max(-size.toLong, math.min(size.toLong, f.end.get))
    val isSum = name.startsWith("sum")
    // Whether each row is folded, or null when every row is (count_star, count())
    var counted: Array[Boolean] = null
    // Values of the summed rows, or null for counts
    var values: Array[Long] = null
    val isDecimal           = c.ptype.isDecimal
    name match
      case "count" if args.nonEmpty =>
        val v = args(0).gather(rows, size)
        counted = new Array[Boolean](size)
        var k = 0
        while k < size do
          counted(k) = !v.isNull(k)
          k += 1
      case "count_if" | "countif" =>
        counted = Kernels.trueMask(args(0).gather(rows, size))
      case "sum" | "sum_if" =>
        val v = args(0).gather(rows, size)
        counted = new Array[Boolean](size)
        if v.ptype != PType.Null then
          val m =
            if args.length > 1 then
              Kernels.trueMask(args(1).gather(rows, size))
            else
              null
          values =
            c.ptype match
              case d: PType.Decimal =>
                Kernels.asUnscaled(v, d.scale)
              case _ =>
                Kernels.asLongs(v)
          if values == null then
            return false
          var abs = 0L
          var k   = 0
          while k < size do
            if !v.isNull(k) && (m == null || m(k)) then
              val x = values(k)
              if x == Long.MinValue || abs > Long.MaxValue - math.abs(x) then
                return false
              abs += math.abs(x)
              counted(k) = true
            k += 1
      case _ =>
    end match

    // rows(s until e) are in the frame
    var s   = 0
    var e   = 0
    var cnt = 0L
    var sum = 0L
    var k   = 0
    while k < size do
      val ts = math.max(0L, math.min(size, k + lo)).toInt
      val te = math.max(0L, math.min(size, k + hi + 1)).toInt
      while e < te do
        if counted == null || counted(e) then
          cnt += 1
          if values != null then
            sum += values(e)
        e += 1
      while s < ts && s < e do
        if counted == null || counted(s) then
          cnt -= 1
          if values != null then
            sum -= values(s)
        s += 1
      if s < ts then
        // The frame starts past every fed row: rows before ts never enter a later frame
        s = ts
        e = ts
      val i = rows(k)
      if !isSum then
        out.setLong(i, cnt)
      else if cnt == 0 then
        out.setNull(i)
      else if isDecimal then
        out.setUnscaled(i, sum)
      else
        out.setLong(i, sum)
      k += 1
    end while
    true

  end aggregateSliding

  /**
    * min / max / max_by / min_by over ROWS frames bounded on both sides. The per-frame fold keeps
    * the first non-NULL value (or key) and replaces it only on a strict improvement, so it ends on
    * the leftmost best row of the frame. A monotonic deque of row positions finds that row for
    * every frame in O(size): a row enters at the back after dropping the rows it strictly beats,
    * and leaves at the front once it falls out of the frame, so the front is the leftmost best row.
    * For Float64 min / max, the fold compares with primitive `<` / `>`: a NaN never replaces a
    * value and is never replaced, so the result is NaN when the frame's first non-NULL value is
    * NaN, and otherwise the deque's winner over the non-NaN values.
    *
    * The winning rows are then fed to one accumulator of the aggregate, one group per row, so each
    * result is the per-frame accumulator's result over its winning row alone: the same value, NULLs
    * included (a frame without a winner feeds nothing)
    */
  private def aggregateSlidingExtreme(
      c: WindowCall,
      rows: Array[Int],
      args: Array[Vec],
      out: WindowOp.OutColumn
  ): Unit =
    val size  = rows.length
    val f     = c.frame.get
    val lo    = math.max(-size.toLong, math.min(size.toLong, f.start.get))
    val hi    = math.max(-size.toLong, math.min(size.toLong, f.end.get))
    val name  = c.name.toLowerCase
    val isBy  = name != "max" && name != "min"
    val isMax = name == "max" || name == "max_by" || name == "arg_max"
    // The compared values: the key of max_by / min_by
    val v = args(
      if isBy then
        1
      else
        0
    ).gather(rows, size)
    // Rows that can win the deque
    val eligible                 = new Array[Boolean](size)
    def notNull(k: Int): Boolean = v.nulls == null || !v.nulls(k)
    def eligibleNonNull(): Unit  =
      var k = 0
      while k < size do
        eligible(k) = notNull(k)
        k += 1
    def boxedValues(): Array[Any] =
      val o = new Array[Any](size)
      var k = 0
      while k < size do
        val x = v.get(k)
        o(k) = x
        eligible(k) = x != null
        k += 1
      o
    // The compared values, in exactly one of three layouts. ByKey compares non-NULL keys with
    // ValueOps.compare: Long / Int keys as Longs, doubles with Double.compare (a total order)
    val longs: Array[Long] =
      if isBy then
        v match
          case l: LongVec =>
            l.values
          case i: IntVec =>
            Kernels.asLongs(i)
          case _ =>
            null
      else
        c.ptype match
          case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp | PType.TimestampTz =>
            v match
              case l: LongVec =>
                l.values
              case other =>
                Kernels.asLongs(other)
          case PType.Float64 =>
            null
          case _ =>
            v match
              // One scale for the whole vector: comparing unscaled Longs equals comparing values
              case l: LongVec if l.ptype.isDecimal =>
                l.values
              case _ =>
                null
    // Float64 min / max: the frame's first non-NULL value wins when it is NaN
    val nanFirst               = !isBy && c.ptype == PType.Float64
    val doubles: Array[Double] =
      if nanFirst then
        Kernels.asDoubles(v)
      else
        null
    val objs: Array[Any] =
      if longs == null && doubles == null then
        boxedValues()
      else
        null
    val nonNull: Array[Boolean] =
      if nanFirst then
        new Array[Boolean](size)
      else
        null
    if longs != null then
      eligibleNonNull()
    else if nanFirst then
      var k = 0
      while k < size do
        nonNull(k) = notNull(k)
        eligible(k) = nonNull(k) && !java.lang.Double.isNaN(doubles(k))
        k += 1

    // Whether row a strictly beats row b
    def beats(a: Int, b: Int): Boolean =
      val cmp =
        if longs != null then
          java.lang.Long.compare(longs(a), longs(b))
        else if nanFirst then
          val x = doubles(a)
          val y = doubles(b)
          if x < y then
            -1
          else if x > y then
            1
          else
            0
        else
          ValueOps.compare(objs(a), objs(b))
      if isMax then
        cmp > 0
      else
        cmp < 0

    val dq      = new Array[Int](size)
    var head    = 0
    var tail    = 0
    val winners = new Array[Int](size)
    val groups  = new Array[Int](size)
    var m       = 0
    var e       = 0 // rows before e have been offered to the deque
    var p       = 0 // the first non-NULL row at or after the frame start, for nanFirst
    var k       = 0
    while k < size do
      val ts = math.max(0L, math.min(size, k + lo)).toInt
      val te = math.max(0L, math.min(size, k + hi + 1)).toInt
      if e < ts then
        // Rows before the frame start never enter a later frame
        e = ts
      while e < te do
        if eligible(e) then
          while tail > head && beats(e, dq(tail - 1)) do
            tail -= 1
          dq(tail) = e
          tail += 1
        e += 1
      while head < tail && dq(head) < ts do
        head += 1
      var w = -1
      if nanFirst then
        if p < ts then
          p = ts
        while p < te && !nonNull(p) do
          p += 1
        if p < te && java.lang.Double.isNaN(doubles(p)) then
          w = p
      if w < 0 && head < tail then
        w = dq(head)
      if w >= 0 then
        winners(m) = rows(w)
        groups(m) = k
        m += 1
      k += 1
    end while

    val acc = Aggregates.accumulator(c.name, c.args.map(_.ptype), c.ptype)
    acc.ensureGroups(size)
    val in = new Array[Vec](args.length)
    var a  = 0
    while a < in.length do
      in(a) = args(a).gather(winners, m)
      a += 1
    acc.update(groups, m, in)
    val res = acc.result(size)
    k = 0
    while k < size do
      out.copyFrom(rows(k), res, k)
      k += 1

  end aggregateSlidingExtreme

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
    * The indexes of `calls`, grouped by equal PARTITION BY and ORDER BY in the order of each
    * group's first call; one group per call unless `share`. Expressions are compared with `==`, not
    * `sig`: the signature of a row function leaves out its closure
    */
  def callGroups(calls: IndexedSeq[WindowCall], share: Boolean): Seq[IndexedSeq[Int]] =
    if !share then
      calls.indices.map(IndexedSeq(_))
    else
      val groups = scala
        .collection
        .mutable
        .LinkedHashMap
        .empty[(Seq[VExpr], Seq[SortKey]), IndexedSeq[Int]]
      calls
        .indices
        .foreach { i =>
          val key = (calls(i).partitionBy, calls(i).orderBy)
          groups(key) = groups.getOrElse(key, IndexedSeq.empty) :+ i
        }
      groups.values.toSeq

  /**
    * A running count, sum, min, or max (a frame from the partition start) over `rows`, folded in
    * primitive values: count(*) and count(x) over any column, sum over Int64, Decimal (of the
    * result scale), and Float64 columns, min / max over Int64 and Decimal columns. Each row's value
    * is the fold of the rows up to its frame end, as `aggregateRunning` computes it with an
    * accumulator. The caller passes only running frames ([[isRunningFrame]]). Returns false,
    * possibly after writing some rows, for other calls and columns, and when a Long sum overflows;
    * the caller then computes every row of the partition
    */
  def runningPrimitive(
      c: WindowCall,
      rows: Array[Int],
      args: Array[Vec],
      out: OutColumn,
      peers: (Int, Int) => Boolean
  ): Boolean =
    val name = c.name.toLowerCase
    val arg  = args.headOption.orNull
    val kind =
      name match
        case "count" | "count_star" if c.ptype == PType.Int64 && args.length <= 1 =>
          0
        case "sum" if args.length == 1 =>
          (arg, c.ptype) match
            case (v: LongVec, PType.Int64) if v.ptype == PType.Int64 =>
              1
            case (v: LongVec, d: PType.Decimal)
                if v.ptype.isInstanceOf[PType.Decimal] &&
                  v.ptype.asInstanceOf[PType.Decimal].scale == d.scale =>
              2
            case (_: DoubleVec, PType.Float64) =>
              3
            case _ =>
              -1
        case "min" | "max" if args.length == 1 =>
          arg match
            case v: LongVec if v.ptype == c.ptype =>
              if c.ptype == PType.Int64 then
                4
              else if c.ptype.isInstanceOf[PType.Decimal] then
                5
              else
                -1
            case _ =>
              -1
        case _ =>
          -1
    if kind < 0 then
      return false
    val toEnd   = c.frame.exists(_.end.isEmpty)
    val byPeers = !toEnd && c.frame.forall(!_.rows)
    val rowsEnd =
      c.frame match
        case Some(f) if f.rows && f.end.nonEmpty =>
          f.end.get
        case _ =>
          0L
    val size  = rows.length
    val longs =
      arg match
        case v: LongVec =>
          v.values
        case _ =>
          null
    val doubles =
      arg match
        case v: DoubleVec =>
          v.values
        case _ =>
          null
    val isMax    = name == "max"
    var count    = 0L // non-NULL rows folded (all rows for count(*))
    var l        = 0L
    var d        = 0.0
    var fed      = 0
    var lastEnd  = Int.MinValue
    var k        = 0
    var overflow = false
    while k < size && !overflow do
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
      while fed <= e && !overflow do
        val r = rows(fed)
        if arg == null || !arg.isNull(r) then
          kind match
            case 0     =>
            case 1 | 2 =>
              val s = l + longs(r)
              // Overflow when both operands have the sign the sum lacks
              if ((l ^ s) & (longs(r) ^ s)) < 0 then
                overflow = true
              l = s
            case 3 =>
              d += doubles(r)
            case _ =>
              val v = longs(r)
              if count == 0 || (isMax && v > l) || (!isMax && v < l) then
                l = v
          count += 1
        fed += 1
      lastEnd = e
      val i = rows(k)
      if kind == 0 then
        out.setLong(i, count)
      else if count == 0 then
        out.setNull(i)
      else
        kind match
          case 1 | 4 =>
            out.setLong(i, l)
          case 2 | 5 =>
            out.setUnscaled(i, l)
          case _ =>
            out.setDouble(i, d)
      k += 1
    end while
    !overflow

  end runningPrimitive

  /**
    * Keep only the rows of each partition that rank within `limit` by the ORDER BY keys: the first
    * `limit` rows, and with `ties` every row tied with the last of them (rank() <= limit). Valid
    * when every call is a row_number() or rank() over the same window, whose values depend only on
    * the rows before them, and a filter above drops the other rows
    */
  case class TopN(limit: Long, ties: Boolean)

  /**
    * Sorts the row indexes `part` by one Int32, Date, or Int64 key the way [[IndexSort]] does with
    * the key's comparator (stable, with the key's direction and NULL placement), but as one
    * `Arrays.sort` of longs packing each row's key code above its position in `part`. The code is
    * the key's offset from the partition's minimum (from its maximum when descending), with NULLs
    * coded below or above every value; equal codes keep their positions' order. False, leaving
    * `part` unchanged, when the key is another type or the codes and positions don't fit in 63 bits
    */
  def sortByPrimitiveKey(part: Array[Int], key: Vec, k: SortKey): Boolean =
    val n = part.length
    if n <= 1 then
      return true
    val (ints, longs, nulls) =
      key match
        case v: IntVec =>
          (v.values, null, v.nulls)
        case v: LongVec =>
          (null, v.values, v.nulls)
        case _ =>
          return false
    def value(row: Int): Long =
      if ints != null then
        ints(row).toLong
      else
        longs(row)
    def isNull(row: Int): Boolean = nulls != null && nulls(row)
    var min                       = Long.MaxValue
    var max                       = Long.MinValue
    var hasNull                   = false
    var i                         = 0
    while i < n do
      val row = part(i)
      if isNull(row) then
        hasNull = true
      else
        val x = value(row)
        if x < min then
          min = x
        if x > max then
          max = x
      i += 1
    if min > max then
      // Only NULLs: every row is a peer, so the order stays
      return true
    val range = max - min
    if range < 0 then
      // max - min overflows
      return false
    val posBits = 32 - Integer.numberOfLeadingZeros(n - 1)
    // Codes are 0..range+1: one slot for NULLs below or above the values
    val nullCode =
      if k.nullsFirst then
        0L
      else
        range + 1
    val shift =
      if hasNull && k.nullsFirst then
        1L
      else
        0L
    if range >= (1L << (62 - posBits)) - 1 then
      return false
    val packed = new Array[Long](n)
    i = 0
    while i < n do
      val row  = part(i)
      val code =
        if isNull(row) then
          nullCode
        else if k.ascending then
          value(row) - min + shift
        else
          max - value(row) + shift
      packed(i) = (code << posBits) | i
      i += 1
    java.util.Arrays.sort(packed)
    val orig = part.clone()
    val mask = (1L << posBits) - 1
    i = 0
    while i < n do
      part(i) = orig((packed(i) & mask).toInt)
      i += 1
    true

  end sortByPrimitiveKey

  /**
    * The rows of `part` within the top `t.limit` by `cmp`, in the order of `part`: a bounded
    * max-heap finds the limit-th row, then with ties every row not after it is kept
    */
  def topRows(part: Array[Int], t: TopN, cmp: SortOp.RowComparator): Array[Int] =
    val n = part.length
    if t.limit >= n then
      return part
    if t.limit <= 0 then
      return Array.emptyIntArray
    val k    = t.limit.toInt
    val heap = new Array[Int](k)
    // Rows in the ORDER BY order, then in input order (`part` is ascending), as the stable sort
    // without a top-N orders them
    def after(a: Int, b: Int): Boolean =
      val c = cmp.compareRows(a, b)
      c > 0 || c == 0 && a > b
    // Max-heap by `after`: heap(0) is the last of the k first rows seen so far
    def siftDown(from: Int): Unit =
      var i    = from
      var done = false
      while !done do
        val l       = 2 * i + 1
        var largest = i
        if l < k && after(heap(l), heap(largest)) then
          largest = l
        if l + 1 < k && after(heap(l + 1), heap(largest)) then
          largest = l + 1
        if largest == i then
          done = true
        else
          val x = heap(i)
          heap(i) = heap(largest)
          heap(largest) = x
          i = largest
    System.arraycopy(part, 0, heap, 0, k)
    var i = k / 2 - 1
    while i >= 0 do
      siftDown(i)
      i -= 1
    i = k
    while i < n do
      if after(heap(0), part(i)) then
        heap(0) = part(i)
        siftDown(0)
      i += 1
    if !t.ties then
      // In input order, so that the stable sort numbers tied rows as it does without a top-N
      java.util.Arrays.sort(heap)
      heap
    else
      val last = heap(0)
      val out  = Array.newBuilder[Int]
      out.sizeHint(k)
      i = 0
      while i < n do
        if cmp.compareRows(part(i), last) <= 0 then
          out += part(i)
        i += 1
      out.result()

  end topRows

  /**
    * The top-N a filter `cond` over the output of a window op with `calls` over `width` input
    * columns allows: when every call is a row_number() or rank() over the same window with ORDER
    * BY, and a conjunct of `cond` bounds one call's value by a constant (`r <= k`, `r < k`,
    * `r = k`)
    */
  def topNOf(calls: Seq[WindowCall], width: Int, cond: VExpr): Option[TopN] =
    val first                              = calls.headOption
    def sameWindow(c: WindowCall): Boolean = first.exists { f =>
      c.partitionBy.map(_.sig) == f.partitionBy.map(_.sig) &&
      c.orderBy.map(k => (k.expr.sig, k.ascending, k.nullsFirst)) ==
        f.orderBy.map(k => (k.expr.sig, k.ascending, k.nullsFirst))
    }
    val eligible =
      calls.nonEmpty &&
        calls.forall(c => (c.name == "row_number" || c.name == "rank") && sameWindow(c)) &&
        first.exists(_.orderBy.nonEmpty)
    def conjuncts(e: VExpr): List[VExpr] =
      e match
        case AndExpr(l, r) =>
          conjuncts(l) ++ conjuncts(r)
        case other =>
          List(other)
    def limitOf(e: VExpr): Option[Long] = rankBound(e, _ >= width).map(_._2)
    if !eligible then
      None
    else
      conjuncts(cond)
        .flatMap(limitOf)
        .minOption
        .map(l => TopN(math.max(0L, l), ties = calls.exists(_.name == "rank")))

  end topNOf

  /**
    * The column index and the largest value a conjunct `e` allows for a ranking column (one that
    * `ranked` accepts) bounded by an integer constant: `r <= k` and `r = k` allow k, `r < k` allows
    * k - 1, either way round
    */
  def rankBound(e: VExpr, ranked: Int => Boolean): Option[(Int, Long)] =
    def bound(op: CmpOp, k: Long): Option[Long] =
      op match
        case CmpOp.Le | CmpOp.Eq =>
          Some(k)
        case CmpOp.Lt =>
          Some(k - 1)
        case _ =>
          None
    e match
      case Compare(op, ColRef(i, _, _), Const(v: (Int | Long), _)) if ranked(i) =>
        bound(op, v.asInstanceOf[Number].longValue).map(i -> _)
      case Compare(op, Const(v: (Int | Long), _), ColRef(i, _, _)) if ranked(i) =>
        bound(op.flip, v.asInstanceOf[Number].longValue).map(i -> _)
      case _ =>
        None

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
    * Aggregates whose frame result is one leftmost best row, which `aggregateSlidingExtreme` finds
    */
  private[exec] val extremeAggregates: Set[String] = Set(
    "min",
    "max",
    "max_by",
    "arg_max",
    "min_by",
    "arg_min"
  )

  /**
    * True when `c` is a count, a sum with an Int64 or Decimal result, or min / max / max_by /
    * min_by over a ROWS frame bounded on both sides, which `aggregateSliding` folds with exact adds
    * and removes (or a monotonic deque for min / max). Float64 sums and avg stay per frame:
    * removing a double changes the result bits
    */
  def isSlidingFrame(c: WindowCall): Boolean =
    val bounded  = c.frame.exists(f => f.rows && f.start.nonEmpty && f.end.nonEmpty)
    def exactSum = c.ptype == PType.Int64 || c.ptype.isDecimal
    val name     = c.name.toLowerCase
    bounded && (
      name match
        case "min" | "max" =>
          c.args.size == 1
        case _ if extremeAggregates.contains(name) =>
          c.args.size == 2
        case "count_star" =>
          true
        case "count" =>
          c.args.size <= 1
        case "count_if" | "countif" =>
          c.args.size == 1
        case "sum" =>
          c.args.size == 1 && exactSum
        case "sum_if" =>
          c.args.size == 2 && exactSum
        case _ =>
          false
    )

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
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
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

    /** Sets an unscaled value of this column's Decimal type */
    def setUnscaled(i: Int, v: Long): Unit =
      assert(layout == DecimalLayout, s"Not a Decimal column: ${ptype}")
      longs(i) = v

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

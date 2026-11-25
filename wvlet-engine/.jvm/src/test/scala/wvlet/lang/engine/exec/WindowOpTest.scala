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
import wvlet.uni.test.UniTest

class WindowOpTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(
    IndexedSeq(Field("g", PType.Int64), Field("s", PType.Str), Field("v", PType.Int64))
  )

  /** 10 batches of 1000 rows: about 100 partitions by (g, s), NULL keys included */
  private val batches: Seq[Batch] = (0 until 10).map { b =>
    val n = 1000
    val r = Array.tabulate(n)(i => b * n + i)
    Batch(
      Array(
        LongVec(PType.Int64, r.map(i => (i % 50).toLong), r.map(_ % 97 == 0)),
        StrVec(r.map(i => s"s${i % 2}"), r.map(_ % 89 == 0)),
        LongVec(PType.Int64, r.map(i => ((i * 7919) % 101).toLong), null)
      ),
      n
    )
  }

  private val g     = ColRef(0, PType.Int64, "g")
  private val s     = ColRef(1, PType.Str, "s")
  private val v     = ColRef(2, PType.Int64, "v")
  private val order = Seq(SortKey(v, ascending = true, nullsFirst = false))

  private def call(
      name: String,
      args: Seq[VExpr],
      partitionBy: Seq[VExpr],
      orderBy: Seq[SortKey],
      frame: Option[FrameSpec],
      ptype: PType
  ) = WindowCall(name, args, partitionBy, orderBy, frame, false, ptype)

  private val calls = IndexedSeq(
    call("row_number", Nil, Seq(g, s), order, None, PType.Int64),
    call("rank", Nil, Seq(g), order, None, PType.Int64),
    call("sum", Seq(v), Seq(g, s), order, Some(FrameSpec(true, Some(-2), Some(0))), PType.Int64),
    call("lag", Seq(v), Seq(s), order, None, PType.Int64),
    call("count", Seq(v), Seq(g), Nil, None, PType.Int64),
    // No PARTITION BY: stays sequential
    call("row_number", Nil, Nil, order, None, PType.Int64)
  )

  private val outSchema = Schema(
    schema.fields ++ calls.indices.map(i => Field(s"w${i}", calls(i).ptype))
  )

  private def rows(parallel: Boolean): Seq[String] =
    val op = WindowOp(
      BatchesOp(schema, batches),
      calls,
      outSchema,
      if parallel then
        runner
      else
        TaskRunner.Sequential
      ,
      minParallelRows = 0
    )
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
    finally op.close()

  test("should evaluate partitioned windows in parallel with the sequential results") {
    val expected = rows(parallel = false)
    val actual   = rows(parallel = true)
    actual.size shouldBe 10000
    actual shouldBe expected
  }

  test("should partition rows in parallel as GroupTable.partitionRows does") {
    val n        = 100000
    val keys     = LongVec(PType.Int64, Array.tabulate(n)(i => (i % 40000).toLong), null)
    val h        = GroupTable(IndexedSeq(PType.Int64)).keyHashes(Array(keys), n)
    val expected = GroupTable.partitionRows(h, n, 16).map(_.toSeq).toSeq
    WindowOp.partitionRows(h, n, 16, runner).map(_.toSeq).toSeq shouldBe expected
    WindowOp.partitionRows(h, 10, 16, runner).map(_.toSeq).toSeq shouldBe
      GroupTable.partitionRows(h, 10, 16).map(_.toSeq).toSeq
  }

  /**
    * Same vector class, values, and NULL mask (null when there is no NULL). Values are compared
    * with `equals`, so NaN matches NaN while -0.0 and 0.0, or 1.5 and 1.50, differ
    */
  private def sameVec(actual: Vec, expected: Vec): Unit =
    actual.getClass shouldBe expected.getClass
    actual.ptype shouldBe expected.ptype
    actual.size shouldBe expected.size
    Option(actual.nulls).map(_.toSeq) shouldBe Option(expected.nulls).map(_.toSeq)
    def show(v: Any) = s"${v} (${Option(v).map(_.getClass.getSimpleName).getOrElse("null")})"
    (0 until actual.size)
      .find(i => !java.util.Objects.equals(actual.get(i), expected.get(i)))
      .foreach(i =>
        s"row ${i}: ${show(actual.get(i))}" shouldBe s"row ${i}: ${show(expected.get(i))}"
      )

  test("should build typed output columns equal to Vec.fromValues") {
    val dec                           = PType.Decimal(38, 2)
    val small                         = java.math.BigDecimal("12.34")
    val huge                          = java.math.BigDecimal("123456789012345678901234.5")
    val cases: Seq[(PType, Seq[Any])] = Seq(
      PType.Bool      -> Seq(true, false, true),
      PType.Int32     -> Seq(1, -2, 3),
      PType.Date      -> Seq(19000, 19001, 19002),
      PType.Int64     -> Seq(1L, 2L, 3L),
      PType.Timestamp -> Seq(1000L, 2000L, 3000L),
      PType.Float64   -> Seq(1.5, -0.25, 3.0),
      PType.Str       -> Seq("a", "", "c"),
      dec             -> Seq(small, java.math.BigDecimal("1.5"), java.math.BigDecimal("-7")),
      // One value overflows the unscaled Long, so the column falls back to BigDecimal values
      dec        -> Seq(small, huge, java.math.BigDecimal("3")),
      PType.AnyT -> Seq(Seq(1), Seq(2, 3), Seq.empty)
    )
    for
      (t, vs)  <- cases
      withNull <- Seq(false, true)
    do
      val values =
        if withNull then
          vs.updated(1, null)
        else
          vs
      val src = Vec.fromValues(t, values)
      val n   = values.size
      // Typed copies from a source vector of the same type
      val copied = WindowOp.OutColumn(t, n)
      (0 until n).foreach(i => copied.copyFrom(i, src, i))
      sameVec(copied.result(), src)
      // Engine values, converted as the builder does
      val updated = WindowOp.OutColumn(t, n)
      (0 until n).foreach(i => updated.update(i, values(i)))
      sameVec(updated.result(), src)
  }

  test("should convert copies from a vector of another type as Vec.fromValues does") {
    val ints = IntVec(PType.Int32, Array(1, 2, 3), Array(false, true, false))
    for t <- Seq(PType.Int64, PType.Float64, PType.Decimal(10, 2), PType.Str) do
      val out = WindowOp.OutColumn(t, 3)
      (0 until 3).foreach(i => out.copyFrom(i, ints, i))
      sameVec(out.result(), Vec.fromValues(t, (0 until 3).map(ints.get)))
  }

  test("should evaluate first_value, last_value, and nth_value with IGNORE NULLS") {
    val x = LongVec(
      PType.Int64,
      Array(0L, 10L, 0L, 30L, 40L),
      Array(true, false, true, false, false)
    )
    val o     = LongVec(PType.Int64, Array(1L, 2L, 3L, 4L, 5L), null)
    val in    = Schema(IndexedSeq(Field("x", PType.Int64), Field("o", PType.Int64)))
    val xr    = ColRef(0, PType.Int64, "x")
    val ord   = Seq(SortKey(ColRef(1, PType.Int64, "o"), ascending = true, nullsFirst = false))
    val whole = Some(FrameSpec(true, None, None))
    def win(name: String, args: Seq[VExpr], ignoreNulls: Boolean) = WindowCall(
      name,
      args,
      Nil,
      ord,
      whole,
      ignoreNulls,
      PType.Int64
    )
    val two = Const(2L, PType.Int64)
    val cs  = IndexedSeq(
      win("first_value", Seq(xr), false),
      win("first_value", Seq(xr), true),
      win("last_value", Seq(xr), false),
      win("nth_value", Seq(xr, two), true),
      win("nth_value", Seq(xr, Const(9L, PType.Int64)), true)
    )
    val op = WindowOp(
      BatchesOp(in, Seq(Batch(Array(x, o), 5))),
      cs,
      Schema(in.fields ++ cs.indices.map(i => Field(s"w${i}", PType.Int64)))
    )
    try
      val b = op.drain().head
      (2 until 7).map(c => b.columns(c).get(0)) shouldBe Seq(null, 10L, 40L, 30L, null)
    finally
      op.close()
  }

  test("should sort row indexes stably as java.util.Arrays.sort does") {
    val rnd = scala.util.Random(42)
    for n <- Seq(0, 1, 2, 31, 32, 33, 100, 1000, 5000) do
      val keys = LongVec(
        PType.Int64,
        Array.fill(n)(rnd.nextInt(20).toLong),
        Array.fill(n)(rnd.nextInt(10) == 0)
      )
      val cmp = SortOp.comparator(
        Array(keys),
        Seq(SortKey(ColRef(0, PType.Int64, "k"), ascending = false, nullsFirst = true))
      )
      // Start from a shuffled order, so stability is checked against the input order
      val idx   = rnd.shuffle((0 until n).toSeq).toArray
      val boxed = idx.map(Integer.valueOf)
      java.util.Arrays.sort(boxed, cmp)
      IndexSort.sort(idx, cmp)
      idx.toSeq shouldBe boxed.toSeq.map(_.intValue())
  }

  test("should assign the same group ids from precomputed key hashes") {
    val n    = 5000
    val keys = Array[Vec](
      LongVec(PType.Int64, Array.tabulate(n)(i => (i % 37).toLong), Array.tabulate(n)(_ % 11 == 0)),
      StrVec(Array.tabulate(n)(i => s"k${i % 3}"), null)
    )
    val types = IndexedSeq(PType.Int64, PType.Str)
    val h     = GroupTable(types).keyHashes(keys, n)
    GroupTable(types).groupIds(keys, n, h).toSeq shouldBe GroupTable(types).groupIds(keys, n).toSeq
  }

  test("should fold running frames incrementally with the per-frame results") {
    val rnd = scala.util.Random(7)
    val n   = 3000
    val dec = PType.Decimal(38, 2)
    // Partition 0 has a single peer group; the others have ties and NULL order keys
    val part = Array.tabulate(n)(i => (i % 5).toLong)
    val in   = Schema(
      IndexedSeq(
        Field("p", PType.Int64),
        Field("o", PType.Int64),
        Field("x", PType.Int64),
        Field("d", PType.Float64),
        Field("m", dec),
        Field("dt", PType.Date),
        Field("b", PType.Bool),
        Field("s", PType.Str),
        Field("ms", dec)
      )
    )
    val cols = Array[Vec](
      LongVec(PType.Int64, part, null),
      LongVec(
        PType.Int64,
        Array.tabulate(n)(i =>
          if part(i) == 0 then
            0L
          else
            rnd.nextInt(40).toLong
        ),
        Array.tabulate(n)(i => part(i) == 4 && i % 13 == 0)
      ),
      LongVec(
        PType.Int64,
        Array.fill(n)(rnd.nextInt(1000).toLong),
        Array.fill(n)(rnd.nextInt(7) == 0)
      ),
      // Magnitudes far apart, so the sums depend on the summation order
      DoubleVec(
        Array.fill(n)((rnd.nextDouble() - 0.5) * math.pow(10, rnd.nextInt(20))),
        Array.fill(n)(rnd.nextInt(9) == 0)
      ),
      // Unscaled values near Long.MaxValue / 2, so running sums overflow to BigDecimal
      Vec.fromValues(
        dec,
        (0 until n).map(i =>
          if i % 11 == 0 then
            null
          else
            java.math.BigDecimal.valueOf(4000000000000000000L + rnd.nextInt(1000), 2)
        )
      ),
      IntVec(
        PType.Date,
        Array.fill(n)(19000 + rnd.nextInt(500)),
        Array.fill(n)(rnd.nextInt(6) == 0)
      ),
      BoolVec(Array.fill(n)(rnd.nextInt(10) != 0), Array.fill(n)(rnd.nextInt(8) == 0)),
      StrVec(Array.fill(n)(s"s${rnd.nextInt(100)}"), Array.fill(n)(rnd.nextInt(5) == 0)),
      // Unscaled decimals whose sums fit in a Long
      LongVec(
        dec,
        Array.fill(n)(rnd.nextInt(100000).toLong - 50000),
        Array.fill(n)(rnd.nextInt(10) == 0)
      )
    )
    def col(i: Int)                       = ColRef(i, in.fields(i).ptype, in.fields(i).name)
    val (pr, or, xr, dr, mr, dtr, br, sr) =
      (col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7))
    val msr                             = col(8)
    val aggs: Seq[(String, Seq[VExpr])] = Seq(
      "count_star"     -> Nil,
      "count"          -> Seq(dr),
      "count_if"       -> Seq(br),
      "count_distinct" -> Seq(xr),
      "sum"            -> Seq(xr),
      "sum"            -> Seq(dr),
      "sum"            -> Seq(mr),
      "sum"            -> Seq(msr),
      "sum_if"         -> Seq(dr, br),
      "product"        -> Seq(dr),
      "avg"            -> Seq(dr),
      "avg"            -> Seq(mr),
      "min"            -> Seq(dtr),
      "max"            -> Seq(sr),
      "max"            -> Seq(dr),
      "min"            -> Seq(xr),
      "max"            -> Seq(xr),
      "min"            -> Seq(msr),
      "max"            -> Seq(msr),
      "max"            -> Seq(mr),
      "first"          -> Seq(sr),
      "last"           -> Seq(xr),
      "max_by"         -> Seq(sr, xr),
      "min_by"         -> Seq(xr, dr),
      "bool_and"       -> Seq(br),
      "bool_or"        -> Seq(br),
      "stddev"         -> Seq(dr),
      "var_pop"        -> Seq(dr)
    )
    val ord    = Seq(SortKey(or, ascending = true, nullsFirst = false))
    val frames = Seq(
      None,
      Some(FrameSpec(true, None, Some(0))),
      Some(FrameSpec(true, None, Some(3))),
      Some(FrameSpec(true, None, Some(-2))),
      // Ends beyond either side of a 600-row partition
      Some(FrameSpec(true, None, Some(5000))),
      Some(FrameSpec(true, None, Some(-5000))),
      Some(FrameSpec(false, None, Some(-2))),
      Some(FrameSpec(true, None, None)),
      Some(FrameSpec(false, None, Some(0))),
      Some(FrameSpec(false, None, Some(2))),
      Some(FrameSpec(false, None, None))
    )
    val cs =
      for
        (name, args) <- aggs
        frame        <- frames
        orderBy      <- Seq(ord, Nil)
        if !(orderBy.isEmpty && frame.isEmpty)
      yield WindowCall(
        name,
        args,
        Seq(pr),
        orderBy,
        frame,
        false,
        WindowCall.resultType(name, args.map(_.ptype))
      )
    cs.foreach(c => WindowOp.isRunningFrame(c) shouldBe true)
    // Peers hold the input order after the stable sort, so both paths see the same rows in order
    def eval(running: Boolean, kernels: Boolean = false): Seq[Batch] =
      val op = WindowOp(
        BatchesOp(in, Seq(Batch(cols, n))),
        cs.toIndexedSeq,
        Schema(in.fields ++ cs.indices.map(i => Field(s"w${i}", cs(i).ptype))),
        runningFrames = running,
        runningKernels = kernels
      )
      try op.drain()
      finally op.close()
    val expected = eval(running = false)
    // The accumulator fold, and the primitive fold (running_window_kernels) where it applies
    for actual <- Seq(eval(running = true), eval(running = true, kernels = true)) do
      actual.size shouldBe expected.size
      for
        (a, e) <- actual.zip(expected)
        c      <- a.columns.indices
      do
        sameVec(a.columns(c), e.columns(c))
  }

  /** Evaluates `cs` over one batch, folding frames incrementally or recomputing each frame */
  private def evalWindows(in: Schema, cols: Array[Vec], cs: Seq[WindowCall], incremental: Boolean) =
    val op = WindowOp(
      BatchesOp(in, Seq(Batch(cols, cols(0).size))),
      cs.toIndexedSeq,
      Schema(in.fields ++ cs.indices.map(i => Field(s"w${i}", cs(i).ptype))),
      runningFrames = incremental
    )
    try op.drain()
    finally op.close()

  private def sameBatches(actual: Seq[Batch], expected: Seq[Batch]): Unit =
    actual.size shouldBe expected.size
    for
      (a, e) <- actual.zip(expected)
      c      <- a.columns.indices
    do
      sameVec(a.columns(c), e.columns(c))

  test("should add and remove rows of sliding frames with the per-frame results") {
    val rnd = scala.util.Random(11)
    val n   = 4000
    val dec = PType.Decimal(18, 2)
    // Partition 3 holds decimals near the Long limit, so it recomputes each frame
    val part = Array.tabulate(n)(i => (i % 4).toLong)
    val in   = Schema(
      IndexedSeq(
        Field("p", PType.Int64),
        Field("o", PType.Int64),
        Field("x", PType.Int64),
        Field("i", PType.Int32),
        Field("m", dec),
        Field("b", PType.Bool)
      )
    )
    val cols = Array[Vec](
      LongVec(PType.Int64, part, null),
      LongVec(PType.Int64, Array.fill(n)(rnd.nextInt(50).toLong), null),
      LongVec(PType.Int64, Array.fill(n)(rnd.nextLong() >> 20), Array.fill(n)(rnd.nextInt(5) == 0)),
      IntVec(PType.Int32, Array.fill(n)(rnd.nextInt()), Array.fill(n)(rnd.nextInt(7) == 0)),
      Vec.fromValues(
        dec,
        (0 until n).map(i =>
          if i % 9 == 0 then
            null
          else if part(i) == 3 then
            java.math.BigDecimal.valueOf(4000000000000000000L + rnd.nextInt(1000), 2)
          else
            java.math.BigDecimal.valueOf(rnd.nextInt(100000) - 50000, 2)
        )
      ),
      BoolVec(Array.fill(n)(rnd.nextInt(3) != 0), Array.fill(n)(rnd.nextInt(8) == 0))
    )
    def col(i: Int)                     = ColRef(i, in.fields(i).ptype, in.fields(i).name)
    val (pr, or, xr, ir, mr, br)        = (col(0), col(1), col(2), col(3), col(4), col(5))
    val aggs: Seq[(String, Seq[VExpr])] = Seq(
      "count_star" -> Nil,
      "count"      -> Nil,
      "count"      -> Seq(xr),
      "count_if"   -> Seq(br),
      "countif"    -> Seq(br),
      "sum"        -> Seq(xr),
      "sum"        -> Seq(ir),
      "sum"        -> Seq(mr),
      "sum"        -> Seq(br),
      "sum_if"     -> Seq(ir, br),
      "sum_if"     -> Seq(mr, br)
    )
    val frames = Seq(
      FrameSpec(true, Some(-1), Some(1)),
      FrameSpec(true, Some(-5), Some(5)),
      FrameSpec(true, Some(-200), Some(200)),
      FrameSpec(true, Some(0), Some(0)),
      FrameSpec(true, Some(-3), Some(-1)),
      FrameSpec(true, Some(2), Some(6)),
      // Empty frames: start after end, and bounds beyond a 1000-row partition
      FrameSpec(true, Some(3), Some(1)),
      FrameSpec(true, Some(-5000), Some(-2000)),
      FrameSpec(true, Some(2000), Some(5000)),
      FrameSpec(true, Some(-5000), Some(5000))
    )
    val ord = Seq(SortKey(or, ascending = true, nullsFirst = false))
    val cs  =
      for
        (name, args) <- aggs
        frame        <- frames
      yield WindowCall(
        name,
        args,
        Seq(pr),
        ord,
        Some(frame),
        false,
        WindowCall.resultType(name, args.map(_.ptype))
      )
    cs.foreach(c => WindowOp.isSlidingFrame(c) shouldBe true)
    sameBatches(
      evalWindows(in, cols, cs, incremental = true),
      evalWindows(in, cols, cs, incremental = false)
    )
  }

  test("should find sliding min / max / max_by / min_by rows with the per-frame results") {
    val rnd  = scala.util.Random(23)
    val n    = 4000
    val wide = PType.Decimal(38, 2)
    // NaN, -0.0, and 0.0 compare specially in MinMax's Float64 fold
    val specials      = Array(Double.NaN, -0.0, 0.0, 1.5, -2.0)
    def nulls(k: Int) = Array.fill(n)(rnd.nextInt(k) == 0)
    // BigDecimal values of different scales that compare equal, and values beyond a Long
    val wideValues =
      Array.fill[AnyRef](n)(
        rnd.nextInt(6) match
          case 0 =>
            null
          case 1 =>
            java.math.BigDecimal("1.5")
          case 2 =>
            java.math.BigDecimal("1.50")
          case 3 =>
            java.math.BigDecimal(s"${rnd.nextInt(10)}00000000000000000000000.25")
          case _ =>
            java.math.BigDecimal.valueOf(rnd.nextInt(400) - 200, 2)
      )
    val in = Schema(
      IndexedSeq(
        Field("p", PType.Int64),
        Field("o", PType.Int64),
        Field("x", PType.Int64),
        Field("i", PType.Int32),
        Field("d", PType.Date),
        Field("t", PType.Timestamp),
        Field("f", PType.Float64),
        Field("s", PType.Str),
        Field("m", PType.Decimal(18, 2)),
        Field("w", wide),
        Field("b", PType.Bool)
      )
    )
    val cols = Array[Vec](
      LongVec(PType.Int64, Array.tabulate(n)(i => (i % 4).toLong), null),
      LongVec(PType.Int64, Array.fill(n)(rnd.nextInt(50).toLong), null),
      LongVec(PType.Int64, Array.fill(n)(rnd.nextInt(20).toLong - 10), nulls(5)),
      IntVec(PType.Int32, Array.fill(n)(rnd.nextInt()), nulls(7)),
      IntVec(PType.Date, Array.fill(n)(rnd.nextInt(30)), nulls(6)),
      LongVec(PType.Timestamp, Array.fill(n)(rnd.nextLong() >> 8), nulls(6)),
      DoubleVec(
        Array.fill(n)(
          if rnd.nextInt(3) == 0 then
            specials(rnd.nextInt(specials.length))
          else
            rnd.nextInt(10) - 5.0
        ),
        nulls(5)
      ),
      StrVec(Array.fill(n)(s"s${rnd.nextInt(15)}"), nulls(5)),
      Vec.fromValues(
        PType.Decimal(18, 2),
        (0 until n).map(i =>
          if i % 9 == 0 then
            null
          else
            java.math.BigDecimal.valueOf(rnd.nextInt(2000) - 1000, 2)
        )
      ),
      ObjVec(wide, wideValues, wideValues.map(_ == null)),
      BoolVec(Array.fill(n)(rnd.nextBoolean()), nulls(8))
    )
    def col(i: Int)                     = ColRef(i, in.fields(i).ptype, in.fields(i).name)
    val (pr, or)                        = (col(0), col(1))
    val values                          = (2 until in.fields.size).map(col)
    val aggs: Seq[(String, Seq[VExpr])] =
      values.flatMap(c => Seq("min" -> Seq(c), "max" -> Seq(c))) ++
        Seq(
          // Tied keys: the value tells which row won
          "max_by"  -> Seq(col(6), col(2)),
          "min_by"  -> Seq(col(6), col(2)),
          "arg_max" -> Seq(col(2), col(6)),
          "arg_min" -> Seq(col(7), col(6)),
          "max_by"  -> Seq(col(3), col(7)),
          "min_by"  -> Seq(col(9), col(4)),
          "max_by"  -> Seq(col(2), col(9))
        )
    val frames = Seq(
      FrameSpec(true, Some(-1), Some(1)),
      FrameSpec(true, Some(-5), Some(5)),
      FrameSpec(true, Some(-200), Some(200)),
      FrameSpec(true, Some(0), Some(0)),
      FrameSpec(true, Some(-3), Some(-1)),
      FrameSpec(true, Some(2), Some(6)),
      FrameSpec(true, Some(3), Some(1)),
      FrameSpec(true, Some(-5000), Some(-2000)),
      FrameSpec(true, Some(2000), Some(5000)),
      FrameSpec(true, Some(-5000), Some(5000))
    )
    val ord = Seq(SortKey(or, ascending = true, nullsFirst = false))
    val cs  =
      for
        (name, args) <- aggs
        frame        <- frames
      yield WindowCall(
        name,
        args,
        Seq(pr),
        ord,
        Some(frame),
        false,
        WindowCall.resultType(name, args.map(_.ptype))
      )
    cs.foreach(c => WindowOp.isSlidingFrame(c) shouldBe true)
    sameBatches(
      evalWindows(in, cols, cs, incremental = true),
      evalWindows(in, cols, cs, incremental = false)
    )
  }

  test("should overflow sliding integer sums only where the per-frame fold does") {
    val big                   = 6000000000000000000L
    val in                    = Schema(IndexedSeq(Field("o", PType.Int64), Field("x", PType.Int64)))
    val o                     = ColRef(0, PType.Int64, "o")
    val x                     = ColRef(1, PType.Int64, "x")
    def sum(frame: FrameSpec) = WindowCall(
      "sum",
      Seq(x),
      Nil,
      Seq(SortKey(o, ascending = true, nullsFirst = false)),
      Some(frame),
      false,
      PType.Int64
    )
    val n = 100
    // Alternating signs: the partition's absolute values overflow a Long, but no frame does
    val alternating = Array[Vec](
      LongVec(PType.Int64, Array.tabulate(n)(_.toLong), null),
      LongVec(
        PType.Int64,
        Array.tabulate(n)(i =>
          if i % 2 == 0 then
            big
          else
            -big
        ),
        null
      )
    )
    val pair = Seq(sum(FrameSpec(true, Some(-1), Some(0))), sum(FrameSpec(true, Some(0), Some(1))))
    sameBatches(
      evalWindows(in, alternating, pair, incremental = true),
      evalWindows(in, alternating, pair, incremental = false)
    )
    // Two adjacent positive values overflow in both paths
    val positive = Array[Vec](alternating(0), LongVec(PType.Int64, Array.fill(n)(big), null))
    intercept[ArithmeticException] {
      evalWindows(in, positive, pair, incremental = false)
    }
    intercept[ArithmeticException] {
      evalWindows(in, positive, pair, incremental = true)
    }
    // One row per frame never overflows, in both paths
    val single = Seq(sum(FrameSpec(true, Some(0), Some(0))))
    sameBatches(
      evalWindows(in, positive, single, incremental = true),
      evalWindows(in, positive, single, incremental = false)
    )
  }

  test("should not fold sliding frames or collecting aggregates incrementally") {
    val x   = ColRef(0, PType.Int64, "x")
    val ord = Seq(SortKey(x, ascending = true, nullsFirst = false))
    def win(name: String, frame: Option[FrameSpec]) = WindowCall(
      name,
      Seq(x),
      Nil,
      ord,
      frame,
      false,
      PType.Int64
    )
    WindowOp.isRunningFrame(win("sum", Some(FrameSpec(true, Some(-1), Some(1))))) shouldBe false
    WindowOp.isRunningFrame(win("sum", Some(FrameSpec(false, Some(0), None)))) shouldBe false
    WindowOp.isRunningFrame(win("string_agg", None)) shouldBe false
    WindowOp.isRunningFrame(win("median", None)) shouldBe false
    WindowOp.isRunningFrame(win("sum", None)) shouldBe true
    // Add/remove is exact only for counts and Int64 / Decimal sums over bounded ROWS frames
    val sliding = Some(FrameSpec(true, Some(-1), Some(1)))
    WindowOp.isSlidingFrame(win("sum", sliding)) shouldBe true
    WindowOp.isSlidingFrame(win("sum", sliding).copy(ptype = PType.Float64)) shouldBe false
    WindowOp.isSlidingFrame(win("avg", sliding)) shouldBe false
    WindowOp.isSlidingFrame(win("min", sliding)) shouldBe true
    WindowOp.isSlidingFrame(win("string_agg", sliding)) shouldBe false
    WindowOp.isSlidingFrame(win("sum", Some(FrameSpec(false, Some(-1), Some(1))))) shouldBe false
    WindowOp.isSlidingFrame(win("sum", Some(FrameSpec(true, Some(0), None)))) shouldBe false
  }

end WindowOpTest

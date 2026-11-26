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

class PartitionedAggOpTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(
    IndexedSeq(Field("k", PType.Int64), Field("s", PType.Str), Field("v", PType.Int64))
  )

  /** 10 batches of 1000 rows; k repeats every 997 rows, s is NULL on every 13th row */
  private val batches: Seq[Batch] = (0 until 10).map { b =>
    val n = 1000
    val r = Array.tabulate(n)(i => b * n + i)
    Batch(
      Array(
        LongVec(PType.Int64, r.map(i => (i % 997).toLong), null),
        StrVec(r.map(i => s"s${i % 5}"), r.map(_ % 13 == 0)),
        LongVec(PType.Int64, r.map(i => (i % 31).toLong), null)
      ),
      n
    )
  }

  private val keys = IndexedSeq[VExpr](ColRef(0, PType.Int64, "k"), ColRef(1, PType.Str, "s"))
  private val aggs = IndexedSeq(
    AggCall("count", Nil, false, None, PType.Int64),
    AggCall("sum", Seq(ColRef(2, PType.Int64, "v")), false, None, PType.Int64),
    AggCall("count", Seq(ColRef(2, PType.Int64, "v")), true, None, PType.Int64)
  )

  private val outSchema = Schema(
    IndexedSeq(
      Field("k", PType.Int64),
      Field("s", PType.Str),
      Field("n", PType.Int64),
      Field("total", PType.Int64),
      Field("distinct_v", PType.Int64)
    )
  )

  private def rows(op: Operator): Seq[String] =
    try
      op.drain()
        .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
        .sorted
    finally op.close()

  test("should return the same groups as a sequential aggregation") {
    val expected = rows(HashAggOp(BatchesOp(schema, batches), keys, aggs, outSchema))
    val actual   = rows(
      PartitionedAggOp(
        BatchesOp(schema, batches),
        keys,
        aggs,
        outSchema,
        runner,
        minParallelRows = 0
      )
    )
    actual.size shouldBe expected.size
    actual shouldBe expected
    // NULL string keys form their own groups
    actual.exists(_.contains(",null,")) shouldBe true
  }

  test("should aggregate small inputs in input order on the calling thread") {
    val small    = batches.take(1)
    val expected = HashAggOp(BatchesOp(schema, small), keys, aggs, outSchema).drain()
    val actual   = PartitionedAggOp(BatchesOp(schema, small), keys, aggs, outSchema, runner).drain()
    actual.map(_.size) shouldBe expected.map(_.size)
    actual.head.row(0).toSeq shouldBe expected.head.row(0).toSeq
  }

  test("should give the same output, in the same order, routing input rows in parallel or not") {
    // Unsorted rows: each partition's groups come in first-seen order, which must not change
    def run(parallel: Boolean): Seq[Seq[Any]] =
      val op = PartitionedAggOp(
        BatchesOp(schema, batches),
        keys,
        aggs,
        outSchema,
        runner,
        minParallelRows = 0,
        parallelPartitioning = parallel
      )
      try op.drain().flatMap(b => (0 until b.size).map(i => b.row(i).toSeq))
      finally op.close()
    val sequential = run(false)
    sequential.nonEmpty shouldBe true
    run(true) shouldBe sequential
  }

  test("should give the same groups routing by a packed key or not") {
    // Two Int64 keys, one with NULLs, pack into one; half the input batches are deferred
    val packSchema = Schema(
      IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64), Field("v", PType.Int64))
    )
    val input = (0 until 8).map { j =>
      val n = 1000
      val r = Array.tabulate(n)(i => j * n + i)
      val b = Batch(
        Array(
          LongVec(PType.Int64, r.map(i => (i % 101).toLong), r.map(_ % 17 == 0)),
          LongVec(PType.Int64, r.map(i => (i % 7).toLong), null),
          LongVec(PType.Int64, r.map(i => (i % 31).toLong), null)
        ),
        n
      )
      if j % 2 == 0 then
        b.take(Array.tabulate(n / 2)(_ * 2), n / 2)
      else
        b
    }
    val packKeys = IndexedSeq[VExpr](ColRef(0, PType.Int64, "a"), ColRef(1, PType.Int64, "b"))
    val packOut  = Schema(
      IndexedSeq(
        Field("a", PType.Int64),
        Field("b", PType.Int64),
        Field("n", PType.Int64),
        Field("total", PType.Int64),
        Field("distinct_v", PType.Int64)
      )
    )
    def run(packed: Boolean): Seq[Seq[Any]] =
      // Fresh deferred batches per run: reading a deferred column caches it
      val op = PartitionedAggOp(
        BatchesOp(packSchema, input.map(b => b.take(Array.tabulate(b.size)(identity), b.size))),
        packKeys,
        aggs,
        packOut,
        runner,
        minParallelRows = 0,
        packKeys = true
      ).withPackedRouting(packed)
      try
        val out = op.drain().flatMap(b => (0 until b.size).map(i => b.row(i).toSeq))
        op.usedPackedRouting shouldBe packed
        out
      finally
        op.close()
    // Partitions are chosen by the hash of the packed key, so groups come in another order
    def sorted(out: Seq[Seq[Any]]): Seq[String] =
      out.map(_.map(String.valueOf).mkString(",")).sorted
    val expected = rows(HashAggOp(BatchesOp(packSchema, input), packKeys, aggs, packOut))
    val copied   = run(false)
    copied.exists(_.head == null) shouldBe true
    sorted(copied) shouldBe expected
    sorted(run(true)) shouldBe expected
  }

end PartitionedAggOpTest

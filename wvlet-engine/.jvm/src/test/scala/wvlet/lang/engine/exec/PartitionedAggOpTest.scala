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

  test("should aggregate rows routed to partitions by PartitionRouteOp") {
    val expected = rows(HashAggOp(BatchesOp(schema, batches), keys, aggs, outSchema))
    val parts    = GroupTable.partitionCount(runner.parallelism)
    def routed   = PartitionRouteOp(BatchesOp(schema, batches), keys.size, parts, true)
    // Every routed batch holds the rows of one partition, with their key hashes
    val hasher = GroupTable(keys.map(_.ptype), 0, true)
    val pieces = routed.drain()
    pieces.map(_.size).sum shouldBe batches.map(_.size).sum
    pieces.foreach { b =>
      val p = PartitionRouteOp.partitionOf(b, parts)
      val h = hasher.keyHashes(Array(b.column(0), b.column(1)), b.size)
      b.column(b.numColumns - 1).asInstanceOf[IntVec].values.toSeq shouldBe h.toSeq
      h.forall(x => (x >>> (32 - Integer.numberOfTrailingZeros(parts))) == p) shouldBe true
    }
    for
      minRows <- Seq(0, Int.MaxValue)
      pack    <- Seq(false, true)
    do
      val actual = rows(
        PartitionedAggOp(
          routed,
          keys,
          aggs,
          outSchema,
          runner,
          minParallelRows = minRows,
          packKeys = pack,
          preRouted = true
        )
      )
      actual shouldBe expected
  }

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

end PartitionedAggOpTest

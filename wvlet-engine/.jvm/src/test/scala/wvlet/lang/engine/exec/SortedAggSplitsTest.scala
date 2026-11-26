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

class SortedAggSplitsTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(
    IndexedSeq(Field("s", PType.Str), Field("k", PType.Int64), Field("v", PType.Int32))
  )

  /**
    * 20 batches of 1000 rows whose k is `key(i)` for the i-th row overall, so runs of equal keys
    * cross batch boundaries
    */
  private def batches(key: Int => Long, nullAt: Int = -1): IndexedSeq[Batch] = (0 until 20).map {
    b =>
      val n = 1000
      val r = Array.tabulate(n)(i => b * n + i)
      Batch(
        Array(
          StrVec(r.map(i => s"s${i % 3}"), null),
          LongVec(PType.Int64, r.map(i => key(i)), r.map(_ == nullAt)),
          IntVec(PType.Int32, r.map(_ % 17), null)
        ),
        n
      )
  }

  // Every batch boundary starts a new key
  private val sortedKeys = batches(i => (i / 8).toLong)

  private val keys = IndexedSeq[VExpr](ColRef(0, PType.Str, "s"), ColRef(1, PType.Int64, "k"))
  private val aggs = IndexedSeq(
    AggCall("count", Nil, false, None, PType.Int64),
    AggCall("sum", Seq(ColRef(2, PType.Int32, "v")), false, None, PType.Int64)
  )

  private val outSchema = Schema(
    IndexedSeq(
      Field("s", PType.Str),
      Field("k", PType.Int64),
      Field("n", PType.Int64),
      Field("total", PType.Int64)
    )
  )

  private def aggregate(input: Seq[Batch], splits: Boolean): (Seq[String], Boolean) =
    val op = PartitionedAggOp(BatchesOp(schema, input), keys, aggs, outSchema, runner, 0)
      .withSortedSplits(splits)
    try
      val rows = op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
      (rows, op.usedSortedSplits)
    finally
      op.close()

  test("should split input sorted by a grouping key into runs sharing no key") {
    val input = batches(i => (i / 7).toLong)
    val runs  = SortedAggSplits.of(input, keys, 4).getOrElse(fail("No splits"))
    // Key i / 7 changes at a batch boundary only at rows 7000 and 14000
    runs shouldBe Seq(0 to 6, 7 to 13, 14 until 20)
    runs.flatten shouldBe input.indices
    val keySets = runs.map(r =>
      r.flatMap { i =>
          val k = input(i).column(1).asInstanceOf[LongVec]
          k.values.take(input(i).size)
        }
        .toSet
    )
    keySets
      .combinations(2)
      .foreach { case Seq(a, b) =>
        a.intersect(b) shouldBe Set.empty
      }
  }

  test("should return the same groups as hash routing for sorted input") {
    val (expected, hashed) = aggregate(sortedKeys, splits = false)
    val (actual, split)    = aggregate(sortedKeys, splits = true)
    hashed shouldBe false
    split shouldBe true
    actual.sorted shouldBe expected.sorted
    // The runs come in key order
    actual.map(_.split(",")(1).toLong) shouldBe actual.map(_.split(",")(1).toLong).sorted
  }

  test("should hash-route input whose keys go back") {
    val unsorted        = batches(i => ((i * 7919L) % 2003))
    val (expected, _)   = aggregate(unsorted, splits = false)
    val (actual, split) = aggregate(unsorted, splits = true)
    split shouldBe false
    actual.sorted shouldBe expected.sorted
  }

  test("should hash-route input with a NULL key") {
    val withNull        = batches(i => (i / 7).toLong, nullAt = 12345)
    val (expected, _)   = aggregate(withNull, splits = false)
    val (actual, split) = aggregate(withNull, splits = true)
    split shouldBe false
    actual.sorted shouldBe expected.sorted
  }

  test("should not split when every row has the same key") {
    SortedAggSplits.of(batches(_ => 1L), keys, 8) shouldBe None
  }

  test("should not split on a key with too few values to fill the tasks") {
    // Key i / 7 allows only 3 runs, fewer than half of 8
    SortedAggSplits.of(batches(i => (i / 7).toLong), keys, 8) shouldBe None
  }

end SortedAggSplitsTest

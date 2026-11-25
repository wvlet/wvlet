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

class PartialAggSkipTest extends UniTest:
  test("should stop grouping once enough rows were seen and nearly all of them were groups") {
    val s = PartialAggSkip(minRows = 100, minRatio = 0.9)
    s.shouldStop(50, 50) shouldBe false
    s.shouldStop(100, 80) shouldBe false
    s.skipping shouldBe false
    s.shouldStop(100, 95) shouldBe true
    // The decision is never reverted
    s.shouldStop(1000, 1) shouldBe true
    s.skipping shouldBe true
  }

  test("should stop grouping from the rows and groups reported by finished parts") {
    val s = PartialAggSkip(minRows = 100, minRatio = 0.9)
    s.report(60, 60)
    s.skipping shouldBe false
    s.report(60, 58)
    s.skipping shouldBe true
    val low = PartialAggSkip(minRows = 100, minRatio = 0.9)
    low.report(200, 20)
    low.skipping shouldBe false
  }

  private val dec    = PType.Decimal(7, 2)
  private val sumDec = Aggregates.resultType("sum", Seq(dec))
  private val in     = Schema(
    IndexedSeq(
      Field("k", PType.Int64),
      Field("x", PType.Int64),
      Field("d", dec),
      Field("f", PType.Bool)
    )
  )

  private val k = ColRef(0, PType.Int64, "k")
  private val x = ColRef(1, PType.Int64, "x")
  private val d = ColRef(2, dec, "d")
  private val f = ColRef(3, PType.Bool, "f")
  // Partial aggregates as two-phase aggregation decomposes them, and how the final one merges them
  private val partials = IndexedSeq(
    AggCall("count_star", Nil, false, None, PType.Int64),
    AggCall("count", Seq(x), false, Some(f), PType.Int64),
    AggCall("sum", Seq(x), false, None, PType.Int64),
    AggCall("sum", Seq(d), false, Some(f), sumDec),
    AggCall("min", Seq(x), false, None, PType.Int64),
    AggCall("max", Seq(d), false, None, dec)
  )

  private val partialSchema = Schema(
    Field("k", PType.Int64) +: partials.indices.map(i => Field(s"p${i}", partials(i).ptype))
  )

  private val finals = partials
    .zipWithIndex
    .map { (c, i) =>
      val col = ColRef(i + 1, c.ptype, s"p${i}")
      if c.name.startsWith("count") then
        AggCall("sum", Seq(col), false, None, PType.Int64)
      else
        AggCall(c.name, Seq(col), false, None, c.ptype)
    }

  /** Batches of rows with keys mostly distinct, NULLs in every column, and repeated keys */
  private def input(n: Int, batch: Int): Seq[Batch] =
    val rnd = scala.util.Random(3)
    (0 until n)
      .grouped(batch)
      .map { rows =>
        val m                             = rows.size
        def nulls(p: Int): Array[Boolean] = Array.fill(m)(rnd.nextInt(p) == 0)
        Batch(
          Array(
            LongVec(PType.Int64, rows.map(i => (i % (n - 7)).toLong).toArray, nulls(50)),
            LongVec(PType.Int64, Array.fill(m)(rnd.nextLong() % 1000), nulls(5)),
            LongVec(dec, Array.fill(m)(rnd.nextLong() % 100000), nulls(5)),
            BoolVec(Array.fill(m)(rnd.nextBoolean()), nulls(7))
          ),
          m
        )
      }
      .toSeq

  /** The final aggregate over the partial aggregate's output, as one row per key */
  private def aggregate(batches: Seq[Batch], skip: PartialAggSkip): Map[Any, Seq[Any]] =
    val partial = HashAggOp(
      BatchesOp(in, batches),
      IndexedSeq(k),
      partials,
      partialSchema,
      skip = skip
    )
    val out =
      try partial.drain()
      finally partial.close()
    val fin = HashAggOp(
      BatchesOp(partialSchema, out),
      IndexedSeq(ColRef(0, PType.Int64, "k")),
      finals,
      Schema(Field("k", PType.Int64) +: finals.indices.map(i => Field(s"v${i}", finals(i).ptype)))
    )
    val rows =
      try fin.drain()
      finally fin.close()
    rows
      .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
      .map(r => r.head -> r.tail)
      .toMap

  test(
    "should give the final aggregate the same groups when partial aggregates pass rows through"
  ) {
    val batches  = input(5000, 300)
    val expected = aggregate(batches, null)
    // Stops after the second batch, or after the first
    for minRows <- Seq(600L, 0L) do
      val skip = PartialAggSkip(minRows = minRows, minRatio = 0.5)
      aggregate(batches, skip) shouldBe expected
      skip.skipping shouldBe true
  }

  test("should keep grouping while the groups reduce the rows") {
    val batches = input(5000, 300).map(b =>
      // Ten distinct keys
      Batch(
        LongVec(PType.Int64, Array.tabulate(b.size)(i => (i % 10).toLong), null) +: b.columns.tail,
        b.size
      )
    )
    val skip = PartialAggSkip(minRows = 600, minRatio = 0.5)
    aggregate(batches, skip) shouldBe aggregate(batches, null)
    skip.skipping shouldBe false
  }

end PartialAggSkipTest

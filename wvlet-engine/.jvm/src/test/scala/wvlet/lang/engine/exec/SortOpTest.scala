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

class SortOpTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(
    IndexedSeq(Field("k", PType.Int64), Field("s", PType.Str), Field("id", PType.Int64))
  )

  /** 10 batches of 1000 rows: k has many ties and NULLs, id is the input position */
  private val batches: Seq[Batch] = (0 until 10).map { b =>
    val n = 1000
    val r = Array.tabulate(n)(i => b * n + i)
    Batch(
      Array(
        LongVec(PType.Int64, r.map(i => ((i * 7919) % 37).toLong), r.map(_ % 17 == 0)),
        StrVec(r.map(i => s"s${(i * 31) % 11}"), r.map(_ % 23 == 0)),
        LongVec(PType.Int64, r.map(_.toLong), null)
      ),
      n
    )
  }

  private val keys = Seq(
    SortKey(ColRef(0, PType.Int64, "k"), ascending = false, nullsFirst = false),
    SortKey(ColRef(1, PType.Str, "s"), ascending = true, nullsFirst = true)
  )

  private def rows(op: Operator): Seq[String] =
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
    finally op.close()

  private def sorted(limit: Option[Long], parallel: Boolean): Seq[String] = rows(
    SortOp(
      BatchesOp(schema, batches),
      keys,
      limit,
      if parallel then
        runner
      else
        TaskRunner.Sequential
      ,
      minParallelRows = 0
    )
  )

  test("should sort in parallel in the same order as the sequential stable sort") {
    val expected = sorted(None, parallel = false)
    val actual   = sorted(None, parallel = true)
    actual.size shouldBe 10000
    actual shouldBe expected
  }

  test("should keep the same top rows in parallel") {
    for l <- Seq(0L, 1L, 10L, 2500L, 20000L) do
      sorted(Some(l), parallel = true) shouldBe sorted(Some(l), parallel = false)
  }

  test("should merge sorted runs with ties going to the earlier run") {
    val v    = LongVec(PType.Int64, Array(1L, 2L, 2L, 1L, 2L, 3L), null)
    val cmp  = SortOp.comparator(Array(v), Seq(SortKey(ColRef(0, PType.Int64, "v"), true, false)))
    val runs = Array(
      SortOp.sortRange(cmp, 0, 3, 3),
      Array.empty[Int],
      SortOp.sortRange(cmp, 3, 6, 3)
    )
    SortOp.merge(runs, cmp, 6).toSeq shouldBe Seq(0, 3, 1, 2, 4, 5)
    SortOp.merge(runs, cmp, 3).toSeq shouldBe Seq(0, 3, 1)
  }

  /** 300 rows with many ties; runs of uneven sizes, one empty */
  private val tieVec = LongVec(
    PType.Int64,
    Array.tabulate(300)(i => ((i * 7919) % 13).toLong),
    null
  )

  private val tieCmp = SortOp.comparator(
    Array(tieVec),
    Seq(SortKey(ColRef(0, PType.Int64, "v"), true, false))
  )

  private val bounds  = Seq(0, 40, 40, 170, 171, 300)
  private val tieRuns =
    bounds.sliding(2).map(b => SortOp.sortRange(tieCmp, b(0), b(1), Int.MaxValue)).toArray

  private val tieMerge = SortOp.merge(tieRuns, tieCmp, Int.MaxValue)

  test("should cut sorted runs at every rank of their merge") {
    val runOf = tieRuns.zipWithIndex.flatMap((run, r) => run.map(_ -> r)).toMap
    for k <- 0 to tieMerge.length do
      val expected = Array.fill(tieRuns.length)(0)
      tieMerge.take(k).foreach(i => expected(runOf(i)) += 1)
      SortOp.cutAt(tieRuns, tieCmp, k).toSeq shouldBe expected.toSeq
  }

  test("should merge split output ranges into the full merge") {
    for
      n      <- Seq(tieMerge.length, 100)
      ranges <- Seq(1, 2, 3, 7, 100)
    do
      val cuts  = SortOp.splitRanks(tieRuns, tieCmp, n, ranges)
      val parts = (0 until ranges).flatMap(t =>
        SortOp.mergeRange(tieRuns, tieCmp, cuts(t), cuts(t + 1))
      )
      parts shouldBe tieMerge.take(n).toSeq
  }

  test("should keep stable top rows with a bounded heap") {
    val full = SortOp.sortRange(tieCmp, 0, 300, 300).toSeq
    tieMerge.toSeq shouldBe full
    for l <- Seq(1, 2, 13, 14, 37) do
      SortOp.topRange(tieCmp, 0, 300, l).toSeq shouldBe full.take(l)
      SortOp.topRange(tieCmp, 40, 170, l).toSeq shouldBe
        SortOp.sortRange(tieCmp, 40, 170, 130).take(l).toSeq
  }

end SortOpTest

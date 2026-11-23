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
package wvlet.lang.engine.vector

import wvlet.uni.test.UniTest

class BatchTest extends UniTest:
  private def batchOf(n: Int): Batch = Batch(
    Array(
      IntVec(PType.Int32, Array.tabulate(n)(identity), null),
      StrVec(Array.tabulate(n)(i => s"s${i}"), null)
    ),
    n
  )

  test("slice past the end should stop at the last row") {
    val b = batchOf(5000)
    val s = b.slice(4096, 8192)
    s.size shouldBe 904
    s.column(0).get(0) shouldBe 4096
    s.column(1).get(903) shouldBe "s4999"
  }

  test("slice covering the whole batch should return the batch itself") {
    val b = batchOf(10)
    b.slice(0, 4096) shouldBeTheSameInstanceAs b
  }

  test("take should defer columns until they are read") {
    val b = batchOf(10)
    val t = b.take(Array(7, 2, 5), 3)
    t.isDeferred shouldBe true
    t.column(1).get(0) shouldBe "s7"
    // A read column is cached
    t.column(1) shouldBeTheSameInstanceAs t.column(1)
    t.isDeferred shouldBe true
    t.columns.map(_.get(2)).toSeq shouldBe Seq(5, "s5")
    t.isDeferred shouldBe false
  }

  test("take over deferred columns should compose the positions, keeping -1 as NULL") {
    val b = batchOf(10)
    val t = b.take(Array(9, 8, 7, 6), 4).take(Array(3, -1, 0), 3)
    t.row(0).toSeq shouldBe Seq(6, "s6")
    t.row(1).toSeq shouldBe Seq(null, null)
    t.row(2).toSeq shouldBe Seq(9, "s9")
    // A NULL row of a deferred batch taken again stays NULL
    t.take(Array(1, 2), 2).row(0).toSeq shouldBe Seq(null, null)
  }

  test("takeWhere, select, ++, and slice should keep deferred columns") {
    val b = batchOf(6)
    val t = b.takeWhere(Array(true, false, true, false, true, false))
    t.size shouldBe 3
    val s = t.select(Array(1))
    s.isDeferred shouldBe true
    s.column(0).get(2) shouldBe "s4"
    val joined = t ++ b.take(Array(5, -1, 0), 3)
    joined.numColumns shouldBe 4
    joined.row(1).toSeq shouldBe Seq(2, "s2", null, null)
    val sl = joined.slice(1, 3)
    sl.isDeferred shouldBe true
    sl.row(1).toSeq shouldBe Seq(4, "s4", 0, "s0")
  }

  test("gather and concat should return gathered columns") {
    val b = batchOf(6)
    val t = b.take(Array(1, 3), 2)
    t.gather(Array(1), 1).isDeferred shouldBe false
    val c = Batch.concat(Schema(IndexedSeq(Field("a", PType.Int32), Field("b", PType.Str))), Seq(t))
    c.isDeferred shouldBe false
    c.row(1).toSeq shouldBe Seq(3, "s3")
  }

end BatchTest

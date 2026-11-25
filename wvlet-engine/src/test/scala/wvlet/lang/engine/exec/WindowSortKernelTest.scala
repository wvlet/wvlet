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

class WindowSortKernelTest extends UniTest:
  private val directions =
    for
      asc       <- Seq(true, false)
      nullFirst <- Seq(true, false)
    yield (asc, nullFirst)

  /** The row order IndexSort gives with the key's comparator: the reference */
  private def reference(part: Array[Int], key: Vec, k: SortKey): Seq[Int] =
    val a = part.clone()
    IndexSort.sort(a, SortOp.comparator(Array(key), Seq(k)))
    a.toSeq

  private def check(part: Array[Int], key: Vec, ptype: PType): Unit =
    for (asc, nullsFirst) <- directions do
      val k        = SortKey(ColRef(0, ptype, "k"), asc, nullsFirst)
      val expected = reference(part, key, k)
      val actual   = part.clone()
      WindowOp.sortByPrimitiveKey(actual, key, k) shouldBe true
      actual.toSeq shouldBe expected

  test("should sort a partition by an integer key like the row comparator, keeping ties stable") {
    val rnd = scala.util.Random(7)
    val n   = 3000
    for sizes <- Seq(2, 5, 33, 700, n) do
      // Few distinct values, so ties are common, and some NULLs
      val ints   = Array.fill(n)(rnd.nextInt(40) - 20)
      val longs  = Array.fill(n)(rnd.nextLong() % 1000L)
      val nulls  = Array.fill(n)(rnd.nextInt(10) == 0)
      val part   = rnd.shuffle((0 until n).toSeq).take(sizes).toArray
      val dates  = IntVec(PType.Date, ints, nulls)
      val int64s = LongVec(PType.Int64, longs, nulls)
      check(part, dates, PType.Date)
      check(part, int64s, PType.Int64)
      check(part, IntVec(PType.Int32, ints, null), PType.Int32)
  }

  test("should sort keys at the integer limits and partitions of only NULLs") {
    val ints = Array(Int.MaxValue, Int.MinValue, 0, Int.MinValue, Int.MaxValue)
    check(Array(0, 1, 2, 3, 4), IntVec(PType.Int32, ints, null), PType.Int32)
    val nulls = Array(true, true, true)
    check(Array(2, 0, 1), LongVec(PType.Int64, Array(1L, 2L, 3L), nulls), PType.Int64)
  }

  test("should leave the partition to the row comparator when the key range doesn't fit") {
    val k    = SortKey(ColRef(0, PType.Int64, "k"), true, false)
    val key  = LongVec(PType.Int64, Array(Long.MaxValue, Long.MinValue, 0L), null)
    val part = Array(0, 1, 2)
    WindowOp.sortByPrimitiveKey(part, key, k) shouldBe false
    part.toSeq shouldBe Seq(0, 1, 2)
    val strs = StrVec(Array("b", "a"), null)
    WindowOp.sortByPrimitiveKey(Array(0, 1), strs, k) shouldBe false
  }

end WindowSortKernelTest

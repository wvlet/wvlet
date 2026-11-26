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

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

class LongGroupTableTest extends UniTest:
  private def batch(n: Int, seed: Int, spread: Long, nullEvery: Int): Array[Vec] =
    val rnd   = scala.util.Random(seed)
    val nulls =
      if nullEvery == 0 then
        null
      else
        Array.fill(n)(rnd.nextInt(nullEvery) == 0)
    Array(LongVec(PType.Int64, Array.fill(n)(rnd.nextLong() % spread), nulls))

  /** The ids, the group count, and the group keys each table gives for the batches */
  private def grouped(t: GroupIndex, batches: Seq[Array[Vec]]) =
    val ids  = batches.map(b => t.groupIds(b, b(0).size).toSeq)
    val keys = t.keyVectors()(0)
    (ids, t.numGroups, (0 until t.numGroups).map(keys.get))

  test("should give the groups of a GroupTable across growth, with and without NULLs") {
    for
      (spread, nullEvery) <- Seq((50L, 0), (50L, 7), (1L << 40, 0), (1L << 40, 13))
      presize             <- Seq(0, 100000)
    do
      val batches = (0 until 8).map(i => batch(4096, i, spread, nullEvery))
      grouped(LongGroupTable(PType.Int64, presize), batches) shouldBe
        grouped(GroupTable(IndexedSeq(PType.Int64)), batches)
  }

  test("should keep the key type of its groups") {
    val t = LongGroupTable(PType.Timestamp)
    t.groupIds(Array(LongVec(PType.Timestamp, Array(5L, 3L, 5L), null)), 3).toSeq shouldBe
      Seq(0, 1, 0)
    t.keyVectors()(0).ptype shouldBe PType.Timestamp
  }

  test("should group NULL as one group, and as 0 keys apart") {
    val t   = LongGroupTable(PType.Int64)
    val ids = t.groupIds(
      Array(LongVec(PType.Int64, Array(0L, 0L, 0L, 1L), Array(true, false, true, false))),
      4
    )
    ids.toSeq shouldBe Seq(0, 1, 0, 2)
    val k = t.keyVectors()(0)
    (0 until 3).map(k.get) shouldBe Seq(null, 0L, 1L)
  }

  test("should back packed group tables with the same groups") {
    val types = IndexedSeq(PType.Int32, PType.Int64)
    val rnd   = scala.util.Random(3)
    val bs    = (0 until 4).map { s =>
      Array[Vec](
        IntVec(PType.Int32, Array.fill(2000)(rnd.nextInt(300)), null),
        LongVec(PType.Int64, Array.fill(2000)(rnd.nextInt(200) + s * 1000L), null)
      )
    }
    def run(t: GroupIndex) =
      val ids = bs.map(b => t.groupIds(b, 2000).toSeq)
      val kv  = t.keyVectors()
      (ids, (0 until t.numGroups).map(i => kv.map(_.get(i)).toSeq))
    run(PackedGroupTable(types, 0, longTables = true)) shouldBe run(GroupTable(types))
  }

end LongGroupTableTest

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

class GroupTableTest extends UniTest:
  private def longs(xs: Long*): Vec  = LongVec(PType.Int64, xs.toArray, null)
  private def strs(xs: String*): Vec = StrVec(
    xs.toArray,
    if xs.contains(null) then
      xs.map(_ == null).toArray
    else
      null
  )

  test("should assign dense ids to composite keys") {
    val t   = GroupTable(IndexedSeq(PType.Int64, PType.Str))
    val ids = t.groupIds(Array(longs(1, 2, 1, 1), strs("a", "a", "a", "b")), 4)
    ids.toSeq shouldBe Seq(0, 1, 0, 2)
    t.numGroups shouldBe 3
    val keys = t.keyVectors()
    (0 until 3).map(i => (keys(0).get(i), keys(1).get(i))) shouldBe
      Seq((1L, "a"), (2L, "a"), (1L, "b"))
  }

  test("should group NULL keys together but never match them in lookups") {
    val t = GroupTable(IndexedSeq(PType.Str))
    t.groupIds(Array(strs("x", null, null, "x")), 4).toSeq shouldBe Seq(0, 1, 1, 0)
    t.keyVectors()(0).isNull(1) shouldBe true
    t.lookup(Array(strs(null, "x", "y")), 3).toSeq shouldBe Seq(-1, 0, -1)
  }

  test("should keep ids stable while the table grows") {
    val t   = GroupTable(IndexedSeq(PType.Int64))
    val n   = 50000
    val v   = LongVec(PType.Int64, Array.tabulate(n)(i => (i % 20000).toLong * 7919L), null)
    val ids = t.groupIds(Array(v), n)
    t.numGroups shouldBe 20000
    (0 until n).forall(i => ids(i) == ids(i % 20000)) shouldBe true
    t.lookup(Array(longs(7919L * 3, 5L)), 2).toSeq shouldBe Seq(3, -1)
  }

  test("should match keys given in another vector representation") {
    val t = GroupTable(IndexedSeq(PType.Int64))
    t.groupIds(Array(longs(10, 20)), 2)
    // int32 values for an int64 key are converted, not compared by representation
    t.lookup(Array(IntVec(PType.Int32, Array(20, 30), null)), 2).toSeq shouldBe Seq(1, -1)
  }

  test("should compare doubles by value bits") {
    val t = GroupTable(IndexedSeq(PType.Float64))
    val v = DoubleVec(Array(Double.NaN, Double.NaN, 0.0, -0.0, 1.5), null)
    t.groupIds(Array(v), 5).toSeq shouldBe Seq(0, 0, 1, 2, 3)
  }

  test("should put NULL keys in one group whatever their slots hold") {
    // Kernels compute values for NULL rows too, e.g. NULL + 5 and NULL + 7
    val nulls = Array(true, true, false)
    for v <- Seq[Vec](
        LongVec(PType.Int64, Array(5L, 7L, 1L), nulls),
        IntVec(PType.Int32, Array(5, 7, 1), nulls),
        DoubleVec(Array(5.0, 7.0, 1.0), nulls),
        StrVec(Array("a", "b", "c"), nulls),
        BoolVec(Array(true, false, true), nulls)
      )
    do
      GroupTable(IndexedSeq(v.ptype)).groupIds(Array(v), 3).toSeq shouldBe Seq(0, 0, 1)
  }

  test("should partition rows by key hash without reusing a table's slot bits") {
    val n    = 100000
    val keys = LongVec(PType.Int64, Array.tabulate(n)(i => (i % 40000).toLong), null)
    val h    = GroupTable(IndexedSeq(PType.Int64)).keyHashes(Array(keys), n)
    val rows = GroupTable.partitionRows(h, n, 16)
    // Every row once, in input order within a partition
    rows.flatten.sorted.toSeq shouldBe (0 until n)
    rows.forall(r => r.toSeq == r.sorted.toSeq) shouldBe true
    // Rows with the same key share a partition
    val partOf = new Array[Int](n)
    rows.indices.foreach(p => rows(p).foreach(i => partOf(i) = p))
    (0 until n).forall(i => partOf(i) == partOf(i % 40000)) shouldBe true
    // Bits 16 and up, which large partition tables use as slot bits, still vary inside a partition
    rows.foreach(r => r.map(i => h(i) & 0xffff0000).distinct.size > 1 shouldBe true)
  }

end GroupTableTest

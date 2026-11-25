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

import java.math.BigDecimal as JBigDecimal

class PackedGroupTableTest extends UniTest:
  private val dec   = PType.Decimal(7, 2)
  private val types = IndexedSeq(PType.Int32, PType.Date, PType.Int64, dec)

  /** n rows whose Int64 and decimal columns span about `spread` values from `base` */
  private def batch(n: Int, seed: Int, base: Long, spread: Int): Array[Vec] =
    val rnd                           = scala.util.Random(seed)
    def nulls(p: Int): Array[Boolean] = Array.fill(n)(rnd.nextInt(p) == 0)
    Array(
      IntVec(PType.Int32, Array.fill(n)(rnd.nextInt(5) - 2), nulls(6)),
      IntVec(PType.Date, Array.fill(n)(10000 + rnd.nextInt(30)), null),
      LongVec(PType.Int64, Array.fill(n)(base + rnd.nextInt(spread)), nulls(11)),
      LongVec(dec, Array.fill(n)(rnd.nextInt(spread) - 50L), nulls(7))
    )

  private def rows(cols: Array[Vec], n: Int): Seq[Seq[Any]] = (0 until n).map(i =>
    cols.map(_.get(i)).toSeq
  )

  /** The ids, the group count, and the group keys each table gives for the batches */
  private def grouped(t: GroupIndex, batches: Seq[(Array[Vec], Int)]) =
    val ids = batches.map((b, n) => t.groupIds(b, n).toSeq)
    (ids, t.numGroups, rows(t.keyVectors(), t.numGroups))

  test("should give the groups of an unpacked table while repacking into wider ranges") {
    val batches = Seq(
      (batch(500, 1, 0L, 40), 500),
      (batch(500, 2, 0L, 40), 500),
      // Beyond the first ranges, at both ends
      (batch(500, 3, 1000L, 400), 500),
      (batch(500, 4, -5000L, 400), 500),
      (batch(500, 5, 0L, 40), 500)
    )
    val packed = PackedGroupTable(types, 0, longDecimals = true)
    val result = grouped(packed, batches)
    result shouldBe grouped(GroupTable(types), batches)
    packed.isPacked shouldBe true
    packed.repacks > 0 shouldBe true
    packed.keyVectors().map(_.ptype).toSeq shouldBe types
  }

  test("should move the groups to an unpacked table once the codes need over 63 bits") {
    val t2                       = IndexedSeq(PType.Int64, PType.Int64)
    def b(xs: Long*): Array[Vec] = Array(
      LongVec(PType.Int64, xs.toArray, null),
      LongVec(PType.Int64, xs.reverse.toArray, null)
    )
    val batches = Seq((b(1L, 2L, 1L), 3), (b(1L << 40, 3L, 2L), 3), (b(1L, Long.MinValue, 3L), 3))
    val packed  = PackedGroupTable(t2)
    grouped(packed, batches) shouldBe grouped(GroupTable(t2), batches)
    packed.isPacked shouldBe false
  }

  test("should fall back when a key isn't in the representation of its type") {
    val t2 = IndexedSeq(PType.Int32, dec)
    // A decimal column of another scale is rescaled by the unpacked table
    val other = Array[Vec](
      IntVec(PType.Int32, Array(1, 1, 2), null),
      LongVec(PType.Decimal(7, 1), Array(15L, 150L, 15L), null)
    )
    val first  = Array[Vec](IntVec(PType.Int32, Array(1), null), LongVec(dec, Array(150L), null))
    val packed = PackedGroupTable(t2, 0, longDecimals = true)
    packed.groupIds(first, 1).toSeq shouldBe Seq(0)
    packed.groupIds(other, 3).toSeq shouldBe Seq(0, 1, 2)
    packed.isPacked shouldBe false
    rows(packed.keyVectors(), 3) shouldBe
      Seq(Seq(1, JBigDecimal("1.50")), Seq(1, JBigDecimal("15.00")), Seq(2, JBigDecimal("1.50")))
  }

  test("should group decimals by unscaled values, as boxed BigDecimals at the type's scale") {
    val d     = PType.Decimal(20, 2)
    val wide  = JBigDecimal("123456789012345678.90")
    val longs = LongVec(d, Array(150L, -3L, 150L, 0L), Array(false, false, false, true))
    val boxed = ObjVec(
      d,
      Array[AnyRef](JBigDecimal("1.5"), wide, JBigDecimal("-0.030"), null, wide),
      Array(false, false, false, true, false)
    )
    val table = GroupTable(IndexedSeq(d), longDecimals = true)
    val ids   = table.groupIds(Array(longs), 4).toSeq ++ table.groupIds(Array(boxed), 5).toSeq
    ids shouldBe Seq(0, 1, 0, 2, 0, 3, 1, 2, 3)
    rows(table.keyVectors(), table.numGroups).map(_.head) shouldBe
      Seq(JBigDecimal("1.50"), JBigDecimal("-0.03"), null, wide)
    // Without wide values, the keys stay Long-backed
    val narrow = GroupTable(IndexedSeq(dec), longDecimals = true)
    narrow.groupIds(Array(LongVec(dec, Array(1L, 2L, 1L), null)), 3).toSeq shouldBe Seq(0, 1, 0)
    narrow.keyVectors()(0).isInstanceOf[LongVec] shouldBe true
  }

  test("should key decimals alike in hashes and lookups") {
    val t     = IndexedSeq(dec, PType.Int32)
    val ks    = batch(300, 9, 0L, 60)
    val cols  = Array[Vec](ks(3), ks(0))
    val table = GroupTable(t, longDecimals = true)
    val ids   = table.groupIds(cols, 300)
    ids.toSeq shouldBe GroupTable(t).groupIds(cols, 300).toSeq
    table.lookup(cols, 300, nullsMatch = true).toSeq shouldBe ids.toSeq
    val again = GroupTable(t, longDecimals = true)
    again.groupIds(cols, 300, table.keyHashes(cols, 300)).toSeq shouldBe ids.toSeq
  }

  test("should choose a packed table only for packable keys") {
    PackedGroupTable.forKeys(types, 0, pack = true, longDecimals = true) match
      case _: PackedGroupTable =>
      case other               =>
        fail(s"Expected a packed table: ${other}")
    PackedGroupTable.forKeys(types, 0, pack = true, longDecimals = false) match
      case _: GroupTable =>
      case other         =>
        fail(s"Expected a GroupTable: ${other}")
    PackedGroupTable.forKeys(types.take(3), 0, pack = false, longDecimals = true) match
      case _: GroupTable =>
      case other         =>
        fail(s"Expected a GroupTable: ${other}")
    // No rows: empty key columns of the key types
    val empty = PackedGroupTable(types, 0, longDecimals = true)
    empty.numGroups shouldBe 0
    empty.keyVectors().map(_.size).toSeq shouldBe Seq(0, 0, 0, 0)
  }

end PackedGroupTableTest

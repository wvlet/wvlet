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

class AggKeyPackingTest extends UniTest:
  private val types = IndexedSeq(PType.Int32, PType.Date, PType.Int64)

  private def keys(n: Int, seed: Int): Array[Vec] =
    val rnd                           = scala.util.Random(seed)
    def nulls(p: Int): Array[Boolean] = Array.fill(n)(rnd.nextInt(p) == 0)
    Array(
      IntVec(PType.Int32, Array.fill(n)(rnd.nextInt(7) - 3), nulls(5)),
      IntVec(PType.Date, Array.fill(n)(10000 + rnd.nextInt(400)), null),
      LongVec(PType.Int64, Array.fill(n)(rnd.nextLong() % 100000L), nulls(9))
    )

  private def rows(cols: Array[Vec], n: Int): Seq[Seq[Any]] = (0 until n).map(i =>
    cols.map(_.get(i)).toSeq
  )

  test("should pack keys so that equal packed values are equal keys, NULLs included") {
    val n      = 3000
    val ks     = keys(n, 5)
    val p      = AggKeyPacking.of(types, Seq((ks, n))).getOrElse(fail("Expected a packing"))
    val packed = p.pack(ks, n)
    packed.nulls shouldBe null
    val original = rows(ks, n)
    // Unpacking restores every key, with its NULLs and types
    rows(p.unpack(packed, n), n) shouldBe original
    p.unpack(packed, n).map(_.ptype).toSeq shouldBe types
    val byPacked = (0 until n).groupBy(i => packed.values(i)).values.map(_.map(original(_)).toSet)
    byPacked.foreach(_.size shouldBe 1)
    byPacked.size shouldBe original.distinct.size
  }

  test("should unpack branch-free as with a NULL check per value") {
    val n = 3000
    for seed <- Seq(5, 6) do
      val ks     = keys(n, seed)
      val p      = AggKeyPacking.of(types, Seq((ks, n))).getOrElse(fail("Expected a packing"))
      val packed = p.pack(ks, n)
      val lean   = p.unpack(packed, n, branchFree = true)
      val plain  = p.unpack(packed, n)
      rows(lean, n) shouldBe rows(plain, n)
      lean.map(_.ptype).toSeq shouldBe types
      // NULL flags only for the columns with NULLs, and NULL values zeroed as before
      lean.map(_.nulls == null).toSeq shouldBe plain.map(_.nulls == null).toSeq
      lean(0).asInstanceOf[IntVec].values.toSeq shouldBe plain(0).asInstanceOf[IntVec].values.toSeq
      lean(2).asInstanceOf[LongVec].values.toSeq shouldBe
        plain(2).asInstanceOf[LongVec].values.toSeq
  }

  test("should take the ranges of every batch and pack columns of only NULLs") {
    val a = keys(100, 1)
    val b = keys(100, 2)
    val p = AggKeyPacking.of(types, Seq((a, 100), (b, 100))).getOrElse(fail("Expected a packing"))
    rows(p.unpack(p.pack(b, 100), 100), 100) shouldBe rows(b, 100)
    val allNull = Array[Vec](
      IntVec(PType.Int32, new Array[Int](3), Array(true, true, true)),
      LongVec(PType.Int64, Array(Long.MinValue, 0L, Long.MaxValue), null)
    )
    AggKeyPacking.of(IndexedSeq(PType.Int32, PType.Int64), Seq((allNull, 3))) shouldBe None
    val small = Array[Vec](allNull(0), LongVec(PType.Int64, Array(-5L, 0L, 7L), null))
    val q     = AggKeyPacking
      .of(IndexedSeq(PType.Int32, PType.Int64), Seq((small, 3)))
      .getOrElse(fail("Expected a packing"))
    rows(q.unpack(q.pack(small, 3), 3), 3) shouldBe rows(small, 3)
  }

  test("should not pack a single column, other types, or keys wider than 63 bits") {
    val n   = 4
    val big = LongVec(PType.Int64, Array(0L, 1L << 40, 3L, 4L), null)
    AggKeyPacking.of(IndexedSeq(PType.Int64), Seq((Array[Vec](big), n))) shouldBe None
    AggKeyPacking.of(IndexedSeq(PType.Int64, PType.Int64), Seq((Array[Vec](big, big), n))) shouldBe
      None
    val strs = StrVec(Array("a", "b", "c", "d"), null)
    AggKeyPacking.of(IndexedSeq(PType.Int64, PType.Str), Seq((Array[Vec](big, strs), n))) shouldBe
      None
  }

  test(
    "should give the groups of an unpacked aggregation in a partitioned one with pack_agg_keys"
  ) {
    val n   = 5000
    val ks  = keys(n, 9)
    val rnd = scala.util.Random(4)
    val v   = LongVec(PType.Int64, Array.fill(n)(rnd.nextLong() % 1000), null)
    val in  = Schema(
      IndexedSeq(
        Field("a", PType.Int32),
        Field("d", PType.Date),
        Field("b", PType.Int64),
        Field("v", PType.Int64)
      )
    )
    val cols    = ks :+ v
    val batches = (0 until n)
      .grouped(700)
      .map(r => Batch(cols.map(_.gather(r.toArray, r.size)), r.size))
      .toSeq
    val keyRefs = IndexedSeq[VExpr](
      ColRef(0, PType.Int32, "a"),
      ColRef(1, PType.Date, "d"),
      ColRef(2, PType.Int64, "b")
    )
    val aggs = IndexedSeq(
      AggCall("count_star", Nil, false, None, PType.Int64),
      AggCall("sum", Seq(ColRef(3, PType.Int64, "v")), false, None, PType.Int64)
    )
    val out = Schema(in.fields.take(3) ++ Seq(Field("n", PType.Int64), Field("s", PType.Int64)))
    def run(pack: Boolean): Seq[Seq[Any]] =
      val op = PartitionedAggOp(
        BatchesOp(in, batches),
        keyRefs,
        aggs,
        out,
        TaskRunner.Sequential,
        packKeys = pack
      )
      try op
          .drain()
          .flatMap { b =>
            b.columns.take(3).map(_.ptype).toSeq shouldBe types
            rows(b.columns, b.size)
          }
      finally op.close()
    // The same groups, in the same first-seen order
    run(pack = true) shouldBe run(pack = false)
  }

end AggKeyPackingTest

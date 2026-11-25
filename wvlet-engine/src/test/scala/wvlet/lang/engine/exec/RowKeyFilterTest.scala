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

class RowKeyFilterTest extends UniTest:
  private def filterOf(keys: Array[Long]): RowKeyFilter =
    val v = LongVec(PType.Int64, keys, null)
    RowKeyFilter.of(v, keys.indices.toArray, keys.length, keys.min, keys.max).get

  private def selected(f: RowKeyFilter, probe: Vec, sel: Array[Int] = null): Seq[Int] =
    val n =
      if sel == null then
        probe.size
      else
        sel.length
    val out = new Array[Int](n)
    val k   = f.select(probe, sel, n, out)
    out.take(k).toSeq

  test("should keep exactly the build keys with a bitmap over a dense range") {
    val f = filterOf(Array(10L, 12L, 12L, 20L))
    f.kind shouldBe RowKeyFilter.Kind.Bitmap
    (0L to 30L).filter(f.mayContain) shouldBe Seq(10L, 12L, 20L)
  }

  test("should keep every build key with a Bloom filter over a sparse range") {
    val keys = Array.tabulate(1000)(i => i.toLong * 1000003L - 7L)
    val f    = filterOf(keys)
    f.kind shouldBe RowKeyFilter.Kind.Bloom
    keys.forall(f.mayContain) shouldBe true
    // Values within the range but not build keys mostly fail
    val others = (0 until 10000).map(i => i.toLong * 99991L + 5L).filterNot(keys.contains)
    (others.count(f.mayContain).toDouble / others.size < 0.05) shouldBe true
    f.mayContain(keys.max + 1) shouldBe false
  }

  test("should test only the range for builds above the key limit") {
    val n = RowKeyFilter.MaxKeys + 1
    val v = LongVec(PType.Int64, Array(5L, 9L), null)
    // Rows beyond the vector are never read for a range filter
    val f = RowKeyFilter.of(v, Array(0, 1), n, 5L, 9L).get
    f.kind shouldBe RowKeyFilter.Kind.Range
    (0L to 12L).filter(f.mayContain) shouldBe (5L to 9L)
  }

  test("should drop NULL probe keys and read int keys") {
    val f     = filterOf(Array(1L, 3L))
    val probe = IntVec(PType.Int32, Array(1, 2, 3, 3, 1), Array(false, false, false, true, false))
    selected(f, probe) shouldBe Seq(0, 2, 4)
  }

  test("should narrow a selection in place") {
    val f     = filterOf(Array(1L, 3L))
    val probe = LongVec(PType.Int64, Array(1L, 2L, 3L, 4L, 1L), null)
    val sel   = Array(1, 2, 4)
    val k     = f.select(probe, sel, sel.length, sel)
    sel.take(k).toSeq shouldBe Seq(2, 4)
  }

  test("should pass every row of keys that are not integers") {
    val f = filterOf(Array(1L))
    selected(f, StrVec(Array("a", "b"), null)) shouldBe Seq(0, 1)
  }

  test("should pause after a sample that keeps most rows, then sample again") {
    val f = filterOf(Array(1L, 2L))
    f.active() shouldBe true
    f.record(RowKeyFilter.SampleRows.toInt, RowKeyFilter.SampleRows.toInt)
    val skipped = Iterator.continually(f.active()).takeWhile(!_).size
    skipped shouldBe RowKeyFilter.MinPause.toInt
    // A selective sample keeps it on
    f.record(RowKeyFilter.SampleRows.toInt, 0)
    f.active() shouldBe true
    f.droppedRows shouldBe RowKeyFilter.SampleRows
  }

  test("should set the per-row filter from the build only with rows") {
    val keys = LongVec(PType.Int64, Array(4L, 8L), null)
    val off  = RuntimeFilter(0, "k", (_, _) => None)
    off.set(keys, Array(0, 1), 2)
    off.rowFilter shouldBe null
    val on = RuntimeFilter(0, "k", (_, _) => None, rows = true)
    on.set(keys, Array(0, 1), 2)
    on.rowFilter.mayContain(8L) shouldBe true
    on.rowFilter.mayContain(6L) shouldBe false
  }

end RowKeyFilterTest

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

class KeyPackerTest extends UniTest:
  private def nullsOf(xs: Seq[Any]): Array[Boolean] =
    if xs.contains(null) then
      xs.map(_ == null).toArray
    else
      null

  private def ints(xs: Any*): Vec = IntVec(
    PType.Int32,
    xs.map {
        case null =>
          0
        case i: Int =>
          i
      }
      .toArray,
    nullsOf(xs)
  )

  private def longs(xs: Any*): Vec = LongVec(
    PType.Int64,
    xs.map {
        case null =>
          0L
        case l: Long =>
          l
      }
      .toArray,
    nullsOf(xs)
  )

  private def all(n: Int): Array[Int] = Array.tabulate(n)(identity)

  test("should pack equal keys alike and distinct keys apart") {
    val keys   = Array(ints(1, 2, 1, 3, -5), longs(10L, 10L, 10L, 7L, 1000000000000L))
    val packer = KeyPacker.of(IndexedSeq(PType.Int32, PType.Int64), keys, all(5), 5)
    packer shouldNotBe null
    val p = packer.pack(keys, 5)
    p.nulls shouldBe null
    p.values(0) shouldBe p.values(2)
    p.values.distinct.length shouldBe 4
  }

  test("should pack NULL keys and values outside the build ranges as NULL") {
    val build  = Array(ints(1, 2, 3), longs(10L, 20L, 30L))
    val packer = KeyPacker.of(IndexedSeq(PType.Int32, PType.Int64), build, all(3), 3)
    val probe  = Array(ints(2, 0, 4, null, 3), longs(20L, 20L, 20L, 20L, 31L))
    val p      = packer.pack(probe, 5)
    p.nulls.toSeq shouldBe Seq(false, true, true, true, true)
    p.values(0) shouldBe packer.pack(build, 3).values(1)
  }

  test("should not pack single, non-integer, or too wide keys") {
    KeyPacker.of(IndexedSeq(PType.Int64), Array(longs(1L, 2L)), all(2), 2) shouldBe null
    KeyPacker.of(
      IndexedSeq(PType.Int64, PType.Str),
      Array(longs(1L), StrVec(Array("a"), null)),
      all(1),
      1
    ) shouldBe null
    // Two columns of 40 bits each
    val wide = Array(longs(0L, 1L << 39), longs(0L, 1L << 39))
    KeyPacker.of(IndexedSeq(PType.Int64, PType.Int64), wide, all(2), 2) shouldBe null
    // A range overflowing a Long
    val huge = Array(longs(Long.MinValue, Long.MaxValue), longs(0L, 1L))
    KeyPacker.of(IndexedSeq(PType.Int64, PType.Int64), huge, all(2), 2) shouldBe null
    // No rows
    KeyPacker.of(IndexedSeq(PType.Int32, PType.Int32), Array(ints(), ints()), all(0), 0) shouldBe
      null
  }

  /** Runs tasks on the calling thread but reports parallelism, so builds partition their rows */
  private object PartitioningRunner extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private def firstMatches(
      build: Array[Vec],
      probe: Array[Vec],
      n: Int,
      packKeys: Boolean,
      runner: TaskRunner
  ): Seq[Int] =
    val schema = Schema(build.indices.map(i => Field(s"k${i}", build(i).ptype)))
    val keys   = build.indices.map(i => ColRef(i, build(i).ptype, s"k${i}"))
    val jb     = JoinBuild(
      BatchesOp(schema, Seq(Batch(build, build(0).size))),
      keys,
      runner = runner,
      packKeys = packKeys
    )
    // The packed keys of these builds are dense: test the hash tables, not the direct index
    jb.directIndex = false
    jb.ensureBuilt()
    jb.firstMatches(probe, n).toSeq

  test("should find the same matches with packed join keys") {
    val build = Array(ints(1, 2, 1, null, 3, 2), longs(5L, 5L, 5L, 5L, null, 6L))
    val probe = Array(ints(1, 2, 3, null, 9, 2, -1), longs(5L, 6L, null, 5L, 5L, 7L, 5L))
    val plain = firstMatches(build, probe, 7, packKeys = false, TaskRunner.Sequential)
    plain shouldBe Seq(0, 5, -1, -1, -1, -1, -1)
    firstMatches(build, probe, 7, packKeys = true, TaskRunner.Sequential) shouldBe plain
  }

  test("should find the same matches with packed keys over partitioned builds") {
    val n     = 70000
    val build = Array(
      ints((0 until n).map(i => i % 1000)*),
      longs((0 until n).map(i => (i / 1000).toLong * 3)*)
    )
    val m     = 5000
    val probe = Array(
      ints((0 until m).map(i => (i * 7) % 1200)*),
      longs((0 until m).map(i => (i % 80).toLong * 3 + (i % 2))*)
    )
    val plain = firstMatches(build, probe, m, packKeys = false, PartitioningRunner)
    plain.count(_ >= 0) shouldNotBe 0
    firstMatches(build, probe, m, packKeys = true, PartitioningRunner) shouldBe plain
  }

end KeyPackerTest

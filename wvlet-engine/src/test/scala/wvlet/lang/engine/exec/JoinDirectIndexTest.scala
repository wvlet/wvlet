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

class JoinDirectIndexTest extends UniTest:
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

  private def joinBuild(build: Array[Vec], direct: Boolean, runner: TaskRunner): JoinBuild =
    val schema = Schema(build.indices.map(i => Field(s"k${i}", build(i).ptype)))
    val keys   = build.indices.map(i => ColRef(i, build(i).ptype, s"k${i}"))
    val jb = JoinBuild(BatchesOp(schema, Seq(Batch(build, build(0).size))), keys, runner = runner)
    jb.directIndex = direct
    jb.ensureBuilt()
    jb

  /** Every match of each probe row, in chain order */
  private def allMatches(jb: JoinBuild, probe: Array[Vec], n: Int): Seq[Seq[Int]] = jb
    .firstMatches(probe, n)
    .toSeq
    .map { first =>
      Iterator.iterate(first)(jb.nextMatch).takeWhile(_ >= 0).toSeq
    }

  /** Matches through the direct index, checked against the hash tables */
  private def directMatches(
      build: Array[Vec],
      probe: Array[Vec],
      runner: TaskRunner = TaskRunner.Sequential
  ): (JoinBuild, Seq[Seq[Int]]) =
    val n      = probe(0).size
    val direct = joinBuild(build, direct = true, runner)
    direct.usesDirectIndex shouldBe true
    val hashed = joinBuild(build, direct = false, runner)
    hashed.usesDirectIndex shouldBe false
    val m = allMatches(direct, probe, n)
    m shouldBe allMatches(hashed, probe, n)
    direct.uniqueKeys shouldBe hashed.uniqueKeys
    (direct, m)

  test("should find Int32 key matches in input order through a direct index") {
    val build   = Array(ints(5, 7, 5, null, -2, 7, 5))
    val probe   = Array(ints(5, 7, -2, null, 6, 100, -3, Int.MinValue, Int.MaxValue))
    val (jb, m) = directMatches(build, probe)
    m shouldBe Seq(Seq(0, 2, 6), Seq(1, 5), Seq(4), Nil, Nil, Nil, Nil, Nil, Nil)
    jb.uniqueKeys shouldBe false
  }

  test("should not match Int64 probe keys outside the build range") {
    val build   = Array(longs(-3L, 0L, 4L, 2L))
    val probe   = Array(longs(4L, -3L, 2L, 1L, Long.MinValue, Long.MaxValue, null, -4L, 5L))
    val (jb, m) = directMatches(build, probe)
    m shouldBe Seq(Seq(2), Seq(0), Seq(3), Nil, Nil, Nil, Nil, Nil, Nil)
    jb.uniqueKeys shouldBe true
  }

  test("should match build ranges near the ends of Int64") {
    val top    = Array(longs(Long.MaxValue, Long.MaxValue - 2L))
    val (_, m) = directMatches(top, Array(longs(Long.MaxValue, Long.MinValue, Long.MaxValue - 1L)))
    m shouldBe Seq(Seq(0), Nil, Nil)
    val bottom  = Array(longs(Long.MinValue, Long.MinValue + 1L))
    val (_, m2) = directMatches(bottom, Array(longs(Long.MaxValue, Long.MinValue + 1L, 0L)))
    m2 shouldBe Seq(Nil, Seq(1), Nil)
  }

  test("should index a few keys over a range that fits in cache") {
    val build   = Array(ints(200000, 17, 150000, 17))
    val probe   = Array(ints(17, 150000, 200000, 18, 200001, 16))
    val (jb, m) = directMatches(build, probe)
    m shouldBe Seq(Seq(1, 3), Seq(2), Seq(0), Nil, Nil, Nil)
    jb.uniqueKeys shouldBe false
  }

  test("should use hash tables for sparse keys") {
    val sparse = Array(ints(0, 1000000, 2000000))
    joinBuild(sparse, direct = true, TaskRunner.Sequential).usesDirectIndex shouldBe false
    // A range overflowing a Long
    val wide = Array(longs(Long.MinValue, Long.MaxValue))
    joinBuild(wide, direct = true, TaskRunner.Sequential).usesDirectIndex shouldBe false
  }

  /** Runs tasks on the calling thread but reports parallelism, so hashed builds partition */
  private object PartitioningRunner extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  test("should match partitioned hash tables on large builds") {
    val n       = 70000
    val build   = Array(ints((0 until n).map(i => (i * 13) % 50000 - 100)*))
    val probe   = Array(ints((0 until 5000).map(i => i * 11 - 500)*))
    val (jb, m) = directMatches(build, probe, PartitioningRunner)
    m.count(_.nonEmpty) shouldNotBe 0
    jb.uniqueKeys shouldBe false
  }

  /** A build over batches of `rows` rows each, built serially or in parallel */
  private def batchedBuild(cols: Seq[Vec], rows: Int, parallel: Boolean): JoinBuild =
    val n       = cols.head.size
    val schema  = Schema(cols.indices.map(i => Field(s"c${i}", cols(i).ptype)))
    val batches = (0 until n by rows).map { from =>
      val idx = (from until math.min(n, from + rows)).toArray
      Batch(cols.map(_.gather(idx, idx.length)).toArray, idx.length)
    }
    val jb = JoinBuild(
      BatchesOp(schema, batches),
      IndexedSeq(ColRef(0, cols.head.ptype, "c0")),
      runner = PartitioningRunner
    )
    jb.parallelBuild = parallel
    jb.ensureBuilt()
    jb

  /** The parallel build matches the serial one: rows, matches in chain order, and key uniqueness */
  private def sameAsSerial(cols: Seq[Vec], probe: Vec, rows: Int = 9973): JoinBuild =
    val serial   = batchedBuild(cols, rows, parallel = false)
    val parallel = batchedBuild(cols, rows, parallel = true)
    parallel.usesDirectIndex shouldBe serial.usesDirectIndex
    parallel.build.size shouldBe serial.build.size
    (0 until serial.build.size).foreach { i =>
      parallel.build.row(i).toSeq shouldBe serial.build.row(i).toSeq
    }
    val n = probe.size
    allMatches(parallel, Array(probe), n) shouldBe allMatches(serial, Array(probe), n)
    parallel.uniqueKeys shouldBe serial.uniqueKeys
    parallel

  test("should build the same direct index in parallel, with NULL and duplicate keys") {
    val n    = 100000
    val keys = ints(
      (0 until n).map(i =>
        if i % 17 == 0 then
          null
        else
          (i * 7) % 60000 - 5
      )*
    )
    val names = StrVec(
      (0 until n)
        .map(i =>
          if i % 5 == 0 then
            null
          else
            s"r${i}"
        )
        .toArray,
      null
    )
    val probe = ints((0 until 3000).map(i => i * 23 - 100)*)
    val jb    = sameAsSerial(Seq(keys, names), probe)
    jb.usesDirectIndex shouldBe true
    jb.uniqueKeys shouldBe false
  }

  test("should build the same direct index in parallel when most rows share a key") {
    val n    = 80000
    val keys = longs(
      (0 until n).map(i =>
        if i % 10 == 0 then
          i.toLong
        else
          7L
      )*
    )
    val probe = longs((0 until 2000).map(i => (i * 41).toLong)*)
    sameAsSerial(Seq(keys), probe).usesDirectIndex shouldBe true
  }

  test("should build the same unique direct index in parallel without NULL keys") {
    val n     = 70000
    val keys  = ints((0 until n).map(i => n - i)*)
    val probe = ints((0 until 5000).map(i => i * 17)*)
    val jb    = sameAsSerial(Seq(keys), probe, rows = n)
    jb.usesDirectIndex shouldBe true
    jb.uniqueKeys shouldBe true
  }

  test("should build the same hash tables in parallel for sparse keys") {
    val n    = 70000
    val keys = longs(
      (0 until n).map(i =>
        if i % 9 == 0 then
          null
        else
          i.toLong * 1000003L
      )*
    )
    val probe = longs((0 until 4000).map(i => i.toLong * 3000009L)*)
    sameAsSerial(Seq(keys), probe).usesDirectIndex shouldBe false
  }

  test("should index packed multi-column keys") {
    val build  = Array(ints(1, 2, 1, 3), longs(10L, 10L, 11L, 10L))
    val probe  = Array(ints(1, 1, 2, 3, 4), longs(11L, 10L, 10L, 12L, 10L))
    val (_, m) = directMatches(build, probe)
    m shouldBe Seq(Seq(2), Seq(0), Seq(1), Nil, Nil)
  }

end JoinDirectIndexTest

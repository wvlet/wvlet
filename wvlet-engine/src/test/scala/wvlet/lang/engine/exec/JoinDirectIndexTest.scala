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

  test("should index packed multi-column keys") {
    val build  = Array(ints(1, 2, 1, 3), longs(10L, 10L, 11L, 10L))
    val probe  = Array(ints(1, 1, 2, 3, 4), longs(11L, 10L, 10L, 12L, 10L))
    val (_, m) = directMatches(build, probe)
    m shouldBe Seq(Seq(2), Seq(0), Seq(1), Nil, Nil)
  }

end JoinDirectIndexTest

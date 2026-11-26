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

class LongJoinTableTest extends UniTest:
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

  /** Runs tasks on the calling thread but reports parallelism, so builds partition their rows */
  private object PartitioningRunner extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private def joinBuild(build: Array[Vec], flat: Boolean, runner: TaskRunner): JoinBuild =
    val schema = Schema(build.indices.map(i => Field(s"k${i}", build(i).ptype)))
    val keys   = build.indices.map(i => ColRef(i, build(i).ptype, s"k${i}"))
    val jb     = JoinBuild(
      BatchesOp(schema, Seq(Batch(build, build(0).size))),
      keys,
      runner = runner,
      flatTable = flat
    )
    // Dense keys would take the direct index
    jb.directIndex = false
    jb.ensureBuilt()
    jb

  /** Every match of each probe row, in chain order */
  private def allMatches(jb: JoinBuild, probe: Array[Vec]): Seq[Seq[Int]] = jb
    .firstMatches(probe, probe(0).size)
    .toSeq
    .map(first => Iterator.iterate(first)(jb.nextMatch).takeWhile(_ >= 0).toSeq)

  /** Matches through the flat table, checked against the GroupTables */
  private def flatMatches(
      build: Array[Vec],
      probe: Array[Vec],
      runner: TaskRunner = TaskRunner.Sequential
  ): (JoinBuild, Seq[Seq[Int]]) =
    val flat = joinBuild(build, flat = true, runner)
    flat.usesFlatTable shouldBe true
    val grouped = joinBuild(build, flat = false, runner)
    grouped.usesFlatTable shouldBe false
    val m = allMatches(flat, probe)
    m shouldBe allMatches(grouped, probe)
    flat.uniqueKeys shouldBe grouped.uniqueKeys
    (flat, m)

  test("should find Int32 key matches in input order") {
    val build   = Array(ints(5, 70000, 5, null, -2, 70000, 5))
    val probe   = Array(ints(5, 70000, -2, null, 6, 0, Int.MinValue, Int.MaxValue))
    val (jb, m) = flatMatches(build, probe)
    m shouldBe Seq(Seq(0, 2, 6), Seq(1, 5), Seq(4), Nil, Nil, Nil, Nil, Nil)
    jb.uniqueKeys shouldBe false
  }

  test("should find Int64 key matches at the ends of the range") {
    val build   = Array(longs(Long.MinValue, -1L, 0L, Long.MaxValue, null))
    val probe   = Array(longs(Long.MaxValue, Long.MinValue, 0L, -1L, 1L, null))
    val (jb, m) = flatMatches(build, probe)
    m shouldBe Seq(Seq(3), Seq(0), Seq(2), Seq(1), Nil, Nil)
    jb.uniqueKeys shouldBe true
  }

  test("should find matches in partitioned builds") {
    // Keys spanning less than 2^32 values take one Long per entry, wider ones two
    for step <- Seq(1003L, 1000003L) do
      val n       = 70000
      val build   = Array(longs((0 until n).map(i => (i % 40000).toLong * step - 7L)*))
      val probe   = Array(longs((0 until 5000).map(i => (i * 7).toLong * step - 7L + (i % 3))*))
      val (jb, m) = flatMatches(build, probe, PartitioningRunner)
      m.count(_.nonEmpty) shouldNotBe 0
      jb.uniqueKeys shouldBe false
  }

  test("should find matches in a build range of exactly 2^32 - 1") {
    val build  = Array(longs(5L, 5L + 0xffffffffL, 7L))
    val probe  = Array(longs(5L + 0xffffffffL, 5L, 4L, 6L + 0xffffffffL, 7L, null))
    val (_, m) = flatMatches(build, probe)
    m shouldBe Seq(Seq(1), Seq(0), Nil, Nil, Seq(2), Nil)
  }

  /** The first rows of `probe` through a table over `key`, and whether its entries are narrow */
  private def firstRowsOf(key: Array[Long], probe: Array[Long], fitRowBits: Boolean) =
    val chain  = Array.fill(key.length)(-1)
    val (t, _) = LongJoinTable.build(
      key,
      Array(key.indices.toArray),
      0,
      chain,
      TaskRunner.Sequential,
      fitRowBits = fitRowBits
    )
    val out = new Array[Int](probe.length)
    t.firstRows(probe, null, probe.length, out)
    (out.toSeq, t.hasNarrowEntries)

  test("should fit keys wider than 32 bits beside few row bits in one Long") {
    // 3 rows take 2 row bits, leaving 62 bits for the key range
    for (hi, narrow) <- Seq((1L << 40, true), ((1L << 62) - 1, true), (1L << 62, false)) do
      val key   = Array(-3L, hi - 3L, 7L)
      val probe = Array(hi - 3L, -3L, 7L, hi - 2L, -4L, 8L)
      firstRowsOf(key, probe, fitRowBits = true) shouldBe (Seq(1, 0, 2, -1, -1, -1), narrow)
      // 32 row bits leave too few for any of these ranges
      firstRowsOf(key, probe, fitRowBits = false) shouldBe (Seq(1, 0, 2, -1, -1, -1), false)
  }

  test("should find duplicate keys of a wide range through narrow entries") {
    val n              = 5000
    val key            = Array.tabulate(n)(i => (i % 1000).toLong * 1000000007L)
    val probe          = Array.tabulate(1200)(i => i.toLong * 1000000007L + (i % 2))
    val (rows, narrow) = firstRowsOf(key, probe, fitRowBits = true)
    narrow shouldBe true
    rows shouldBe probe.toSeq.map(k => key.indexWhere(_ == k))
  }

  test("should find matches when keys collide in one region") {
    // Many keys in a small region stress linear probing
    val keys   = (0 until 50).map(i => i.toLong << 40)
    val build  = Array(longs(keys*))
    val probe  = Array(longs((keys.reverse :+ (51L << 40))*))
    val (_, m) = flatMatches(build, probe)
    m shouldBe (49 to 0 by -1).map(Seq(_)) :+ Nil
  }

  test("should leave builds skewed by a frequent key to GroupTables") {
    // Most rows share one key, so one partition holds nearly every row
    val n     = 70000
    val build = Array(
      longs(
        (0 until n).map(i =>
          if i % 100 == 0 then
            i.toLong * 1000003L
          else
            7L
        )*
      )
    )
    val probe  = Array(longs(7L, 0L, 100L * 1000003L, 8L))
    val skewed = joinBuild(build, flat = true, PartitioningRunner)
    skewed.usesFlatTable shouldBe false
    val grouped = joinBuild(build, flat = false, PartitioningRunner)
    allMatches(skewed, probe) shouldBe allMatches(grouped, probe)
    allMatches(skewed, probe)(0).size shouldBe n - n / 100
  }

  test("should find the same first rows through a Bloom filter of the keys") {
    val rnd = java.util.Random(7)
    // Narrow (range < 2^32) and wide entries
    Seq(1_000_000L, 1L << 40).foreach { span =>
      val n                      = 20_000
      val keys                   = Array.fill(n)((rnd.nextLong() & Long.MaxValue) % span)
      val parts                  = 4
      val h                      = keys.map(LongJoinTable.hash)
      val rows                   = GroupTable.partitionRows(h, n, parts)
      def table(filter: Boolean) =
        val (t, _) = LongJoinTable.build(
          keys,
          rows,
          2,
          Array.fill(n)(-1),
          TaskRunner.Sequential,
          missFilter = filter,
          minFilterBytes = 0L
        )
        t
      val plain    = table(false)
      val filtered = table(true)
      plain.hasMissFilter shouldBe false
      filtered.hasMissFilter shouldBe true
      // Half build keys, half other values, and some NULLs
      val m     = 8192
      val probe =
        Array.tabulate(m)(i =>
          if i % 2 == 0 then
            keys(rnd.nextInt(n))
          else
            (rnd.nextLong() & Long.MaxValue) % span
        )
      val nulls  = Array.tabulate(m)(_ % 7 == 0)
      val expect = new Array[Int](m)
      val actual = new Array[Int](m)
      plain.firstRows(probe, nulls, m, expect)
      filtered.firstRows(probe, nulls, m, actual)
      actual.toSeq shouldBe expect.toSeq
      // Every build key is found
      expect.indices.count(i => i % 2 == 0 && !nulls(i) && expect(i) < 0) shouldBe 0
    }
  }

end LongJoinTableTest

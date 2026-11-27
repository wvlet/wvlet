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

/** The key steps of a join build run in tasks (`parallel_join_keys`) give the serial results */
class JoinBuildKeyStepsTest extends UniTest:
  /** Runs tasks on the calling thread but reports parallelism, so steps split into chunks */
  private object ChunkingRunner extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private def longs(n: Int)(f: Int => Long, isNull: Int => Boolean = _ => false): LongVec =
    val nulls = Array.tabulate(n)(isNull)
    LongVec(
      PType.Int64,
      Array.tabulate(n)(i =>
        if nulls(i) then
          0L
        else
          f(i)
      ),
      if nulls.contains(true) then
        nulls
      else
        null
    )

  test("should partition a flat table's rows as the serial hash partitioning does") {
    val n      = 10007
    val key    = Array.tabulate(n)(i => (i * 7919L) % 3001L - 1500L)
    val valid  = (0 until n).filter(_ % 5 != 0).toArray
    val nv     = valid.length
    val parts  = 8
    val h      = Array.tabulate(nv)(j => LongJoinTable.hash(key(valid(j))))
    val serial = GroupTable.partitionRows(h, nv, parts).map(_.map(valid(_)).toSeq).toSeq
    JoinBuildSteps.flatPartitions(key, valid, nv, parts, ChunkingRunner).map(_.toSeq).toSeq shouldBe
      serial
  }

  test("should pack keys in chunks as one pass does, with NULL and out-of-range rows") {
    val n     = 5003
    val a     = longs(n)(i => (i % 97).toLong, _ % 41 == 0)
    val b     = IntVec(PType.Int32, Array.tabulate(n)(i => i % 1013 - 500), null)
    val types = IndexedSeq(PType.Int64, PType.Int32)
    val rows  = (0 until n).filter(i => i % 41 != 0 && i % 3 != 0).toArray
    val keys  = Array[Vec](a, b)
    val p1    = KeyPacker.of(types, keys, rows, rows.length)
    val p2    = KeyPacker.of(types, keys, rows, rows.length, ChunkingRunner)
    p1 shouldNotBe null
    p2 shouldNotBe null
    // Probe values outside the build ranges pack as NULL
    val probe = Array[Vec](longs(n)(i => (i % 101).toLong - 2, _ % 43 == 0), b)
    for k <- Seq(keys, probe) do
      val s = p1.pack(k, n)
      val c = p2.pack(k, n, ChunkingRunner)
      c.values.toSeq shouldBe s.values.toSeq
      c.nulls.toSeq shouldBe s.nulls.toSeq
    // No NULL rows: no mask
    p2.pack(Array[Vec](longs(n)(i => (i % 97).toLong), b), n, ChunkingRunner).nulls shouldBe null
  }

  test("should find the same matches with the key steps in tasks") {
    val n      = 70001
    val schema = Schema(IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64)))
    // (customer, item) pairs with duplicates and NULLs, as TPC-DS q97's distinct sets
    val build = Array[Vec](
      longs(n)(i => (i * 31L) % 20011L, _ % 101 == 0),
      longs(n)(i => (i * 17L) % 307L + 1000000L)
    )
    def built(parallelKeys: Boolean): JoinBuild =
      val jb = JoinBuild(
        BatchesOp(schema, Seq(Batch(build, n))),
        IndexedSeq(ColRef(0, PType.Int64, "a"), ColRef(1, PType.Int64, "b")),
        runner = ChunkingRunner
      )
      jb.directIndex = false
      jb.parallelKeys = parallelKeys
      jb.ensureBuilt()
      jb
    val probe = Array[Vec](
      longs(3000)(i => (i * 13L) % 20011L, _ % 97 == 0),
      longs(3000)(i => (i * 5L) % 311L + 1000000L)
    )
    def matches(jb: JoinBuild): Seq[Seq[Int]] = jb
      .firstMatches(probe, 3000)
      .toSeq
      .map(first => Iterator.iterate(first)(jb.nextMatch).takeWhile(_ >= 0).toSeq)
    val serial   = built(parallelKeys = false)
    val parallel = built(parallelKeys = true)
    parallel.usesFlatTable shouldBe true
    val m = matches(parallel)
    m.count(_.nonEmpty) shouldNotBe 0
    m shouldBe matches(serial)
    parallel.uniqueKeys shouldBe serial.uniqueKeys
  }

  test("should find the same rows in a flat table whose partitions take their own ranges") {
    val n     = 20000
    val key   = Array.tabulate(n)(i => (i % 9001).toLong * 1000003L - 77L)
    val rows  = (0 until n).toArray
    val h     = Array.tabulate(n)(i => LongJoinTable.hash(key(i)))
    val parts = GroupTable.partitionRows(h, n, 8)
    def firstRows(parallelSteps: Boolean): Seq[Int] =
      val chain  = Array.fill(n)(-1)
      val (t, _) = LongJoinTable.build(
        key,
        parts,
        3,
        chain,
        ChunkingRunner,
        parallelSteps = parallelSteps
      )
      val probe = Array.tabulate(4000)(i => (i * 3L) * 1000003L - 77L + (i % 2))
      val out   = new Array[Int](probe.length)
      t.firstRows(probe, null, probe.length, out)
      out.toSeq
    val p = firstRows(parallelSteps = true)
    p.count(_ >= 0) shouldNotBe 0
    p shouldBe firstRows(parallelSteps = false)
  }

end JoinBuildKeyStepsTest

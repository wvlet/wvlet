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

/**
  * A LEFT join's flat table gets a Bloom filter when its first probe batch mostly misses
  * (`sampled_miss_filter`), and probes find the same first rows either way
  */
class MissFilterSampleTest extends UniTest:
  private val n     = 20_000
  private val rnd   = java.util.Random(11)
  private val keys  = Array.fill(n)((rnd.nextLong() & Long.MaxValue) % 1_000_000L)
  private val parts = 4
  private val rows  = GroupTable.partitionRows(keys.map(LongJoinTable.hash), n, parts)

  private def table(): LongJoinTable =
    val (t, _) = LongJoinTable.build(keys, rows, 2, Array.fill(n)(-1), TaskRunner.Sequential)
    t

  // m probe keys, a `hit` share of them build keys, the others values above every build key
  private def probe(m: Int, hit: Double): LongVec = LongVec(
    PType.Int64,
    Array.tabulate(m)(i =>
      if i < m * hit then
        keys(rnd.nextInt(n))
      else
        2_000_000L + rnd.nextInt(1_000_000)
    ),
    null
  )

  private def firstRows(t: LongJoinTable, s: MissFilterSample, key: LongVec): Seq[Int] =
    val out = new Array[Int](key.size)
    MissFilterSample.probe(t, s, key, key.size, out)
    out.toSeq

  test("should add a Bloom filter after a first batch that mostly misses") {
    val t = table()
    val s = MissFilterSample(keys, rows)
    // Too few rows to decide
    val small = probe(100, 0.1)
    firstRows(t, s, small) shouldBe firstRows(t, null, small)
    s.hasFilter shouldBe false
    val first = probe(4096, 0.1)
    firstRows(t, s, first) shouldBe firstRows(t, null, first)
    s.hasFilter shouldBe true
    s.tableFor(t).hasMissFilter shouldBe true
    // Later probes go through the filter and find the same rows
    val later = probe(4096, 0.5)
    val found = firstRows(t, s, later)
    found shouldBe firstRows(t, null, later)
    found.take(2048).count(_ < 0) shouldBe 0
  }

  test("should keep probing without a filter after a first batch that mostly matches") {
    val t     = table()
    val s     = MissFilterSample(keys, rows)
    val first = probe(4096, 0.9)
    firstRows(t, s, first) shouldBe firstRows(t, null, first)
    s.hasFilter shouldBe false
    // Decided once: a later batch of misses doesn't add one
    val later = probe(4096, 0.0)
    firstRows(t, s, later) shouldBe firstRows(t, null, later)
    s.hasFilter shouldBe false
    s.tableFor(t) shouldBeTheSameInstanceAs t
  }

end MissFilterSampleTest

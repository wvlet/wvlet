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

import wvlet.lang.engine.vector.Vec

import java.util.concurrent.atomic.AtomicBoolean

/**
  * A pending decision whether a LEFT join's large flat table gets a Bloom filter of its keys
  * (`sampled_miss_filter`), taken from the first probe batch of at least
  * [[MissFilterSample.MinRows]] rows: when at most half of its rows match, the probes after it go
  * through a copy of the table with the filter that `probe_miss_filter` gives joins known at plan
  * time to mostly miss (anti joins, unmatched-only LEFT joins). A LEFT join can't tell from its
  * plan: TPC-DS q75 and q80 join sales to returns, which most sales rows miss, while dimension LEFT
  * joins mostly match.
  *
  * Holds the keys and partition rows the table was built from until the decision. One thread takes
  * the decision and fills the filter; the others keep probing the table without it meanwhile, which
  * gives the same matches.
  */
private[exec] final class MissFilterSample(
    private var key: Array[Long],
    private var rowsOf: Array[Array[Int]]
):
  private val taken = AtomicBoolean(false)

  // The table with the filter, once the decision gave it one
  @volatile
  private var filtered: LongJoinTable = null

  /** The table to probe instead of `t`: its filtered copy, once made */
  def tableFor(t: LongJoinTable): LongJoinTable =
    val f = filtered
    if f != null then
      f
    else
      t

  /**
    * Take the decision after a probe of `t` by n rows found `first` (-1 for no match), unless it is
    * taken or n is below [[MissFilterSample.MinRows]]. The decision drops the build keys
    */
  def decide(t: LongJoinTable, first: Array[Int], n: Int): Unit =
    if n >= MissFilterSample.MinRows && !taken.get() && taken.compareAndSet(false, true) then
      val (k, r) = (key, rowsOf)
      key = null
      rowsOf = null
      if MissFilterSample.mostlyMisses(first, n) then
        filtered = t.withMissFilter(k, r)

  /** Whether probes now go through a filtered table (for tests) */
  def hasFilter: Boolean = filtered != null

end MissFilterSample

private[exec] object MissFilterSample:
  /** The fewest probe rows that decide */
  inline val MinRows = 1024

  /**
    * The first build row of each of the first n probe keys `key` (normalized) into `out`, through
    * `t` or, with a pending or taken decision `s` (null for none), the table it gives
    */
  def probe(t: LongJoinTable, s: MissFilterSample, key: Vec, n: Int, out: Array[Int]): Unit =
    if s == null then
      t.firstRows(key, n, out)
    else
      val table = s.tableFor(t)
      table.firstRows(key, n, out)
      if table eq t then
        s.decide(t, out, n)

  /** Whether at most half of the n probe rows matched */
  def mostlyMisses(first: Array[Int], n: Int): Boolean =
    var matched = 0
    var i       = 0
    while i < n do
      if first(i) >= 0 then
        matched += 1
      i += 1
    matched * 2 <= n

  /**
    * A pending decision for build `b`'s table `t`, built from `key` and `rowsOf`, or null when `b`
    * doesn't sample (`sampledMissFilter` off, or `missFilter` decided at plan time) or `t` is too
    * small to keep a filter (it mostly stays in cache)
    */
  def of(
      b: JoinBuild,
      t: LongJoinTable,
      key: Array[Long],
      rowsOf: Array[Array[Int]],
      minFilterBytes: Long = LongJoinTable.MinMissFilterBytes
  ): MissFilterSample =
    if !b.sampledMissFilter || b.missFilter || t.hasMissFilter || t.entryBytes < minFilterBytes then
      null
    else
      MissFilterSample(key, rowsOf)

end MissFilterSample

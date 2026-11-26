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

/**
  * Hash join outputs made from the first match of each probe row alone, without match pairs: semi
  * and anti joins, and the LEFT joins that emit only their unmatched rows
  * ([[HashJoinOp.unmatchedOnly]], `unmatched_left_probe`). Kept out of Operators.scala, which
  * scalafmt reflows as a whole once it grows
  */
private[exec] object ProbeRows:

  /** The plan-text tags of a join's probe options */
  def tags(j: HashJoinOp): String =
    val unmatched =
      if j.unmatchedOnly then
        ", unmatched only"
      else
        ""
    val filter =
      if j.joinBuild.missFilter then
        ", miss filter"
      else
        ""
    unmatched + filter

  /**
    * A semi (`wantMatch`: rows with a match) or anti (rows without one) join's output from the
    * first matches, or null when no row is kept
    */
  def exists(probe: Batch, first: Array[Int], wantMatch: Boolean, lateGather: Boolean): Batch =
    val keep = new Array[Boolean](probe.size)
    var kept = 0
    var i    = 0
    while i < probe.size do
      if (first(i) >= 0) == wantMatch then
        keep(i) = true
        kept += 1
      i += 1
    if kept == 0 then
      null
    else if kept == probe.size then
      probe
    else if lateGather then
      probe.takeWhere(keep)
    else
      probe.filter(keep)

  /**
    * The rows of `probe` without a match (`first` is -1), in probe order, next to NULL rows of
    * `build`, or null when every row matched. With `lateGather`, the columns are deferred
    */
  def unmatched(probe: Batch, build: Batch, first: Array[Int], lateGather: Boolean): Batch =
    val n    = probe.size
    var kept = 0
    var i    = 0
    while i < n do
      if first(i) < 0 then
        kept += 1
      i += 1
    if kept == 0 then
      return null
    val rows =
      if kept == n then
        probe
      else
        val idx = new Array[Int](kept)
        var k   = 0
        i = 0
        while i < n do
          if first(i) < 0 then
            idx(k) = i
            k += 1
          i += 1
        take(probe, idx, kept, lateGather)
    rows ++ take(build, Vec.filledInts(kept, -1), kept, lateGather)

  private def take(b: Batch, idx: Array[Int], n: Int, lateGather: Boolean): Batch =
    if lateGather then
      b.take(idx, n)
    else
      b.gather(idx, n)

end ProbeRows

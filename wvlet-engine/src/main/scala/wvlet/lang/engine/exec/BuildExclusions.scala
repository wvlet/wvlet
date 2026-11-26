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

import wvlet.lang.engine.expr.Kernels
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.Vec

/**
  * The exclusion marks of a RIGHT SEMI [[HashJoinOp]] with `exclude` (`semi_anti_one_probe`): an
  * EXISTS and a NOT EXISTS over the same table checked in one probe pass
  * ([[wvlet.lang.engine.runtime.SemiAntiProbe]]). Each pair of a probe row and a build row with
  * equal keys marks the build row matched when the residual holds, as any RIGHT SEMI join does, and
  * excluded when `exclude` (a condition over the joined row, as the residual) holds. The join emits
  * the matched build rows that no pair excluded. The marks are shared by the join's probe copies as
  * the matched marks are: copies only set entries, and the rows are read after every copy finished
  */
object BuildExclusions:

  /** The explain text of an exclusion */
  def tag(exclude: Option[VExpr]): String = exclude.map(e => s", exclude=${e.sig}").getOrElse("")

  /**
    * The exclusion marks a join reads: the build's shared ones, or its own for a copy that tracks
    * its own matches over a shared build ([[OuterMode.Own]])
    */
  def marks(build: JoinBuild, mode: OuterMode, sharedBuild: Boolean, size: Int): Array[Boolean] =
    if mode == OuterMode.Own && sharedBuild then
      new Array[Boolean](size)
    else
      build.excluded

  /** Mark the build rows `rIdx(j)` of the pairs whose exclusion `cond` is TRUE */
  def mark(cond: Vec, rIdx: Array[Int], excluded: Array[Boolean]): Unit =
    val mask = Kernels.trueMask(cond)
    var j    = 0
    while j < rIdx.length do
      if mask(j) then
        excluded(rIdx(j)) = true
      j += 1

  /**
    * The build positions in `range` (clipped to [0, n)) a join emits at the end: those whose match
    * mark is `value` ([[BuildPositions.in]]), or, with exclusion marks, the matched ones not
    * excluded
    */
  def positions(
      matched: Array[Boolean],
      excluded: Array[Boolean],
      n: Int,
      range: (Int, Int),
      value: Boolean
  ): Array[Int] =
    if excluded == null then
      BuildPositions.in(matched, n, range, value)
    else
      val from  = math.max(0, range._1)
      val until = math.min(n, range._2)
      val out   = Array.newBuilder[Int]
      var i     = from
      while i < until do
        if matched(i) && !excluded(i) then
          out += i
        i += 1
      out.result()

end BuildExclusions

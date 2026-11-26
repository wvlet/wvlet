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

/** Positions of a hash join's build rows by their match marks */
object BuildPositions:

  /**
    * The positions in `range` (clipped to [0, n)) where mask is value: the build rows a
    * [[OuterMode.Tail]] run emits, over one range of the build with `parallel_outer_tail`
    */
  def in(mask: Array[Boolean], n: Int, range: (Int, Int), value: Boolean): Array[Int] =
    val from  = math.max(0, range._1)
    val until = math.min(n, range._2)
    var count = 0
    var i     = from
    while i < until do
      if mask(i) == value then
        count += 1
      i += 1
    val out = new Array[Int](count)
    var k   = 0
    i = from
    while i < until do
      if mask(i) == value then
        out(k) = i
        k += 1
      i += 1
    out

end BuildPositions

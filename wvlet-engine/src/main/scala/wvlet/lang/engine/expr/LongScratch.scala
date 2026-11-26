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
package wvlet.lang.engine.expr

/**
  * Long arrays reused from batch to batch by one task (`agg_scratch_buffers`): [[take]] hands out a
  * distinct array per call until [[reset]], after which the same arrays are handed out again, with
  * their old values. A vector over such an array is valid only until the next [[reset]], so only a
  * reader that is done with it by then (an aggregation folding one batch) may use them.
  *
  * Allocating a fresh array per batch makes the JVM zero it first: TPC-H q1's fused decimal
  * products spent about 1% of its CPU zeroing their outputs.
  */
final class LongScratch:
  private var arrays = new Array[Array[Long]](4)
  private var used   = 0

  /** Hand out the arrays again from the first; returns this scratch */
  def reset(): LongScratch =
    used = 0
    this

  /** An array of exactly n entries (a vector's size is its array's length), not zeroed */
  def take(n: Int): Array[Long] =
    if used == arrays.length then
      arrays = java.util.Arrays.copyOf(arrays, used * 2)
    var a = arrays(used)
    if a == null || a.length != n then
      a = new Array[Long](n)
      arrays(used) = a
    used += 1
    a

end LongScratch

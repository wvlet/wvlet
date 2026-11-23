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

import java.util.concurrent.atomic.AtomicInteger

/**
  * Group table sizes for the partial aggregations that run once per scan part (see [[MorselOp]]).
  *
  * The planner's `estimate` covers the whole query and ignores filters, so pre-sizing every part's
  * table to it could zero far more memory than the parts use. Parts that start before any part has
  * finished get the estimate capped by their row count; later parts get the largest group count a
  * finished part reported, also capped by their row count. An `estimate` of 0 (unknown, or
  * pre-sizing turned off) gives no hint at all.
  */
final class PartialGroupSizer(estimate: Int):
  // The largest group count of a finished part, or -1 before any part finishes
  private val observed = AtomicInteger(-1)

  /** The expected group count of the part read by `input` */
  def hint(input: Operator): Int =
    if estimate <= 0 then
      0
    else
      val seen = observed.get()
      val base =
        if seen >= 0 then
          seen
        else
          estimate
      val rows =
        input match
          case p: IteratorOp if p.rows >= 0 =>
            p.rows
          case _ =>
            Long.MaxValue
      math.min(math.min(base.toLong, rows), GroupTable.MaxPresizeGroups.toLong).toInt

  /** Records the group count of a finished part */
  def report(groups: Int): Unit =
    if estimate > 0 then
      observed.accumulateAndGet(groups, math.max)

end PartialGroupSizer

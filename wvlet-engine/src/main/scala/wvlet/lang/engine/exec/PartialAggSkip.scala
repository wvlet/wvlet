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

import java.util.concurrent.atomic.AtomicLong

/**
  * Decides when the partial aggregates of a two-phase aggregation stop grouping, shared by the
  * partial [[HashAggOp]]s of every part of the scan. A partial aggregate pays a hash table insert
  * per row to save the final aggregate work only for keys repeated within its part; when nearly
  * every row is a new group (e.g. a GROUP BY of a near-unique key), the rows go to the final
  * aggregate as one-row partial results instead, as DuckDB's partial aggregation abandons a hash
  * table that doesn't reduce its input.
  *
  * Grouping stops once the parts' partial aggregates have seen at least `minRows` rows (summed over
  * the finished parts, or within one part) and kept at least `minRatio` of them as groups. The
  * decision is never reverted. Which rows skip grouping depends on when parts finish, but the final
  * aggregate's groups and values don't: only the order of its groups and of floating-point
  * additions can change.
  */
final class PartialAggSkip(
    minRows: Long = PartialAggSkip.MinRows,
    minRatio: Double = PartialAggSkip.MinRatio
):
  private val rows   = AtomicLong(0)
  private val groups = AtomicLong(0)
  @volatile
  private var passed = false

  /** Whether partial aggregates pass their rows through without grouping */
  def skipping: Boolean = passed

  /** Whether a partial aggregate that grouped `n` rows into `g` groups so far should stop */
  def shouldStop(n: Long, g: Long): Boolean =
    if passed then
      true
    else if n >= minRows && g >= minRatio * n then
      passed = true
      true
    else
      false

  /** Records the rows and groups of a part that grouped all of its rows */
  def report(n: Long, g: Long): Unit =
    if n > 0 then
      val total = rows.addAndGet(n)
      val kept  = groups.addAndGet(g)
      shouldStop(total, kept)

end PartialAggSkip

object PartialAggSkip:
  /** Rows to see before deciding */
  final val MinRows: Long = 16384

  /** The fraction of rows kept as groups above which grouping stops */
  final val MinRatio: Double = 0.9

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
import wvlet.uni.cloud.parquet.ParquetFilter

/**
  * A row-group filter that a hash join's build gives a scan on its probe side: the [min, max] range
  * of the non-NULL values of build key `key`, over the scan column `column`. A probe row outside
  * the range has no match, and only joins that drop unmatched probe rows (on every operator between
  * the scan and the join) get one, so the scan may skip such rows. The build sets the filter once
  * built, before the probe side starts reading; a scan that starts earlier reads without it.
  *
  * With `rows` (`row_join_filters`), the build also sets a [[RowKeyFilter]] over its keys, which
  * the scan applies to every row it reads, from the first batch read after the build is done.
  *
  * `range(min, max)` translates the range of integer key values (epoch days for dates, micros for
  * timestamps) into the scan's filter, or None when it can't be expressed.
  */
final class RuntimeFilter(
    val key: Int,
    val column: String,
    range: (Long, Long) => Option[ParquetFilter],
    val rows: Boolean = false
):
  @volatile
  private var current: Option[ParquetFilter] = None

  @volatile
  private var perRow: RowKeyFilter = null

  /** The per-row filter, once the build has set it (null otherwise) */
  def rowFilter: RowKeyFilter = perRow

  /** The filter, once the build has set it */
  def filter: Option[ParquetFilter] = current

  /** Set the filter from the build keys at rows `rows(0 until n)`, which are all non-NULL */
  def set(keys: Vec, rows: Array[Int], n: Int, runner: TaskRunner = TaskRunner.Sequential): Unit =
    JoinBuildSteps
      .keyRange(keys, rows, n, runner)
      .foreach { bounds =>
        setRange(bounds)
        if this.rows then
          RowKeyFilter.of(keys, rows, n, bounds._1, bounds._2).foreach(perRow = _)
      }

  /** Set the filter from the [min, max] range of the non-NULL build keys */
  def setRange(bounds: (Long, Long)): Unit = current = range(bounds._1, bounds._2)

  override def toString: String = column

end RuntimeFilter

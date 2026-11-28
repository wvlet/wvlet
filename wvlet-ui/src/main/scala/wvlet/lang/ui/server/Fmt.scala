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
package wvlet.lang.ui.server

import java.time.Instant
import scala.scalajs.js

/** Formats the server's numbers and times for display */
object Fmt:

  /** Bytes in B, KB, MB, GB, or TB (powers of 1024), with one decimal past KB */
  def bytes(n: Long): String =
    val units = Seq("B", "KB", "MB", "GB", "TB")
    var v     = n.toDouble
    var i     = 0
    while v >= 1024 && i < units.size - 1 do
      v /= 1024
      i += 1
    if i == 0 then
      s"${n} B"
    else
      f"${v}%.1f ${units(i)}"

  /** A duration such as 850 ms, 2.3 s, 4m 05s, or 1h 02m */
  def duration(millis: Long): String =
    val ms = millis.max(0L)
    if ms < 1000 then
      s"${ms} ms"
    else if ms < 60_000 then
      f"${ms / 1000.0}%.1f s"
    else if ms < 3_600_000 then
      f"${ms / 60_000}m ${(ms % 60_000) / 1000}%02ds"
    else
      f"${ms / 3_600_000}h ${(ms % 3_600_000) / 60_000}%02dm"

  /** A count with thousands separators */
  def count(n: Long): String = "%,d".format(n)

  /** A percentage of `part` in `whole`, 0 when the whole is 0 */
  def percent(part: Long, whole: Long): Int =
    if whole <= 0 then
      0
    else
      math.min(100L, math.round(part * 100.0 / whole)).toInt

  /** The local wall-clock time of an instant, e.g. 14:05:09 */
  def clock(t: Instant): String =
    val d = new js.Date(t.toEpochMilli.toDouble)
    f"${d.getHours().toInt}%02d:${d.getMinutes().toInt}%02d:${d.getSeconds().toInt}%02d"

  /** The local date and time of an instant, e.g. 2026-11-27 14:05:09 */
  def dateTime(t: Instant): String =
    val d = new js.Date(t.toEpochMilli.toDouble)
    f"${d.getFullYear().toInt}%04d-${d.getMonth().toInt + 1}%02d-${d.getDate().toInt}%02d " +
      clock(t)

  /** How long ago `t` was, relative to `now`, e.g. 5 s ago */
  def ago(t: Instant, now: Instant): String = s"${duration(now.toEpochMilli - t.toEpochMilli)} ago"

end Fmt

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
package wvlet.lang.engine.vector

import wvlet.lang.api.StatusCode

/** Platform-specific time zone rules */
trait TimeZoneCompat:
  /**
    * The UTC offset in seconds of the named IANA time zone (e.g. `Asia/Tokyo`) at the instant
    * `epochSecond`. Fails with INVALID_ARGUMENT for an unknown zone
    */
  def zoneOffsetSeconds(zone: String, epochSecond: Long): Int

/**
  * Time zone conversions between wall-clock times and instants, both as microseconds since the
  * epoch. The engine's session time zone is UTC, so a TIMESTAMPTZ's wall-clock time in the session
  * is its UTC value
  */
object TimeZones extends TimeZoneCompatImpl:
  private val utcNames = Set("UTC", "Etc/UTC", "GMT", "Etc/GMT", "Z", "Zulu", "Universal")

  private[vector] def unknownZone(zone: String) = StatusCode
    .INVALID_ARGUMENT
    .newException(s"Unknown time zone '${zone}'")

  private def offsetMicros(zone: String, instantMicros: Long): Long =
    if utcNames.contains(zone) then
      0L
    else
      zoneOffsetSeconds(zone, Math.floorDiv(instantMicros, ValueOps.MicrosPerSecond)).toLong *
        ValueOps.MicrosPerSecond

  /** The wall-clock time in `zone` of an instant (`tstz AT TIME ZONE zone`) */
  def toLocal(instantMicros: Long, zone: String): Long =
    instantMicros + offsetMicros(zone, instantMicros)

  /**
    * The instant of a wall-clock time in `zone` (`ts AT TIME ZONE zone`). As in DuckDB, a time
    * skipped by a forward transition uses the offset before it (02:30 on a spring-forward night in
    * New York is 07:30 UTC), and a repeated time uses the offset after the transition
    */
  def toInstant(localMicros: Long, zone: String): Long =
    val day    = ValueOps.MicrosPerDay
    val before = offsetMicros(zone, localMicros - day)
    val after  = offsetMicros(zone, localMicros + day)
    if before == after then
      localMicros - before
    else
      val i1      = localMicros - before
      val i2      = localMicros - after
      val i1Valid = offsetMicros(zone, i1) == before
      val i2Valid = offsetMicros(zone, i2) == after
      if i2Valid then
        i2
      else if i1Valid then
        i1
      else
        // In a gap: neither offset maps back to this wall-clock time
        i1

end TimeZones

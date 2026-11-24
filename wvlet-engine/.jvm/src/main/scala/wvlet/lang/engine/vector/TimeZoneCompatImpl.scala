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

import java.time.DateTimeException
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.ConcurrentHashMap

/** Time zone rules from the JDK's IANA database */
trait TimeZoneCompatImpl extends TimeZoneCompat:
  private val zones = ConcurrentHashMap[String, ZoneId]()

  def zoneOffsetSeconds(zone: String, epochSecond: Long): Int =
    val z = zones.computeIfAbsent(
      zone,
      name =>
        try
          ZoneId.of(name)
        catch
          case _: DateTimeException =>
            throw TimeZones.unknownZone(name)
    )
    z.getRules.getOffset(Instant.ofEpochSecond(epochSecond)).getTotalSeconds

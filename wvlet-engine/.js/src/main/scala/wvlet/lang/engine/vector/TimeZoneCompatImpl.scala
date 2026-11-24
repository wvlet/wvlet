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

import scala.collection.mutable
import scala.scalajs.js

/** Time zone rules from the JavaScript runtime's Intl database */
trait TimeZoneCompatImpl extends TimeZoneCompat:
  private val formats = mutable.Map.empty[String, js.Dynamic]

  private def formatOf(zone: String): js.Dynamic = formats.getOrElseUpdate(
    zone,
    try
      js.Dynamic
        .newInstance(js.Dynamic.global.Intl.DateTimeFormat)(
          "en-US",
          js.Dynamic
            .literal(
              timeZone = zone,
              hourCycle = "h23",
              year = "numeric",
              month = "numeric",
              day = "numeric",
              hour = "numeric",
              minute = "numeric",
              second = "numeric"
            )
        )
    catch
      case _: js.JavaScriptException =>
        throw TimeZones.unknownZone(zone)
  )

  def zoneOffsetSeconds(zone: String, epochSecond: Long): Int =
    val parts  = formatOf(zone).formatToParts(new js.Date(epochSecond.toDouble * 1000))
    val fields = mutable.Map.empty[String, Int]
    parts
      .asInstanceOf[js.Array[js.Dynamic]]
      .foreach { p =>
        val t = p.selectDynamic("type").asInstanceOf[String]
        if t != "literal" then
          fields(t) = p.value.asInstanceOf[String].toInt
      }
    val days  = ValueOps.daysFromCivil(fields("year"), fields("month"), fields("day")).toLong
    val local = days * 86400L + fields("hour") * 3600L + fields("minute") * 60L + fields("second")
    (local - epochSecond).toInt

end TimeZoneCompatImpl

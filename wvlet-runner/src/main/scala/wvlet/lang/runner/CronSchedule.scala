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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

/**
  * A parsed five-field cron expression (minute, hour, day-of-month, month, day-of-week) supporting
  * `*`, values, ranges (`a-b`), steps (`*&#47;n`, `a-b/n`), and comma lists. Day-of-week accepts
  * 0-7 with both 0 and 7 meaning Sunday. Following standard cron semantics, when both day fields
  * are restricted a time matches if *either* field matches
  */
case class CronSchedule(
    expression: String,
    minutes: Set[Int],
    hours: Set[Int],
    daysOfMonth: Set[Int],
    months: Set[Int],
    daysOfWeek: Set[Int],
    domRestricted: Boolean,
    dowRestricted: Boolean
):
  /** True if the given time (truncated to the minute) matches this schedule */
  def matches(t: ZonedDateTime): Boolean =
    minutes.contains(t.getMinute) && hours.contains(t.getHour) &&
      months.contains(t.getMonthValue) && dayMatches(t)

  private def dayMatches(t: ZonedDateTime): Boolean =
    val domMatch = daysOfMonth.contains(t.getDayOfMonth)
    // java.time: MONDAY=1 .. SUNDAY=7; cron: SUNDAY=0
    val dowMatch = daysOfWeek.contains(t.getDayOfWeek.getValue % 7)
    (domRestricted, dowRestricted) match
      case (true, true) =>
        domMatch || dowMatch
      case (true, false) =>
        domMatch
      case (false, true) =>
        dowMatch
      case (false, false) =>
        true

  /** The first matching time strictly after the given time */
  def nextAfter(t: ZonedDateTime): ZonedDateTime =
    var candidate = t.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1)
    val limit     = t.plusYears(4)
    while !matches(candidate) do
      // Skip in day steps while the date cannot match to keep the scan cheap
      candidate =
        if !months.contains(candidate.getMonthValue) || !dayMatches(candidate) then
          candidate.plusDays(1).truncatedTo(ChronoUnit.DAYS)
        else
          candidate.plusMinutes(1)
      if candidate.isAfter(limit) then
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Cron expression '${expression}' never fires")
    candidate

end CronSchedule

object CronSchedule:

  /** Parse a five-field cron expression, e.g. `0 2 * * *` */
  def parse(expression: String): CronSchedule =
    val fields = expression.trim.split("""\s+""").toList
    if fields.size != 5 then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Invalid cron expression '${expression}': expected 5 fields (minute hour day month weekday), found ${fields
              .size}"
        )
    val List(minF, hourF, domF, monthF, dowF) = fields: @unchecked
    val (minutes, _)                          = parseField(expression, minF, 0, 59)
    val (hours, _)                            = parseField(expression, hourF, 0, 23)
    val (dom, domRestricted)                  = parseField(expression, domF, 1, 31)
    val (months, _)                           = parseField(expression, monthF, 1, 12)
    val (dowRaw, dowRestricted)               = parseField(expression, dowF, 0, 7)
    // Both 0 and 7 mean Sunday
    val dow = dowRaw.map(d =>
      if d == 7 then
        0
      else
        d
    )
    CronSchedule(expression, minutes, hours, dom, months, dow, domRestricted, dowRestricted)

  /** Parse one cron field into its matching values and whether it restricts the field */
  private def parseField(
      expression: String,
      field: String,
      min: Int,
      max: Int
  ): (Set[Int], Boolean) =
    def fail(reason: String): Nothing =
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"Invalid cron expression '${expression}': ${reason}")

    def parseValue(s: String): Int =
      val v =
        try
          s.toInt
        catch
          case _: NumberFormatException =>
            fail(s"'${s}' is not a number")
      if v < min || v > max then
        fail(s"value ${v} is out of range [${min}, ${max}]")
      v

    // range part: *, a, a-b (before an optional /step)
    def parseRange(s: String): Range =
      s match
        case "*" =>
          min to max
        case r if r.contains("-") =>
          r.split("-", 2) match
            case Array(a, b) =>
              val start = parseValue(a)
              val end   = parseValue(b)
              if start > end then
                fail(s"range ${s} is inverted")
              start to end
            case _ =>
              fail(s"cannot parse range '${s}'")
        case v =>
          val value = parseValue(v)
          value to value

    val values =
      field
        .split(",")
        .toList
        .flatMap { part =>
          part.split("/", 2) match
            case Array(range) =>
              parseRange(range)
            case Array(range, step) =>
              val s = parseValue(step)
              if s <= 0 then
                fail(s"step must be positive in '${part}'")
              parseRange(range).by(s)
            case _ =>
              fail(s"cannot parse field part '${part}'")
        }
        .toSet
    if values.isEmpty then
      fail(s"field '${field}' matches no values")
    (values, field != "*")

  end parseField

end CronSchedule

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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.TimeZones
import wvlet.lang.engine.vector.ValueOps

/** Date and time kernels matching DuckDB 2.0.1 (timestamps as microseconds since the epoch) */
object DateTimeFunctions:
  private val MicrosPerDay    = ValueOps.MicrosPerDay
  private val MicrosPerSecond = ValueOps.MicrosPerSecond

  private val dayNames = IndexedSeq(
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday"
  )

  private val monthNames = IndexedSeq(
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  )

  private def dayOf(micros: Long): Int = Math.floorDiv(micros, MicrosPerDay).toInt

  /** ISO day of week, 1 = Monday through 7 = Sunday */
  private def isoDow(day: Int): Int = Math.floorMod(day + 3, 7) + 1

  /** The full English day name (Monday, ...) */
  def dayName(micros: Long): String = dayNames(isoDow(dayOf(micros)) - 1)

  /** The full English month name (January, ...) */
  def monthName(micros: Long): String = monthNames(ValueOps.civilFromDays(dayOf(micros))._2 - 1)

  /** The ISO-8601 week-numbering year: the year of the Thursday in the same ISO week */
  def isoYear(micros: Long): Long =
    val day = dayOf(micros)
    ValueOps.civilFromDays(day - isoDow(day) + 4)._1.toLong

  /**
    * DuckDB's `date_sub(part, start, end)`: the number of complete parts from start to end
    * (negative when end is before start). Months count as DuckDB's `age` does, after moving a start
    * day past the end of a shorter end month back to that month's last day
    */
  def dateSub(part: String, start: Long, end: Long): Long =
    def micros(unit: Long): Long = (end - start) / unit
    part.toLowerCase match
      case "year" | "years" | "y" | "yr" | "yrs" | "isoyear" =>
        monthsBetween(start, end) / 12
      case "quarter" | "quarters" =>
        monthsBetween(start, end) / 3
      case "month" | "months" | "mon" | "mons" =>
        monthsBetween(start, end)
      case "decade" | "decades" =>
        monthsBetween(start, end) / 120
      case "century" | "centuries" =>
        monthsBetween(start, end) / 1200
      case "millennium" | "millennia" =>
        monthsBetween(start, end) / 12000
      case "week" | "weeks" | "w" | "yearweek" =>
        micros(7 * MicrosPerDay)
      case "day" | "days" | "d" | "dayofmonth" | "dow" | "dayofweek" | "isodow" | "doy" |
          "dayofyear" =>
        micros(MicrosPerDay)
      case "hour" | "hours" | "h" | "hr" | "hrs" =>
        micros(3600L * MicrosPerSecond)
      case "minute" | "minutes" | "m" | "min" | "mins" =>
        micros(60L * MicrosPerSecond)
      case "second" | "seconds" | "s" | "sec" | "secs" =>
        micros(MicrosPerSecond)
      case "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" =>
        micros(1000L)
      case "microsecond" | "microseconds" | "us" | "usec" | "usecs" =>
        micros(1L)
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported date_sub part: ${part}")
    end match

  end dateSub

  private def monthsBetween(start0: Long, end: Long): Long =
    if start0 > end then
      -monthsBetween(end, start0)
    else
      val endDay          = dayOf(end)
      val endTime         = Math.floorMod(end, MicrosPerDay)
      val (ey, em, ed)    = ValueOps.civilFromDays(endDay)
      val startTime       = Math.floorMod(start0, MicrosPerDay)
      val (sy0, sm0, sd0) = ValueOps.civilFromDays(dayOf(start0))
      val endDays         = ValueOps.daysInMonth(ey, em)
      var sd              = sd0
      if endDays == ed && (sd0 > endDays || (sd0 == endDays && startTime < endTime)) then
        // Move back to the same time on the last day of the (shorter) end month
        sd = endDays
      val months = (ey - sy0) * 12L + (em - sm0)
      // age() borrows a month when the end's day and time come before the start's
      if ed < sd || (ed == sd && endTime < startTime) then
        months - 1
      else
        months

  /** True when the strptime format reads a UTC offset or zone name, so it returns TIMESTAMPTZ */
  def hasZone(format: String): Boolean =
    var i     = 0
    var found = false
    while i < format.length - 1 && !found do
      if format.charAt(i) == '%' then
        val c = format.charAt(i + 1)
        if c == 'z' || c == 'Z' then
          found = true
        i += 2
      else
        i += 1
    found

  private def parseError(s: String, format: String) = StatusCode
    .INVALID_ARGUMENT
    .newException(s"Could not parse string \"${s}\" according to format specifier \"${format}\"")

  /**
    * Parse `s` with strftime codes, as DuckDB's `strptime`: the whole string must match. Returns
    * microseconds since the epoch: an instant when the format has `%z` or `%Z`, a wall-clock time
    * otherwise. Returns None when the string does not match
    */
  def strptime(s: String, format: String): Option[Long] =
    var pos                     = 0
    var year                    = 1900
    var month                   = 1
    var day                     = 1
    var doy                     = -1
    var hour                    = 0
    var minute                  = 0
    var second                  = 0
    var micros                  = 0L
    var pm: Option[Boolean]     = None
    var offsetSec: Option[Long] = None
    var zone: Option[String]    = None
    var ok                      = true
    // Digits read by the last readInt (after any skipped spaces)
    var digitsRead = 0

    def readInt(maxDigits: Int, signed: Boolean = false): Option[Int] =
      // Spaces before a number are skipped, as in DuckDB
      while pos < s.length && s.charAt(pos) == ' ' do
        pos += 1
      val start = pos
      var neg   = false
      if signed && pos < s.length && (s.charAt(pos) == '-' || s.charAt(pos) == '+') then
        neg = s.charAt(pos) == '-'
        pos += 1
      val digitsStart = pos
      while pos < s.length && pos - digitsStart < maxDigits && s.charAt(pos).isDigit do
        pos += 1
      digitsRead = pos - digitsStart
      if pos == digitsStart then
        pos = start
        None
      else
        val v = s.substring(digitsStart, pos).toInt
        Some(
          if neg then
            -v
          else
            v
        )

    def readName(names: IndexedSeq[String], abbreviated: Boolean): Option[Int] =
      val candidates = names
        .zipWithIndex
        .map { (n, i) =>
          (
            if abbreviated then
              n.take(3)
            else
              n
            ,
            i
          )
        }
      candidates.find((n, _) => s.regionMatches(true, pos, n, 0, n.length)) match
        case Some((n, i)) =>
          pos += n.length
          Some(i)
        case None =>
          None

    def set(v: Option[Int])(f: Int => Unit): Unit =
      v match
        case Some(x) =>
          f(x)
        case None =>
          ok = false

    var i = 0
    while ok && i < format.length do
      val c = format.charAt(i)
      if c == '%' && i + 1 < format.length then
        format.charAt(i + 1) match
          case 'Y' =>
            set(readInt(4, signed = true))(year = _)
          case 'y' =>
            set(readInt(2)) { y =>
              year =
                if y < 69 then
                  2000 + y
                else
                  1900 + y
            }
          case 'm' =>
            set(readInt(2))(month = _)
          case 'd' =>
            set(readInt(2))(day = _)
          case 'j' =>
            set(readInt(3))(doy = _)
          case 'H' =>
            set(readInt(2))(hour = _)
          case 'I' =>
            set(readInt(2))(hour = _)
          case 'M' =>
            set(readInt(2))(minute = _)
          case 'S' =>
            set(readInt(2))(second = _)
          case 'f' =>
            set(readInt(6)) { v =>
              micros = v.toLong * math.pow(10, 6 - digitsRead).toLong
            }
          case 'g' =>
            set(readInt(3)) { v =>
              micros = v.toLong * math.pow(10, 3 - digitsRead).toLong * 1000L
            }
          case 'b' | 'h' =>
            set(readName(monthNames, abbreviated = true))(m => month = m + 1)
          case 'B' =>
            set(readName(monthNames, abbreviated = false))(m => month = m + 1)
          case 'a' =>
            set(readName(dayNames, abbreviated = true))(_ => ())
          case 'A' =>
            set(readName(dayNames, abbreviated = false))(_ => ())
          case 'p' =>
            if s.regionMatches(true, pos, "AM", 0, 2) then
              pm = Some(false)
              pos += 2
            else if s.regionMatches(true, pos, "PM", 0, 2) then
              pm = Some(true)
              pos += 2
            else
              ok = false
          case 'z' =>
            if pos < s.length && (s.charAt(pos) == '+' || s.charAt(pos) == '-') then
              val sign =
                if s.charAt(pos) == '-' then
                  -1L
                else
                  1L
              pos += 1
              set(readInt(2)) { hh =>
                var mm = 0
                if pos < s.length && s.charAt(pos) == ':' then
                  pos += 1
                if pos < s.length && s.charAt(pos).isDigit then
                  set(readInt(2))(mm = _)
                offsetSec = Some(sign * (hh * 3600L + mm * 60L))
              }
            else
              ok = false
          case 'Z' =>
            val start = pos
            while pos < s.length && !s.charAt(pos).isWhitespace do
              pos += 1
            if pos == start then
              ok = false
            else
              zone = Some(s.substring(start, pos))
          case '%' =>
            if pos < s.length && s.charAt(pos) == '%' then
              pos += 1
            else
              ok = false
          case other =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Unsupported strptime format specifier: %${other}")
        end match
        i += 2
      else
        if pos < s.length && s.charAt(pos) == c then
          pos += 1
        else
          ok = false
        i += 1
      end if
    end while

    if !ok || pos != s.length then
      None
    else
      pm.foreach { isPm =>
        if hour < 1 || hour > 12 then
          ok = false
        else if isPm && hour != 12 then
          hour += 12
        else if !isPm && hour == 12 then
          hour = 0
      }
      val days: Option[Int] =
        if doy >= 0 then
          if doy < 1 ||
            doy >
              365 + (
                if ValueOps.isLeapYear(year) then
                  1
                else
                  0
              )
          then
            None
          else
            Some(ValueOps.daysFromCivil(year, 1, 1) + doy - 1)
        else if month < 1 || month > 12 || day < 1 || day > ValueOps.daysInMonth(year, month) then
          None
        else
          Some(ValueOps.daysFromCivil(year, month, day))
      if !ok || hour > 23 || minute > 59 || second > 59 then
        None
      else
        days.map { d =>
          val local =
            d.toLong * MicrosPerDay +
              (hour * 3600L + minute * 60L + second) * MicrosPerSecond + micros
          offsetSec match
            case Some(off) =>
              local - off * MicrosPerSecond
            case None =>
              zone match
                case Some(z) =>
                  TimeZones.toInstant(local, z)
                case None =>
                  local
        }
    end if

  end strptime

end DateTimeFunctions

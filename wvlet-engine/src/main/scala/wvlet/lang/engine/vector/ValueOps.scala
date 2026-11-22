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

import java.math.BigDecimal as JBigDecimal
import java.math.RoundingMode

/** Conversions between engine values, plus calendar arithmetic on epoch days and micros */
object ValueOps:

  def toBoolean(v: Any): Boolean =
    v match
      case b: Boolean =>
        b
      case s: String =>
        s.trim.toLowerCase match
          case "true" | "t" | "1" | "yes" =>
            true
          case "false" | "f" | "0" | "no" =>
            false
          case other =>
            throw castError(s, "boolean")
      case i: Int =>
        i != 0
      case l: Long =>
        l != 0
      case other =>
        throw castError(other, "boolean")

  def toInt(v: Any): Int =
    v match
      case i: Int =>
        i
      case l: Long =>
        if l < Int.MinValue || l > Int.MaxValue then
          throw StatusCode.INVALID_ARGUMENT.newException(s"Integer overflow: ${l}")
        l.toInt
      case other =>
        val l = toLong(other)
        if l < Int.MinValue || l > Int.MaxValue then
          throw StatusCode.INVALID_ARGUMENT.newException(s"Integer overflow: ${l}")
        l.toInt

  def toLong(v: Any): Long =
    v match
      case l: Long =>
        l
      case i: Int =>
        i.toLong
      case d: Double =>
        if d.isNaN || d.isInfinite then
          throw castError(d, "long")
        math.round(d)
      case b: JBigDecimal =>
        b.setScale(0, RoundingMode.HALF_UP).longValueExact()
      case b: Boolean =>
        if b then
          1L
        else
          0L
      case s: String =>
        val t = s.trim
        try
          t.toLong
        catch
          case _: NumberFormatException =>
            try
              toLong(JBigDecimal(t))
            catch
              case _: NumberFormatException =>
                throw castError(s, "long")
      case other =>
        throw castError(other, "long")

  def toDouble(v: Any): Double =
    v match
      case d: Double =>
        d
      case i: Int =>
        i.toDouble
      case l: Long =>
        l.toDouble
      case f: Float =>
        f.toDouble
      case b: JBigDecimal =>
        b.doubleValue()
      case s: String =>
        try
          s.trim.toDouble
        catch
          case _: NumberFormatException =>
            throw castError(s, "double")
      case b: Boolean =>
        if b then
          1.0
        else
          0.0
      case other =>
        throw castError(other, "double")

  def toBigDecimal(v: Any): JBigDecimal =
    v match
      case b: JBigDecimal =>
        b
      case i: Int =>
        JBigDecimal.valueOf(i.toLong)
      case l: Long =>
        JBigDecimal.valueOf(l)
      case d: Double =>
        JBigDecimal(java.lang.Double.toString(d))
      case s: String =>
        try
          JBigDecimal(s.trim)
        catch
          case _: NumberFormatException =>
            throw castError(s, "decimal")
      case b: BigDecimal =>
        b.bigDecimal
      case other =>
        throw castError(other, "decimal")

  def rescale(v: JBigDecimal, scale: Int): JBigDecimal =
    if v.scale() == scale then
      v
    else
      v.setScale(scale, RoundingMode.HALF_UP)

  def toStr(v: Any): String =
    v match
      case s: String =>
        s
      case d: Double =>
        formatDouble(d)
      case b: JBigDecimal =>
        b.toPlainString
      case other =>
        other.toString

  def formatDouble(d: Double): String =
    if d.isNaN then
      "nan"
    else if d.isInfinite then
      if d > 0 then
        "inf"
      else
        "-inf"
    else if d == math.rint(d) && math.abs(d) < 1e15 then
      s"${d.toLong}.0"
    else
      java.lang.Double.toString(d)

  /** Convert an engine value of type `from` into type `to` (SQL CAST semantics) */
  def cast(v: Any, from: PType, to: PType): Any =
    if v == null then
      null
    else if from == to then
      v
    else
      to match
        case PType.Bool =>
          toBoolean(v)
        case PType.Int32 =>
          from match
            case PType.Float64 | _: PType.Decimal =>
              toInt(toLong(v))
            case _ =>
              toInt(v)
        case PType.Int64 =>
          toLong(v)
        case PType.Float64 =>
          toDouble(v)
        case d: PType.Decimal =>
          rescale(toBigDecimal(v), d.scale)
        case PType.Str =>
          from match
            case PType.Date =>
              formatDate(v.asInstanceOf[Int])
            case PType.Timestamp =>
              formatTimestamp(v.asInstanceOf[Long])
            case _ =>
              toStr(v)
        case PType.Date =>
          from match
            case PType.Str =>
              parseDate(v.asInstanceOf[String])
            case PType.Timestamp =>
              Math.floorDiv(v.asInstanceOf[Long], MicrosPerDay).toInt
            case _ =>
              throw castError(v, "date")
        case PType.Timestamp =>
          from match
            case PType.Str =>
              parseTimestamp(v.asInstanceOf[String])
            case PType.Date =>
              v.asInstanceOf[Int].toLong * MicrosPerDay
            case _ =>
              throw castError(v, "timestamp")
        case _ =>
          v

  private def castError(v: Any, tpe: String) = StatusCode
    .INVALID_ARGUMENT
    .newException(s"Could not convert '${v}' to ${tpe}")

  // --- calendar ---------------------------------------------------------------------------

  final val MicrosPerSecond = 1000000L
  final val MicrosPerDay    = 86400L * MicrosPerSecond

  /** Days since 1970-01-01 of a proleptic Gregorian date */
  def daysFromCivil(y0: Int, m: Int, d: Int): Int =
    val y =
      if m <= 2 then
        y0 - 1
      else
        y0
    val era =
      (
        if y >= 0 then
          y
        else
          y - 399
      ) / 400
    val yoe = y - era * 400
    val mp  = (m + 9) % 12
    val doy = (153 * mp + 2) / 5 + d - 1
    val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
    era * 146097 + doe - 719468

  /** (year, month, day) of days since 1970-01-01 */
  def civilFromDays(days: Int): (Int, Int, Int) =
    val z   = days + 719468
    val era =
      (
        if z >= 0 then
          z
        else
          z - 146096
      ) / 146097
    val doe = z - era * 146097
    val yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
    val y   = yoe + era * 400
    val doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
    val mp  = (5 * doy + 2) / 153
    val d   = doy - (153 * mp + 2) / 5 + 1
    val m   =
      if mp < 10 then
        mp + 3
      else
        mp - 9
    (
      if m <= 2 then
        y + 1
      else
        y
      ,
      m,
      d
    )

  def isLeapYear(y: Int): Boolean = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

  def daysInMonth(y: Int, m: Int): Int =
    m match
      case 2 =>
        if isLeapYear(y) then
          29
        else
          28
      case 4 | 6 | 9 | 11 =>
        30
      case _ =>
        31

  /** Add months to a date, clamping the day to the end of the target month */
  def addMonths(days: Int, months: Int): Int =
    val (y, m, d) = civilFromDays(days)
    val total     = y * 12 + (m - 1) + months
    val ny        = Math.floorDiv(total, 12)
    val nm        = Math.floorMod(total, 12) + 1
    daysFromCivil(ny, nm, math.min(d, daysInMonth(ny, nm)))

  def addMonthsToTimestamp(micros: Long, months: Int): Long =
    val day  = Math.floorDiv(micros, MicrosPerDay)
    val rest = Math.floorMod(micros, MicrosPerDay)
    addMonths(day.toInt, months).toLong * MicrosPerDay + rest

  private def pad(v: Int, width: Int): String =
    val s = v.toString
    if s.length >= width then
      s
    else
      ("0" * (width - s.length)) + s

  def formatDate(days: Int): String =
    val (y, m, d) = civilFromDays(days)
    s"${pad(y, 4)}-${pad(m, 2)}-${pad(d, 2)}"

  def formatTimestamp(micros: Long): String =
    val day  = Math.floorDiv(micros, MicrosPerDay)
    val rest = Math.floorMod(micros, MicrosPerDay)
    val secs = rest / MicrosPerSecond
    val frac = rest   % MicrosPerSecond
    val hh   = (secs / 3600).toInt
    val mm   = ((secs % 3600) / 60).toInt
    val ss   = (secs  % 60).toInt
    val base = s"${formatDate(day.toInt)} ${pad(hh, 2)}:${pad(mm, 2)}:${pad(ss, 2)}"
    if frac == 0 then
      base
    else
      val f = pad(frac.toInt, 6).reverse.dropWhile(_ == '0').reverse
      s"${base}.${f}"

  def parseDate(s: String): Int =
    val t     = s.trim
    val parts = t.takeWhile(c => c != ' ' && c != 'T').split("-")
    if parts.length != 3 then
      throw castError(s, "date")
    try
      daysFromCivil(parts(0).toInt, parts(1).toInt, parts(2).toInt)
    catch
      case _: NumberFormatException =>
        throw castError(s, "date")

  def parseTimestamp(s: String): Long =
    val t        = s.trim
    val sepIndex = t.indexWhere(c => c == ' ' || c == 'T')
    if sepIndex < 0 then
      parseDate(t).toLong * MicrosPerDay
    else
      val days = parseDate(t.substring(0, sepIndex))
      var time = t.substring(sepIndex + 1).trim
      // Drop a trailing zone designator (UTC is assumed)
      time = time.stripSuffix("Z")
      val zoneIdx = time.indexWhere(c => c == '+' || (c == '-'))
      if zoneIdx > 0 then
        time = time.substring(0, zoneIdx)
      val hms = time.split(":")
      try
        val h = hms(0).toInt
        val m =
          if hms.length > 1 then
            hms(1).toInt
          else
            0
        val sec =
          if hms.length > 2 then
            hms(2)
          else
            "0"
        val (si, fracMicros) =
          sec.indexOf('.') match
            case -1 =>
              (sec.toInt, 0L)
            case i =>
              val f = (sec.substring(i + 1) + "000000").take(6)
              (sec.substring(0, i).toInt, f.toLong)
        days.toLong * MicrosPerDay + (h * 3600L + m * 60L + si) * MicrosPerSecond + fracMicros
      catch
        case _: NumberFormatException | _: ArrayIndexOutOfBoundsException =>
          throw castError(s, "timestamp")
    end if

  end parseTimestamp

  // --- ordering ---------------------------------------------------------------------------

  /** Compare two non-null engine values of the same physical type */
  def compare(a: Any, b: Any): Int =
    (a, b) match
      case (x: Int, y: Int) =>
        Integer.compare(x, y)
      case (x: Long, y: Long) =>
        java.lang.Long.compare(x, y)
      case (x: Double, y: Double) =>
        java.lang.Double.compare(x, y)
      case (x: String, y: String) =>
        x.compareTo(y)
      case (x: Boolean, y: Boolean) =>
        java.lang.Boolean.compare(x, y)
      case (x: JBigDecimal, y: JBigDecimal) =>
        x.compareTo(y)
      case (x: Int, y: Long) =>
        java.lang.Long.compare(x.toLong, y)
      case (x: Long, y: Int) =>
        java.lang.Long.compare(x, y.toLong)
      case (x, y) if isNumber(x) && isNumber(y) =>
        toBigDecimal(x).compareTo(toBigDecimal(y))
      case (x: Seq[?], y: Seq[?]) =>
        val it1 = x.iterator
        val it2 = y.iterator
        var c   = 0
        while c == 0 && it1.hasNext && it2.hasNext do
          val v1 = it1.next()
          val v2 = it2.next()
          c =
            if v1 == null && v2 == null then
              0
            else if v1 == null then
              1
            else if v2 == null then
              -1
            else
              compare(v1, v2)
        if c != 0 then
          c
        else
          java.lang.Boolean.compare(it1.hasNext, it2.hasNext)
      case _ =>
        a.toString.compareTo(b.toString)

  private def isNumber(x: Any): Boolean =
    x match
      case _: Int | _: Long | _: Double | _: JBigDecimal =>
        true
      case _ =>
        false

  /**
    * Normalize an engine value into the value shape `TableRows` rows carry (the same shapes the
    * JDBC result path produces): integers as Long, decimals / dates / timestamps as strings
    */
  def toExternal(v: Any, ptype: PType): Any =
    if v == null then
      null
    else
      ptype match
        case PType.Int32 =>
          v.asInstanceOf[Int].toLong
        case PType.Date =>
          formatDate(v.asInstanceOf[Int])
        case PType.Timestamp =>
          formatTimestamp(v.asInstanceOf[Long])
        case _: PType.Decimal =>
          v.asInstanceOf[JBigDecimal].toPlainString
        case _ =>
          externalNested(v)

  private def externalNested(v: Any): Any =
    v match
      case s: Seq[?] =>
        s.map(externalNested)
      case m: Map[?, ?] =>
        m.map((k, x) => externalNested(k) -> externalNested(x))
      case i: Int =>
        i.toLong
      case b: JBigDecimal =>
        b.toPlainString
      case other =>
        other

end ValueOps

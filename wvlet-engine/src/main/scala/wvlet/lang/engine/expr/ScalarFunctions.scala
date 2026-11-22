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
import wvlet.lang.engine.vector.*

import java.math.BigDecimal as JBigDecimal
import java.math.RoundingMode
import scala.collection.immutable.NumericRange

/**
  * Scalar functions by (DuckDB-compatible) name. Every function receives bound argument expressions
  * and returns a bound expression, or None when the name is unknown
  */
object ScalarFunctions:

  private type Builder = Seq[VExpr] => VExpr

  def lookup(name: String, args: Seq[VExpr]): Option[VExpr] = registry
    .get(name.toLowerCase)
    .map(_(args))

  def isDefined(name: String): Boolean = registry.contains(name.toLowerCase)

  private def arity(name: String, args: Seq[VExpr], n: Int*): Unit =
    if !n.contains(args.size) then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"${name} expects ${n.mkString(" or ")} arguments, but got ${args.size}")

  private def str(e: VExpr): VExpr =
    if e.ptype == PType.Str then
      e
    else
      CastExpr(e, PType.Str)

  private def toLong(e: VExpr): VExpr =
    if e.ptype == PType.Int64 then
      e
    else
      CastExpr(e, PType.Int64)

  private def toDouble(e: VExpr): VExpr =
    if e.ptype == PType.Float64 then
      e
    else
      CastExpr(e, PType.Float64)

  private def fn(name: String, args: Seq[VExpr], out: PType)(f: Array[Any] => Any): VExpr = RowFn(
    name,
    args,
    out,
    f
  )

  private def nullSafeFn(name: String, args: Seq[VExpr], out: PType)(f: Array[Any] => Any): VExpr =
    RowFn(name, args, out, f, nullSafe = true)

  private def commonType(args: Seq[VExpr]): PType = args
    .map(_.ptype)
    .reduceOption(PType.common)
    .getOrElse(PType.Null)

  private def utf8Substring(s: String, start: Long, len: Option[Long]): String =
    // SQL substring: 1-origin; start <= 0 counts the positions before the string
    val cps   = s.codePointCount(0, s.length)
    val begin =
      if start > 0 then
        start - 1
      else if start == 0 then
        -1
      else
        cps + start
    val end =
      len match
        case Some(l) =>
          if l < 0 then
            throw StatusCode.INVALID_ARGUMENT.newException(s"Negative substring length: ${l}")
          begin + l
        case None =>
          cps.toLong
    val b = math.max(0L, begin)
    val e = math.min(cps.toLong, end)
    if b >= e then
      ""
    else
      val bi = s.offsetByCodePoints(0, b.toInt)
      val ei = s.offsetByCodePoints(bi, (e - b).toInt)
      s.substring(bi, ei)

  private def roundDecimal(v: JBigDecimal, digits: Int): JBigDecimal = v.setScale(
    digits,
    RoundingMode.HALF_UP
  )

  private def datePart(part: String, ptype: PType, v: Any): Long =
    val micros: Long =
      ptype match
        case PType.Date =>
          v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay
        case PType.Timestamp =>
          v.asInstanceOf[Long]
        case PType.Str =>
          ValueOps.parseTimestamp(v.asInstanceOf[String])
        case _ =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"Cannot extract ${part} from ${ptype}")
    val day      = Math.floorDiv(micros, ValueOps.MicrosPerDay).toInt
    val rest     = Math.floorMod(micros, ValueOps.MicrosPerDay)
    lazy val ymd = ValueOps.civilFromDays(day)
    part.toLowerCase match
      case "year" | "years" | "y" =>
        ymd._1
      case "month" | "months" | "mon" =>
        ymd._2
      case "day" | "days" | "d" | "dayofmonth" =>
        ymd._3
      case "quarter" =>
        (ymd._2 - 1) / 3 + 1
      case "hour" | "hours" | "h" =>
        rest / (3600L * ValueOps.MicrosPerSecond)
      case "minute" | "minutes" | "m" =>
        (rest / (60L * ValueOps.MicrosPerSecond)) % 60
      case "second" | "seconds" | "s" =>
        (rest / ValueOps.MicrosPerSecond) % 60
      case "millisecond" | "milliseconds" | "ms" =>
        (rest / 1000L) % 60000L
      case "microsecond" | "microseconds" | "us" =>
        rest % (60L * ValueOps.MicrosPerSecond)
      case "dow" | "dayofweek" =>
        // Sunday = 0
        Math.floorMod(day + 4, 7).toLong
      case "isodow" =>
        val d = Math.floorMod(day + 3, 7) + 1
        d.toLong
      case "doy" | "dayofyear" =>
        day - ValueOps.daysFromCivil(ymd._1, 1, 1) + 1L
      case "week" | "weeks" | "w" | "weekofyear" =>
        isoWeek(day)
      case "epoch" =>
        Math.floorDiv(micros, ValueOps.MicrosPerSecond)
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported date part: ${other}")
    end match

  end datePart

  private def isoWeek(day: Int): Long =
    // Thursday of the same ISO week decides the year
    val isoDow    = Math.floorMod(day + 3, 7) + 1
    val thursday  = day - isoDow + 4
    val (y, _, _) = ValueOps.civilFromDays(thursday)
    val jan1      = ValueOps.daysFromCivil(y, 1, 1)
    ((thursday - jan1) / 7 + 1).toLong

  def dateTrunc(part: String, ptype: PType, v: Any): Any =
    val micros: Long =
      ptype match
        case PType.Date =>
          v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay
        case _ =>
          v.asInstanceOf[Long]
    val day                       = Math.floorDiv(micros, ValueOps.MicrosPerDay).toInt
    val (y, m, _)                 = ValueOps.civilFromDays(day)
    val truncatedDay: Option[Int] =
      part.toLowerCase match
        case "year" | "years" =>
          Some(ValueOps.daysFromCivil(y, 1, 1))
        case "quarter" =>
          Some(ValueOps.daysFromCivil(y, ((m - 1) / 3) * 3 + 1, 1))
        case "month" | "months" =>
          Some(ValueOps.daysFromCivil(y, m, 1))
        case "week" | "weeks" =>
          Some(day - (Math.floorMod(day + 3, 7)))
        case "day" | "days" =>
          Some(day)
        case _ =>
          None
    truncatedDay match
      case Some(d) =>
        if ptype == PType.Date then
          d
        else
          d.toLong * ValueOps.MicrosPerDay
      case None =>
        val unit =
          part.toLowerCase match
            case "hour" | "hours" =>
              3600L * ValueOps.MicrosPerSecond
            case "minute" | "minutes" =>
              60L * ValueOps.MicrosPerSecond
            case "second" | "seconds" =>
              ValueOps.MicrosPerSecond
            case other =>
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(s"Unsupported date_trunc unit: ${other}")
        Math.floorDiv(micros, unit) * unit

  end dateTrunc

  /** Add an interval amount of the given unit to a date or timestamp */
  def addInterval(ptype: PType, v: Any, unit: String, amount: Long): Any =
    unit.toLowerCase.stripSuffix("s") match
      case "year" =>
        addMonthsTo(ptype, v, (amount * 12).toInt)
      case "quarter" =>
        addMonthsTo(ptype, v, (amount * 3).toInt)
      case "month" =>
        addMonthsTo(ptype, v, amount.toInt)
      case "week" =>
        addMicros(ptype, v, amount * 7 * ValueOps.MicrosPerDay)
      case "day" =>
        addMicros(ptype, v, amount * ValueOps.MicrosPerDay)
      case "hour" =>
        addMicros(ptype, v, amount * 3600L * ValueOps.MicrosPerSecond)
      case "minute" =>
        addMicros(ptype, v, amount * 60L * ValueOps.MicrosPerSecond)
      case "second" =>
        addMicros(ptype, v, amount * ValueOps.MicrosPerSecond)
      case "millisecond" =>
        addMicros(ptype, v, amount * 1000L)
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported interval unit: ${unit}")

  private def addMonthsTo(ptype: PType, v: Any, months: Int): Any =
    ptype match
      case PType.Date =>
        ValueOps.addMonths(v.asInstanceOf[Int], months)
      case _ =>
        ValueOps.addMonthsToTimestamp(v.asInstanceOf[Long], months)

  private def addMicros(ptype: PType, v: Any, micros: Long): Any =
    ptype match
      case PType.Date if micros % ValueOps.MicrosPerDay == 0 =>
        v.asInstanceOf[Int] + (micros / ValueOps.MicrosPerDay).toInt
      case PType.Date =>
        v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay + micros
      case _ =>
        v.asInstanceOf[Long] + micros

  private def dateDiff(unit: String, from: Long, to: Long): Long =
    unit.toLowerCase.stripSuffix("s") match
      case "day" =>
        Math.floorDiv(to, ValueOps.MicrosPerDay) - Math.floorDiv(from, ValueOps.MicrosPerDay)
      case "week" =>
        (Math.floorDiv(to, ValueOps.MicrosPerDay) - Math.floorDiv(from, ValueOps.MicrosPerDay)) / 7
      case "hour" =>
        Math.floorDiv(to, 3600L * ValueOps.MicrosPerSecond) -
          Math.floorDiv(from, 3600L * ValueOps.MicrosPerSecond)
      case "minute" =>
        Math.floorDiv(to, 60L * ValueOps.MicrosPerSecond) -
          Math.floorDiv(from, 60L * ValueOps.MicrosPerSecond)
      case "second" =>
        Math.floorDiv(to, ValueOps.MicrosPerSecond) - Math.floorDiv(from, ValueOps.MicrosPerSecond)
      case "month" | "year" | "quarter" =>
        val (y1, m1, _) = ValueOps.civilFromDays(Math.floorDiv(from, ValueOps.MicrosPerDay).toInt)
        val (y2, m2, _) = ValueOps.civilFromDays(Math.floorDiv(to, ValueOps.MicrosPerDay).toInt)
        val months      = (y2 - y1) * 12L + (m2 - m1)
        unit.toLowerCase.stripSuffix("s") match
          case "month" =>
            months
          case "quarter" =>
            ((y2 * 12L + m2 - 1) / 3) - ((y1 * 12L + m1 - 1) / 3)
          case _ =>
            (y2 - y1).toLong
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported date_diff unit: ${unit}")

  private def toMicros(ptype: PType, v: Any): Long =
    ptype match
      case PType.Date =>
        v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay
      case PType.Timestamp =>
        v.asInstanceOf[Long]
      case _ =>
        ValueOps.parseTimestamp(ValueOps.toStr(v))

  private def strftime(micros: Long, format: String): String =
    val day         = Math.floorDiv(micros, ValueOps.MicrosPerDay).toInt
    val rest        = Math.floorMod(micros, ValueOps.MicrosPerDay)
    val (y, m, d)   = ValueOps.civilFromDays(day)
    val secs        = rest / ValueOps.MicrosPerSecond
    def p2(x: Long) =
      if x < 10 then
        s"0${x}"
      else
        x.toString
    val sb = StringBuilder()
    var i  = 0
    while i < format.length do
      val c = format.charAt(i)
      if c == '%' && i + 1 < format.length then
        format.charAt(i + 1) match
          case 'Y' =>
            sb.append(y)
          case 'm' =>
            sb.append(p2(m))
          case 'd' =>
            sb.append(p2(d))
          case 'H' =>
            sb.append(p2(secs / 3600))
          case 'M' =>
            sb.append(p2((secs % 3600) / 60))
          case 'S' =>
            sb.append(p2(secs % 60))
          case 'y' =>
            sb.append(p2(y % 100))
          case 'j' =>
            val doy = day - ValueOps.daysFromCivil(y, 1, 1) + 1
            sb.append(f"${doy}%03d")
          case '%' =>
            sb.append('%')
          case other =>
            sb.append('%').append(other)
        i += 2
      else
        sb.append(c)
        i += 1
    sb.toString

  end strftime

  // String.codePoints() returns a java.util.stream.IntStream, which Scala.js doesn't provide
  private def codePointArray(s: String): Array[Int] =
    val out = Array.newBuilder[Int]
    var i   = 0
    while i < s.length do
      val cp = s.codePointAt(i)
      out += cp
      i += Character.charCount(cp)
    out.result()

  /** Levenshtein distance over code points */
  private def levenshtein(x: String, y: String): Int =
    val a    = codePointArray(x)
    val b    = codePointArray(y)
    var prev = Array.tabulate(b.length + 1)(identity)
    var cur  = new Array[Int](b.length + 1)
    var i    = 1
    while i <= a.length do
      cur(0) = i
      var j = 1
      while j <= b.length do
        val cost =
          if a(i - 1) == b(j - 1) then
            0
          else
            1
        cur(j) = math.min(math.min(cur(j - 1) + 1, prev(j) + 1), prev(j - 1) + cost)
        j += 1
      val t = prev
      prev = cur
      cur = t
      i += 1
    prev(b.length)

  // DuckDB's hash functions (src/common/types/hash.cpp)
  private def murmurHash64(v: Long): Long =
    var x = v
    x ^= x >>> 32
    x *= 0xd6e8feb86659fd93L
    x ^= x >>> 32
    x *= 0xd6e8feb86659fd93L
    x ^= x >>> 32
    x

  private def hashBytes(bytes: Array[Byte]): Long =
    var h   = 0xe17a1465L ^ (bytes.length.toLong * 0xc6a4a7935bd1e995L)
    var pos = 0
    while pos < bytes.length do
      var block = 0L
      var k     = 0
      while k < 8 && pos + k < bytes.length do
        block |= (bytes(pos + k) & 0xffL) << (8 * k)
        k += 1
      h ^= block
      h *= 0xd6e8feb86659fd93L
      pos += 8
    murmurHash64(h)

  private def duckdbHash(v: Any): Long =
    v match
      case s: String =>
        hashBytes(s.getBytes("UTF-8"))
      case i: Int =>
        murmurHash64(i.toLong)
      case l: Long =>
        murmurHash64(l)
      case b: Boolean =>
        murmurHash64(
          if b then
            1L
          else
            0L
        )
      case other =>
        hashBytes(other.toString.getBytes("UTF-8"))

  private def combineHash(a: Long, b: Long): Long = (a * 0xbf58476d1ce4e5b9L) ^ b

  private def regex(p: String): java.util.regex.Pattern = java.util.regex.Pattern.compile(p)

  private val regexCache = scala.collection.mutable.HashMap.empty[String, java.util.regex.Pattern]
  private def cachedRegex(p: String): java.util.regex.Pattern = regexCache.synchronized {
    regexCache.getOrElseUpdate(p, regex(p))
  }

  private def numericRound(name: String, args: Seq[VExpr])(
      f: Double => Double,
      big: (JBigDecimal, Int) => JBigDecimal
  ): VExpr =
    val x      = args.head
    val digits =
      args.lift(1) match
        case Some(Const(v, _)) if v != null =>
          ValueOps.toInt(v)
        case Some(other) =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"${name} requires a constant number of digits")
        case None =>
          0
    x.ptype match
      case d: PType.Decimal =>
        val outType = PType.decimal(d.precision, math.max(0, math.min(d.scale, digits)))
        fn(name, Seq(x), outType)(a => big(a(0).asInstanceOf[JBigDecimal], digits))
      case PType.Int32 | PType.Int64 =>
        x
      case _ =>
        val m = math.pow(10, digits)
        fn(name, Seq(toDouble(x)), PType.Float64)(a => f(a(0).asInstanceOf[Double] * m) / m)

  private val registry: Map[String, Builder] =
    val m                                     = Map.newBuilder[String, Builder]
    def reg(names: String*)(b: Builder): Unit = names.foreach(n => m += n -> b)

    // ---- strings ----
    reg("length", "char_length", "character_length", "len", "strlen") { args =>
      arity("length", args, 1)
      args.head.ptype match
        case PType.AnyT =>
          fn("length", args, PType.Int64) { a =>
            a(0) match
              case s: Seq[?] =>
                s.size.toLong
              case s: String =>
                s.codePointCount(0, s.length).toLong
              case other =>
                other.toString.length.toLong
          }
        case _ =>
          fn("length", Seq(str(args.head)), PType.Int64)(a =>
            a(0).asInstanceOf[String].codePointCount(0, a(0).asInstanceOf[String].length).toLong
          )
    }
    reg("upper", "ucase") { args =>
      fn("upper", Seq(str(args.head)), PType.Str)(a => a(0).asInstanceOf[String].toUpperCase)
    }
    reg("lower", "lcase") { args =>
      fn("lower", Seq(str(args.head)), PType.Str)(a => a(0).asInstanceOf[String].toLowerCase)
    }
    reg("trim") { args =>
      if args.size == 1 then
        fn("trim", Seq(str(args.head)), PType.Str)(a => a(0).asInstanceOf[String].trim)
      else
        fn("trim", Seq(str(args(0)), str(args(1))), PType.Str) { a =>
          val chars = a(1).asInstanceOf[String].toSet
          a(0)
            .asInstanceOf[String]
            .dropWhile(chars.contains)
            .reverse
            .dropWhile(chars.contains)
            .reverse
        }
    }
    reg("ltrim") { args =>
      fn("ltrim", Seq(str(args.head)), PType.Str)(a =>
        a(0).asInstanceOf[String].dropWhile(_.isWhitespace)
      )
    }
    reg("rtrim") { args =>
      fn("rtrim", Seq(str(args.head)), PType.Str)(a =>
        a(0).asInstanceOf[String].reverse.dropWhile(_.isWhitespace).reverse
      )
    }
    reg("reverse") { args =>
      fn("reverse", Seq(str(args.head)), PType.Str)(a => a(0).asInstanceOf[String].reverse)
    }
    reg("substring", "substr") { args =>
      arity("substring", args, 2, 3)
      val in = str(args(0)) +: args.tail.map(toLong)
      fn("substring", in, PType.Str) { a =>
        utf8Substring(
          a(0).asInstanceOf[String],
          a(1).asInstanceOf[Long],
          if a.length > 2 then
            Some(a(2).asInstanceOf[Long])
          else
            None
        )
      }
    }
    reg("left") { args =>
      fn("left", Seq(str(args(0)), toLong(args(1))), PType.Str)(a =>
        utf8Substring(a(0).asInstanceOf[String], 1, Some(a(1).asInstanceOf[Long]))
      )
    }
    reg("right") { args =>
      fn("right", Seq(str(args(0)), toLong(args(1))), PType.Str) { a =>
        val s = a(0).asInstanceOf[String]
        val n = a(1).asInstanceOf[Long].toInt
        s.substring(math.max(0, s.length - n))
      }
    }
    reg("concat") { args =>
      // DuckDB's concat skips NULL arguments
      nullSafeFn("concat", args.map(str), PType.Str)(a =>
        a.iterator.filter(_ != null).map(_.toString).mkString
      )
    }
    reg("||", "__wv_concat") { args =>
      fn("||", args.map(str), PType.Str)(a => a.mkString)
    }
    reg("concat_ws") { args =>
      nullSafeFn("concat_ws", args.map(str), PType.Str) { a =>
        if a(0) == null then
          null
        else
          a.iterator.drop(1).filter(_ != null).map(_.toString).mkString(a(0).toString)
      }
    }
    reg("replace") { args =>
      fn("replace", args.map(str), PType.Str)(a =>
        a(0).asInstanceOf[String].replace(a(1).asInstanceOf[String], a(2).asInstanceOf[String])
      )
    }
    reg("lpad") { args =>
      fn("lpad", Seq(str(args(0)), toLong(args(1)), str(args(2))), PType.Str) { a =>
        val s   = a(0).asInstanceOf[String]
        val n   = a(1).asInstanceOf[Long].toInt
        val pad = a(2).asInstanceOf[String]
        if s.length >= n then
          s.take(n)
        else if pad.isEmpty then
          s
        else
          val sb = StringBuilder()
          while sb.length + s.length < n do
            sb.append(pad)
          sb.toString.take(n - s.length) + s
      }
    }
    reg("rpad") { args =>
      fn("rpad", Seq(str(args(0)), toLong(args(1)), str(args(2))), PType.Str) { a =>
        val s   = a(0).asInstanceOf[String]
        val n   = a(1).asInstanceOf[Long].toInt
        val pad = a(2).asInstanceOf[String]
        if s.length >= n then
          s.take(n)
        else if pad.isEmpty then
          s
        else
          val sb = StringBuilder(s)
          while sb.length < n do
            sb.append(pad)
          sb.toString.take(n)
      }
    }
    reg("strpos", "instr", "position") { args =>
      fn("strpos", args.map(str), PType.Int64) { a =>
        val s = a(0).asInstanceOf[String]
        val i = s.indexOf(a(1).asInstanceOf[String])
        if i < 0 then
          0L
        else
          s.codePointCount(0, i) + 1L
      }
    }
    reg("starts_with", "prefix") { args =>
      fn("starts_with", args.map(str), PType.Bool)(a =>
        a(0).asInstanceOf[String].startsWith(a(1).asInstanceOf[String])
      )
    }
    reg("ends_with", "suffix") { args =>
      fn("ends_with", args.map(str), PType.Bool)(a =>
        a(0).asInstanceOf[String].endsWith(a(1).asInstanceOf[String])
      )
    }
    reg("contains") { args =>
      if args.head.ptype == PType.AnyT then
        // contains(list, x)
        fn("contains", args, PType.Bool) { a =>
          a(0) match
            case s: Seq[?] =>
              s.exists(x => x != null && ValueOps.compare(x, a(1)) == 0)
            case other =>
              ValueOps.toStr(other).contains(ValueOps.toStr(a(1)))
        }
      else
        fn("contains", args.map(str), PType.Bool)(a =>
          a(0).asInstanceOf[String].contains(a(1).asInstanceOf[String])
        )
    }
    reg("repeat") { args =>
      fn("repeat", Seq(str(args(0)), toLong(args(1))), PType.Str)(a =>
        a(0).asInstanceOf[String] * a(1).asInstanceOf[Long].toInt
      )
    }
    reg("split", "string_split", "str_split") { args =>
      fn("split", args.map(str), PType.AnyT)(a =>
        a(0)
          .asInstanceOf[String]
          .split(java.util.regex.Pattern.quote(a(1).asInstanceOf[String]), -1)
          .toSeq
      )
    }
    reg("split_part") { args =>
      fn("split_part", Seq(str(args(0)), str(args(1)), toLong(args(2))), PType.Str) { a =>
        val parts = a(0)
          .asInstanceOf[String]
          .split(java.util.regex.Pattern.quote(a(1).asInstanceOf[String]), -1)
        val i = a(2).asInstanceOf[Long].toInt
        if i >= 1 && i <= parts.length then
          parts(i - 1)
        else
          ""
      }
    }
    reg("regexp_matches", "regexp_like") { args =>
      fn("regexp_matches", args.take(2).map(str), PType.Bool)(a =>
        cachedRegex(a(1).asInstanceOf[String]).matcher(a(0).asInstanceOf[String]).find()
      )
    }
    reg("regexp_replace") { args =>
      val global =
        args.lift(3) match
          case Some(Const(v, _)) if v != null =>
            v.toString.contains("g")
          case _ =>
            false
      fn("regexp_replace", args.take(3).map(str), PType.Str) { a =>
        val m = cachedRegex(a(1).asInstanceOf[String]).matcher(a(0).asInstanceOf[String])
        val r = a(2).asInstanceOf[String].replaceAll("\\\\(\\d)", "\\$$1")
        if global then
          m.replaceAll(r)
        else
          m.replaceFirst(r)
      }
    }
    reg("regexp_extract") { args =>
      val in = Seq(str(args(0)), str(args(1))) ++ args.drop(2).take(1).map(toLong)
      fn("regexp_extract", in, PType.Str) { a =>
        val m   = cachedRegex(a(1).asInstanceOf[String]).matcher(a(0).asInstanceOf[String])
        val grp =
          if a.length > 2 then
            a(2).asInstanceOf[Long].toInt
          else
            0
        if m.find() then
          val g = m.group(grp)
          if g == null then
            ""
          else
            g
        else
          ""
      }
    }
    reg("md5") { args =>
      fn("md5", Seq(str(args.head)), PType.Str)(a => MD5.hex(a(0).asInstanceOf[String]))
    }
    reg("ascii") { args =>
      fn("ascii", Seq(str(args.head)), PType.Int64)(a =>
        if a(0).asInstanceOf[String].isEmpty then
          0L
        else
          a(0).asInstanceOf[String].codePointAt(0).toLong
      )
    }
    reg("chr") { args =>
      fn("chr", Seq(toLong(args.head)), PType.Str)(a =>
        String(Character.toChars(a(0).asInstanceOf[Long].toInt))
      )
    }

    // ---- numbers ----
    reg("abs") { args =>
      val x = args.head
      fn("abs", args, x.ptype) { a =>
        a(0) match
          case i: Int =>
            math.abs(i)
          case l: Long =>
            math.abs(l)
          case d: Double =>
            math.abs(d)
          case b: JBigDecimal =>
            b.abs()
          case other =>
            other
      }
    }
    reg("round") { args =>
      numericRound("round", args)(
        d =>
          if d >= 0 then
            math.floor(d + 0.5)
          else
            math.ceil(d - 0.5)
        ,
        (b, s) => roundDecimal(b, s)
      )
    }
    reg("floor") { args =>
      args.head.ptype match
        case d: PType.Decimal =>
          fn("floor", args, PType.decimal(d.precision, 0))(a =>
            a(0).asInstanceOf[JBigDecimal].setScale(0, RoundingMode.FLOOR)
          )
        case PType.Int32 | PType.Int64 =>
          args.head
        case _ =>
          fn("floor", Seq(toDouble(args.head)), PType.Float64)(a =>
            math.floor(a(0).asInstanceOf[Double])
          )
    }
    reg("ceil", "ceiling") { args =>
      args.head.ptype match
        case d: PType.Decimal =>
          fn("ceil", args, PType.decimal(d.precision, 0))(a =>
            a(0).asInstanceOf[JBigDecimal].setScale(0, RoundingMode.CEILING)
          )
        case PType.Int32 | PType.Int64 =>
          args.head
        case _ =>
          fn("ceil", Seq(toDouble(args.head)), PType.Float64)(a =>
            math.ceil(a(0).asInstanceOf[Double])
          )
    }
    reg("trunc") { args =>
      args.head.ptype match
        case d: PType.Decimal =>
          fn("trunc", args.take(1), PType.decimal(d.precision, 0))(a =>
            a(0).asInstanceOf[JBigDecimal].setScale(0, RoundingMode.DOWN)
          )
        case PType.Int32 | PType.Int64 =>
          args.head
        case _ =>
          fn("trunc", Seq(toDouble(args.head)), PType.Float64)(a =>
            val d = a(0).asInstanceOf[Double];
            if d < 0 then
              math.ceil(d)
            else
              math.floor(d)
          )
    }
    def dbl(name: String)(f: Double => Double): Unit =
      reg(name) { args =>
        fn(name, Seq(toDouble(args.head)), PType.Float64)(a => f(a(0).asInstanceOf[Double]))
      }
    dbl("sqrt")(math.sqrt)
    dbl("cbrt")(math.cbrt)
    dbl("exp")(math.exp)
    dbl("ln")(math.log)
    dbl("log10")(math.log10)
    dbl("log2")(d => math.log(d) / math.log(2))
    dbl("sin")(math.sin)
    dbl("cos")(math.cos)
    dbl("tan")(math.tan)
    dbl("asin")(math.asin)
    dbl("acos")(math.acos)
    dbl("atan")(math.atan)
    dbl("degrees")(math.toDegrees)
    dbl("radians")(math.toRadians)
    reg("log") { args =>
      if args.size == 1 then
        fn("log", Seq(toDouble(args.head)), PType.Float64)(a =>
          math.log10(a(0).asInstanceOf[Double])
        )
      else
        fn("log", args.map(toDouble), PType.Float64)(a =>
          math.log(a(1).asInstanceOf[Double]) / math.log(a(0).asInstanceOf[Double])
        )
    }
    reg("power", "pow") { args =>
      fn("power", args.map(toDouble), PType.Float64)(a =>
        math.pow(a(0).asInstanceOf[Double], a(1).asInstanceOf[Double])
      )
    }
    reg("atan2") { args =>
      fn("atan2", args.map(toDouble), PType.Float64)(a =>
        math.atan2(a(0).asInstanceOf[Double], a(1).asInstanceOf[Double])
      )
    }
    reg("pi") { _ =>
      Const(math.Pi, PType.Float64)
    }
    reg("sign") { args =>
      fn("sign", Seq(toDouble(args.head)), PType.Int64)(a =>
        math.signum(a(0).asInstanceOf[Double]).toLong
      )
    }
    reg("mod") { args =>
      val t = commonType(args)
      Arith(
        ArithOp.Mod,
        args(0),
        args(1),
        if t.isIntegral then
          PType.Int64
        else
          t
      )
    }
    reg("random") { _ =>
      RowFn("random", Nil, PType.Float64, _ => scala.util.Random.nextDouble(), nullSafe = true)
    }
    reg("greatest") { args =>
      val t = commonType(args)
      nullSafeFn(
        "greatest",
        args.map(a =>
          if a.ptype == t then
            a
          else
            CastExpr(a, t)
        ),
        t
      ) { a =>
        a.iterator
          .filter(_ != null)
          .reduceOption((x, y) =>
            if ValueOps.compare(x, y) >= 0 then
              x
            else
              y
          )
          .orNull
      }
    }
    reg("least") { args =>
      val t = commonType(args)
      nullSafeFn(
        "least",
        args.map(a =>
          if a.ptype == t then
            a
          else
            CastExpr(a, t)
        ),
        t
      ) { a =>
        a.iterator
          .filter(_ != null)
          .reduceOption((x, y) =>
            if ValueOps.compare(x, y) <= 0 then
              x
            else
              y
          )
          .orNull
      }
    }

    // ---- conditionals ----
    reg("coalesce", "ifnull") { args =>
      Coalesce(args, commonType(args))
    }
    reg("nullif") { args =>
      val t = PType.common(args(0).ptype, args(1).ptype)
      nullSafeFn("nullif", Seq(CastExpr(args(0), t), CastExpr(args(1), t)), args(0).ptype) { a =>
        if a(0) != null && a(1) != null && ValueOps.compare(a(0), a(1)) == 0 then
          null
        else
          a(0)
      }
    }
    reg("if", "iff") { args =>
      val t = PType.common(args(1).ptype, args(2).ptype)
      CaseWhen(Seq(args(0) -> args(1)), Some(args(2)), t)
    }
    reg("typeof") { args =>
      Const(args.head.ptype.name, PType.Str)
    }

    // ---- dates ----
    reg("date_part", "datepart") { args =>
      val part =
        args.head match
          case Const(v, _) =>
            v.toString
          case _ =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException("date_part requires a constant part name")
      val t = args(1).ptype
      fn("date_part", Seq(args(1)), PType.Int64)(a => datePart(part, t, a(0)))
    }
    def part(name: String): Unit =
      reg(name) { args =>
        val t = args.head.ptype
        fn(name, args, PType.Int64)(a => datePart(name, t, a(0)))
      }
    Seq(
      "year",
      "month",
      "day",
      "hour",
      "minute",
      "second",
      "quarter",
      "dayofweek",
      "dayofyear",
      "week",
      "dayofmonth",
      "isodow",
      "weekofyear"
    ).foreach(part)
    reg("epoch") { args =>
      val t = args.head.ptype
      fn("epoch", args, PType.Int64)(a => datePart("epoch", t, a(0)))
    }
    reg("date_trunc", "datetrunc") { args =>
      val unit =
        args.head match
          case Const(v, _) =>
            v.toString
          case _ =>
            throw StatusCode.INVALID_ARGUMENT.newException("date_trunc requires a constant unit")
      val t =
        args(1).ptype match
          case PType.Str =>
            PType.Timestamp
          case other =>
            other
      val in =
        if args(1).ptype == PType.Str then
          CastExpr(args(1), PType.Timestamp)
        else
          args(1)
      // DuckDB returns a timestamp for date inputs truncated to a sub-day unit
      fn("date_trunc", Seq(in), t)(a => dateTrunc(unit, t, a(0)))
    }
    reg("date_diff", "datediff") { args =>
      val unit =
        args.head match
          case Const(v, _) =>
            v.toString
          case _ =>
            throw StatusCode.INVALID_ARGUMENT.newException("date_diff requires a constant unit")
      val t1 = args(1).ptype
      val t2 = args(2).ptype
      fn("date_diff", Seq(args(1), args(2)), PType.Int64)(a =>
        dateDiff(unit, toMicros(t1, a(0)), toMicros(t2, a(1)))
      )
    }
    reg("strftime") { args =>
      val t = args.head.ptype
      fn("strftime", Seq(args(0), str(args(1))), PType.Str)(a =>
        strftime(toMicros(t, a(0)), a(1).asInstanceOf[String])
      )
    }
    reg("make_date") { args =>
      fn("make_date", args.map(toLong), PType.Date)(a =>
        ValueOps.daysFromCivil(
          a(0).asInstanceOf[Long].toInt,
          a(1).asInstanceOf[Long].toInt,
          a(2).asInstanceOf[Long].toInt
        )
      )
    }
    reg("last_day") { args =>
      val t = args.head.ptype
      fn("last_day", args, PType.Date) { a =>
        val d         = Math.floorDiv(toMicros(t, a(0)), ValueOps.MicrosPerDay).toInt
        val (y, m, _) = ValueOps.civilFromDays(d)
        ValueOps.daysFromCivil(y, m, ValueOps.daysInMonth(y, m))
      }
    }
    reg("to_timestamp") { args =>
      fn("to_timestamp", Seq(toDouble(args.head)), PType.Timestamp)(a =>
        (a(0).asInstanceOf[Double] * ValueOps.MicrosPerSecond).toLong
      )
    }
    reg("epoch_ms") { args =>
      fn("epoch_ms", Seq(toLong(args.head)), PType.Timestamp)(a => a(0).asInstanceOf[Long] * 1000L)
    }

    // ---- lists ----
    reg("list_value", "array_value", "list_pack") { args =>
      nullSafeFn("list_value", args, PType.AnyT)(a => a.toSeq)
    }
    reg("array_length", "len", "list_length", "cardinality") { args =>
      fn("array_length", args.take(1), PType.Int64) { a =>
        a(0) match
          case s: Seq[?] =>
            s.size.toLong
          case m: Map[?, ?] =>
            m.size.toLong
          case s: String =>
            s.length.toLong
          case other =>
            throw StatusCode.INVALID_ARGUMENT.newException(s"Not a list: ${other}")
      }
    }
    reg("list_contains", "array_contains", "list_has", "array_has") { args =>
      fn("list_contains", args, PType.Bool) { a =>
        a(0).asInstanceOf[Seq[Any]].exists(x => x != null && ValueOps.compare(x, a(1)) == 0)
      }
    }
    reg("list_extract", "array_extract", "list_element") { args =>
      fn("list_extract", Seq(args(0), toLong(args(1))), PType.AnyT) { a =>
        a(0) match
          case s: Seq[?] =>
            val i = a(1).asInstanceOf[Long].toInt
            if i >= 1 && i <= s.size then
              s(i - 1)
            else
              null
          case s: String =>
            val i = a(1).asInstanceOf[Long].toInt
            if i >= 1 && i <= s.length then
              s.substring(i - 1, i)
            else
              ""
          case _ =>
            null
      }
    }
    reg("array_to_string", "list_aggr_string", "array_join") { args =>
      fn("array_to_string", Seq(args(0), str(args(1))), PType.Str)(a =>
        a(0)
          .asInstanceOf[Seq[Any]]
          .filter(_ != null)
          .map(ValueOps.toStr)
          .mkString(a(1).asInstanceOf[String])
      )
    }

    reg("list_position", "array_position", "list_indexof", "array_indexof") { args =>
      // 1-origin position of the first match, or NULL when absent
      fn("list_position", args, PType.Int64) { a =>
        val i = a(0)
          .asInstanceOf[Seq[Any]]
          .indexWhere(x => x != null && ValueOps.compare(x, a(1)) == 0)
        if i < 0 then
          null
        else
          i + 1L
      }
    }

    def listFn(names: String*)(f: (Seq[Any], Array[Any]) => Any): Unit =
      reg(names*) { args =>
        fn(names.head, args, PType.AnyT)(a => f(a(0).asInstanceOf[Seq[Any]], a))
      }
    listFn("list_sort", "array_sort", "sort_array") { (s, _) =>
      // NULLs last, as in DuckDB
      val (nulls, values) = s.partition(_ == null)
      values.sortWith((x, y) => ValueOps.compare(x, y) < 0) ++ nulls
    }
    listFn("list_reverse_sort", "array_reverse_sort") { (s, _) =>
      val (nulls, values) = s.partition(_ == null)
      values.sortWith((x, y) => ValueOps.compare(x, y) > 0) ++ nulls
    }
    listFn("list_reverse", "array_reverse")((s, _) => s.reverse)
    listFn("list_distinct", "array_distinct")((s, _) => s.filter(_ != null).distinct)
    reg("list_concat", "array_concat", "array_cat", "list_cat") { args =>
      // NULL lists are skipped
      nullSafeFn("list_concat", args, PType.AnyT)(a =>
        a.iterator.filter(_ != null).flatMap(_.asInstanceOf[Seq[Any]]).toList
      )
    }
    reg("list_slice", "array_slice") { args =>
      fn("list_slice", args.head +: args.tail.map(toLong), PType.AnyT) { a =>
        val s                 = a(0).asInstanceOf[Seq[Any]]
        def pos(i: Long): Int =
          if i < 0 then
            math.max(0, s.size + i.toInt + 1)
          else
            i.toInt
        val begin = math.max(1, pos(a(1).asInstanceOf[Long]))
        val end   = math.min(s.size, pos(a(2).asInstanceOf[Long]))
        s.slice(begin - 1, end)
      }
    }
    def rangeFn(name: String, inclusive: Boolean): Unit =
      reg(name) { args =>
        arity(name, args, 1, 2, 3)
        fn(name, args.map(toLong), PType.AnyT) { a =>
          val (start, stop) =
            if a.length == 1 then
              (0L, a(0).asInstanceOf[Long])
            else
              (a(0).asInstanceOf[Long], a(1).asInstanceOf[Long])
          val step =
            if a.length == 3 then
              a(2).asInstanceOf[Long]
            else
              1L
          if step == 0 then
            throw StatusCode.INVALID_ARGUMENT.newException(s"${name} step must not be 0")
          if inclusive then
            NumericRange.inclusive(start, stop, step).toList
          else
            NumericRange(start, stop, step).toList
        }
      }
    rangeFn("range", inclusive = false)
    rangeFn("generate_series", inclusive = true)

    // ---- maps ----
    reg("map_keys") { args =>
      fn("map_keys", args, PType.AnyT)(a => a(0).asInstanceOf[Map[Any, Any]].keys.toList)
    }
    reg("map_values") { args =>
      fn("map_values", args, PType.AnyT)(a => a(0).asInstanceOf[Map[Any, Any]].values.toList)
    }
    reg("map_contains", "map_contains_key") { args =>
      fn("map_contains", args, PType.Bool)(a => a(0).asInstanceOf[Map[Any, Any]].contains(a(1)))
    }
    reg("map_extract_value", "element_at") { args =>
      fn("map_extract_value", args, PType.AnyT) { a =>
        a(0) match
          case m: Map[?, ?] =>
            m.asInstanceOf[Map[Any, Any]].getOrElse(a(1), null)
          case s: Seq[?] =>
            val i = ValueOps.toLong(a(1)).toInt
            if i >= 1 && i <= s.size then
              s(i - 1)
            else
              null
          case _ =>
            null
      }
    }

    // ---- floating point ----
    def dblPredicate(names: String*)(f: Double => Boolean): Unit =
      reg(names*) { args =>
        args.head.ptype match
          case PType.Float64 =>
            fn(names.head, args, PType.Bool)(a => f(a(0).asInstanceOf[Double]))
          case _ =>
            // Integers and decimals are always finite
            fn(names.head, args, PType.Bool)(_ => f(0.0))
      }
    dblPredicate("isnan")(_.isNaN)
    dblPredicate("isinf")(_.isInfinite)
    dblPredicate("isfinite")(d => !d.isNaN && !d.isInfinite)

    // ---- string similarity ----
    reg("levenshtein", "editdist3") { args =>
      fn("levenshtein", args.map(str), PType.Int64)(a =>
        levenshtein(a(0).asInstanceOf[String], a(1).asInstanceOf[String]).toLong
      )
    }

    // ---- hashing ----
    reg("hash") { args =>
      // DuckDB's hash() returns a UBIGINT
      fn("hash", args, PType.Decimal(20, 0)) { a =>
        val h = a.iterator.map(duckdbHash).reduceLeft((x, y) => combineHash(x, y))
        JBigDecimal(java.lang.Long.toUnsignedString(h))
      }
    }

    // ---- timestamps ----
    reg("make_timestamp") { args =>
      arity("make_timestamp", args, 1, 6)
      if args.size == 1 then
        fn("make_timestamp", Seq(toLong(args.head)), PType.Timestamp)(a => a(0))
      else
        fn("make_timestamp", args.take(5).map(toLong) :+ toDouble(args(5)), PType.Timestamp) { a =>
          def i(k: Int): Int = a(k).asInstanceOf[Long].toInt
          val day            = ValueOps.daysFromCivil(i(0), i(1), i(2)).toLong
          day * ValueOps.MicrosPerDay +
            (i(3) * 3600L + i(4) * 60L) * ValueOps.MicrosPerSecond +
            math.round(a(5).asInstanceOf[Double] * ValueOps.MicrosPerSecond)
        }
    }

    // ---- JSON ----
    reg("json_extract_string", "json_extract_path_text", "json_value") { args =>
      fn("json_extract_string", args.map(str), PType.Str) { a =>
        JsonPath.extract(a(0).asInstanceOf[String], a(1).asInstanceOf[String]) match
          case Some(s: wvlet.uni.json.JSON.JSONString) =>
            s.v
          case Some(_: wvlet.uni.json.JSON.JSONNull) | None =>
            null
          case Some(other) =>
            wvlet.uni.json.JSON.format(other)
      }
    }
    reg("json_extract", "json_extract_path") { args =>
      fn("json_extract", args.map(str), PType.Str) { a =>
        JsonPath
          .extract(a(0).asInstanceOf[String], a(1).asInstanceOf[String])
          .map(wvlet.uni.json.JSON.format)
          .orNull
      }
    }

    m.result()

  end registry

end ScalarFunctions

/** A small MD5 implementation so the engine has no platform-specific digest dependency */
object MD5:
  private val S = Array(
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
    14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21
  )

  private val K = Array.tabulate(64)(i => (math.abs(math.sin(i + 1.0)) * 4294967296.0).toLong.toInt)

  def hex(s: String): String =
    val msg    = s.getBytes("UTF-8")
    val bitLen = msg.length.toLong * 8
    val padLen = ((msg.length + 8) / 64 + 1) * 64
    val buf    = new Array[Byte](padLen)
    System.arraycopy(msg, 0, buf, 0, msg.length)
    buf(msg.length) = 0x80.toByte
    var i = 0
    while i < 8 do
      buf(padLen - 8 + i) = ((bitLen >>> (8 * i)) & 0xff).toByte
      i += 1
    var a0    = 0x67452301
    var b0    = 0xefcdab89.toInt
    var c0    = 0x98badcfe.toInt
    var d0    = 0x10325476
    var chunk = 0
    while chunk < padLen do
      val m =
        Array.tabulate(16) { j =>
          val o = chunk + j * 4
          (buf(o) & 0xff) |
            ((buf(o + 1) & 0xff) << 8) |
            ((buf(o + 2) & 0xff) << 16) |
            ((buf(o + 3) & 0xff) << 24)
        }
      var a = a0
      var b = b0
      var c = c0
      var d = d0
      var k = 0
      while k < 64 do
        var f = 0
        var g = 0
        if k < 16 then
          f = (b & c) | (~b & d)
          g = k
        else if k < 32 then
          f = (d & b) | (~d & c)
          g = (5 * k + 1) % 16
        else if k < 48 then
          f = b ^ c ^ d
          g = (3 * k + 5) % 16
        else
          f = c ^ (b | ~d)
          g = (7 * k) % 16
        val tmp = d
        d = c
        c = b
        b = b + Integer.rotateLeft(a + f + K(k) + m(g), S(k))
        a = tmp
        k += 1
      a0 += a
      b0 += b
      c0 += c
      d0 += d
      chunk += 64
    end while
    val sb = StringBuilder()
    for v <- Seq(a0, b0, c0, d0) do
      var j = 0
      while j < 4 do
        val byte = (v >>> (8 * j)) & 0xff
        sb.append(f"${byte}%02x")
        j += 1
    sb.toString

  end hex

end MD5

/** A subset of JSONPath (`$.a.b[0]`) for json_extract functions */
object JsonPath:
  import wvlet.uni.json.JSON
  import wvlet.uni.json.JSON.*

  def extract(json: String, path: String): Option[JSONValue] =
    val doc =
      try
        JSON.parse(json)
      catch
        case _: Exception =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"Malformed JSON: ${json}")
    steps(path).foldLeft(Option(doc)) { (cur, step) =>
      cur.flatMap { v =>
        (v, step) match
          case (o: JSONObject, Left(key)) =>
            o.v.find(_._1 == key).map(_._2)
          case (a: JSONArray, Right(i)) =>
            val idx =
              if i < 0 then
                a.v.size + i
              else
                i
            a.v.lift(idx)
          case _ =>
            None
      }
    }

  private def steps(path: String): List[Either[String, Int]] =
    val p =
      if path.startsWith("$") then
        path.drop(1)
      else if path.startsWith("/") then
        // JSON pointer
        return path.drop(1).split("/").toList.map(Left(_))
      else
        "." + path
    val out = List.newBuilder[Either[String, Int]]
    var i   = 0
    while i < p.length do
      p.charAt(i) match
        case '.' =>
          val end0 = p.indexWhere(c => c == '.' || c == '[', i + 1)
          val end  =
            if end0 < 0 then
              p.length
            else
              end0
          out += Left(p.substring(i + 1, end).stripPrefix("\"").stripSuffix("\""))
          i = end
        case '[' =>
          val end = p.indexOf(']', i)
          val tok = p.substring(i + 1, end).trim
          if tok.startsWith("\"") || tok.startsWith("'") then
            out += Left(tok.drop(1).dropRight(1))
          else if tok.startsWith("#-") then
            out += Right(-tok.drop(2).toInt)
          else
            out += Right(tok.toInt)
          i = end + 1
        case _ =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported JSON path: ${path}")
    out.result()

  end steps

end JsonPath

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

import java.nio.charset.StandardCharsets

/** Platform differences in text handling */
trait TextCompat:
  /** Unicode NFC normalization */
  def nfcNormalize(s: String): String

/**
  * String functions with DuckDB's semantics, in pure Scala so that they run on every platform.
  * Results against DuckDB are checked in EngineStdlibStringTest
  */
object TextFunctions extends TextCompatImpl:

  private def codePoints(s: String): Array[Int] =
    val out = Array.newBuilder[Int]
    var i   = 0
    while i < s.length do
      val cp = s.codePointAt(i)
      out += cp
      i += Character.charCount(cp)
    out.result()

  /** Replace each code point of `from` with the one at the same position in `to`; drop the rest */
  def translate(s: String, from: String, to: String): String =
    val f = codePoints(from)
    val t = codePoints(to)
    // The first occurrence of a code point in `from` wins; -1 removes it
    val map = scala.collection.mutable.HashMap.empty[Int, Int]
    for
      i <- f.indices
      if !map.contains(f(i))
    do
      map(f(i)) =
        if i < t.length then
          t(i)
        else
          -1
    val sb = java.lang.StringBuilder()
    for cp <- codePoints(s) do
      val c = map.getOrElse(cp, cp)
      if c >= 0 then
        sb.appendCodePoint(c)
    sb.toString

  def octetLength(s: String): Long = s.getBytes(StandardCharsets.UTF_8).length.toLong

  private val HexUpper = "0123456789ABCDEF"

  private def unreserved(b: Int): Boolean =
    (b >= 'A' && b <= 'Z') ||
      (b >= 'a' && b <= 'z') ||
      (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == '~'

  /** RFC 3986 percent-encoding of the UTF-8 bytes: unreserved characters stay, a space is %20 */
  def urlEncode(s: String): String =
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    val sb    = StringBuilder(bytes.length)
    for b <- bytes do
      val v = b & 0xff
      if unreserved(v) then
        sb.append(v.toChar)
      else
        sb.append('%').append(HexUpper.charAt(v >>> 4)).append(HexUpper.charAt(v & 0x0f))
    sb.toString

  private def hexDigit(c: Char): Int =
    if c >= '0' && c <= '9' then
      c - '0'
    else if c >= 'a' && c <= 'f' then
      c - 'a' + 10
    else if c >= 'A' && c <= 'F' then
      c - 'A' + 10
    else
      -1

  /**
    * Decode %XX escapes as UTF-8 bytes. As DuckDB, '+' stays '+' and malformed escapes stay as they
    * are; decoded bytes that are not valid UTF-8 fail
    */
  def urlDecode(s: String): String =
    if s.indexOf('%') < 0 then
      s
    else
      val src = s.getBytes(StandardCharsets.UTF_8)
      val out = java.io.ByteArrayOutputStream(src.length)
      var i   = 0
      while i < src.length do
        val b = src(i)
        if b == '%' && i + 2 < src.length then
          val hi = hexDigit(src(i + 1).toChar)
          val lo = hexDigit(src(i + 2).toChar)
          if hi >= 0 && lo >= 0 then
            out.write((hi << 4) | lo)
            i += 3
          else
            out.write(b)
            i += 1
        else
          out.write(b)
          i += 1
      try
        Encoding.utf8(out.toByteArray)
      catch
        case _: wvlet.lang.api.WvletLangException =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"URL-decoded value of '${s}' is not valid UTF-8")

  /** Number of differing characters of two non-empty strings of the same length, as DuckDB */
  def hamming(a: String, b: String): Long =
    val x = codePoints(a)
    val y = codePoints(b)
    if x.isEmpty || y.isEmpty then
      throw StatusCode.INVALID_ARGUMENT.newException("hamming_distance: strings must not be empty")
    if x.length != y.length then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException("hamming_distance: strings must have the same length")
    var n = 0L
    var i = 0
    while i < x.length do
      if x(i) != y(i) then
        n += 1
      i += 1
    n

  /** Jaro similarity of two code point sequences; 0 when either is empty, as DuckDB */
  private def jaro(x: Array[Int], y: Array[Int]): Double =
    if x.isEmpty || y.isEmpty then
      0.0
    else
      val window  = math.max(0, math.max(x.length, y.length) / 2 - 1)
      val xm      = new Array[Boolean](x.length)
      val ym      = new Array[Boolean](y.length)
      var matches = 0
      var i       = 0
      while i < x.length do
        val lo    = math.max(0, i - window)
        val hi    = math.min(y.length - 1, i + window)
        var j     = lo
        var found = false
        while !found && j <= hi do
          if !ym(j) && x(i) == y(j) then
            xm(i) = true
            ym(j) = true
            matches += 1
            found = true
          j += 1
        i += 1
      if matches == 0 then
        0.0
      else
        var transpositions = 0
        var k              = 0
        i = 0
        while i < x.length do
          if xm(i) then
            while !ym(k) do
              k += 1
            if x(i) != y(k) then
              transpositions += 1
            k += 1
          i += 1
        val m = matches.toDouble
        (m / x.length + m / y.length + (m - transpositions / 2) / m) / 3.0

  /** Jaro-Winkler similarity (prefix scale 0.1, up to 4 characters), as DuckDB */
  def jaroWinkler(a: String, b: String): Double =
    val x   = codePoints(a)
    val y   = codePoints(b)
    val sim = jaro(x, y)
    if sim <= 0.7 then
      sim
    else
      var prefix = 0
      while prefix < 4 && prefix < x.length && prefix < y.length && x(prefix) == y(prefix) do
        prefix += 1
      sim + prefix * 0.1 * (1.0 - sim)

  /** All matches of the pattern: the whole match or the given group, NULL for a missing group */
  def regexpExtractAll(s: String, p: java.util.regex.Pattern, group: Int): Seq[String] =
    val m = p.matcher(s)
    if group < 0 || group > m.groupCount() then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"regexp_extract_all: pattern has no group ${group}")
    val out = Seq.newBuilder[String]
    while m.find() do
      out += m.group(group)
    out.result()

  /**
    * Split around matches of the pattern, keeping empty strings. As DuckDB (RE2), an empty match at
    * the start or end does not split, so an empty pattern splits into characters
    */
  def regexpSplit(s: String, p: java.util.regex.Pattern): Seq[String] =
    val m    = p.matcher(s)
    val out  = Seq.newBuilder[String]
    var last = 0
    while m.find() do
      val empty = m.start() == m.end()
      if !(empty && (m.start() == 0 || m.start() == s.length)) then
        out += s.substring(last, m.start())
        last = m.end()
    out += s.substring(last)
    out.result()

end TextFunctions

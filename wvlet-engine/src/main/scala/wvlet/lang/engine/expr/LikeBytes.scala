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

import java.nio.charset.StandardCharsets

/**
  * A LIKE pattern matched against a row's UTF-8 bytes (`plain_string_bytes`), without building its
  * String.
  *
  * Only patterns of literal fragments and `%` are supported: no `_`, escape character, or case
  * folding. For those, a byte match is the same as a String match. UTF-8 is self-synchronizing, so
  * one valid encoded fragment can't match starting in the middle of another character.
  */
abstract class LikeBytes:
  /** Whether the bytes from `from` (inclusive) to `to` (exclusive) match */
  def matches(b: Array[Byte], from: Int, to: Int): Boolean

object LikeBytes:
  /**
    * The byte matcher of a LIKE pattern, or null when the pattern needs a String match (it has `_`,
    * an escape character, or is case-insensitive)
    */
  def compile(pattern: String, caseInsensitive: Boolean, escape: Option[Char]): LikeBytes =
    if escape.isDefined || caseInsensitive || pattern.contains('_') then
      null
    else
      val parts = pattern.split("%", -1).map(_.getBytes(StandardCharsets.UTF_8))
      if parts.length == 1 then
        val p = parts(0)
        (b, from, to) => to - from == p.length && startsAt(b, from, p)
      else
        val first = parts.head
        val last  = parts.last
        val mids  = parts.slice(1, parts.length - 1).filter(_.nonEmpty)
        (b, from, to) =>
          if to - from < first.length + last.length || !startsAt(b, from, first) ||
            !startsAt(b, to - last.length, last)
          then
            false
          else
            // Each middle fragment, in order, after the previous one and before `last`
            var pos = from + first.length
            val end = to - last.length
            var ok  = true
            var k   = 0
            while ok && k < mids.length do
              val m = mids(k)
              val i = indexOf(b, pos, end, m)
              if i < 0 then
                ok = false
              else
                pos = i + m.length
              k += 1
            ok

  /** Whether `p` occurs in `b` at `at` (the caller checks that it fits) */
  private def startsAt(b: Array[Byte], at: Int, p: Array[Byte]): Boolean = equalAt(b, at, p, 0)

  // Whether b(at + j) == p(j) for j from `start` to the end of p (a loop: the ranged
  // Arrays.equals is not on Scala.js or Scala Native)
  private def equalAt(b: Array[Byte], at: Int, p: Array[Byte], start: Int): Boolean =
    var j = start
    while j < p.length && b(at + j) == p(j) do
      j += 1
    j == p.length

  /** The first position of `p` in `b` from `from`, ending by `to`, or -1 */
  private[expr] def indexOf(b: Array[Byte], from: Int, to: Int, p: Array[Byte]): Int =
    val m    = p.length
    val last = to - m
    if m == 0 then
      return if from <= to then
        from
      else
        -1
    val head = p(0)
    var i    = from
    while i <= last do
      if b(i) == head && equalAt(b, i, p, 1) then
        return i
      i += 1
    -1

end LikeBytes

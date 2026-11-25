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

import wvlet.lang.engine.vector.*

/**
  * `substring(s, start, len) IN (values)` evaluated by looking up the region of each string in a
  * hash table of the values, so that no substring is built: the region's hash is `String.hashCode`
  * over its chars. `start >= 1` and `len >= 0` are constants and the values are non-null strings.
  * The region follows SQL substring over code points: a string without surrogate chars up to the
  * region's end has its code points at its char positions, and any other string falls back to
  * building the substring
  */
final class SubstringInList private (
    str: VExpr,
    start: Long,
    len: Long,
    fallback: VExpr,
    values: Set[Any],
    // Open addressing over the values' hash codes: a power-of-two table with free slots
    slots: Array[String],
    hashes: Array[Int]
):
  private val mask = slots.length - 1

  /** The IN result with InList's NULL semantics; `values` is non-empty */
  def eval(b: Batch, hasNullValue: Boolean, negate: Boolean): Vec =
    val v                     = str.eval(b)
    val n                     = b.size
    val o                     = new Array[Boolean](n)
    var nulls: Array[Boolean] = null
    // The region's char positions in strings without surrogates before its end
    val begin = start - 1
    val end   = begin + len
    var i     = 0
    while i < n do
      val x = v.get(i)
      if x == null then
        if nulls == null then
          nulls = new Array[Boolean](n)
        nulls(i) = true
      else
        val s     = x.asInstanceOf[String]
        val limit = math.min(s.length.toLong, end).toInt
        val from  = math.min(begin, limit.toLong).toInt
        var plain = true
        var h     = 0
        var k     = 0
        while plain && k < limit do
          val c = s.charAt(k)
          if Character.isSurrogate(c) then
            plain = false
          else if k >= from then
            h = 31 * h + c
          k += 1
        val found =
          if plain then
            contains(s, from, limit - from, h)
          else
            // Code points differ from chars: compare the substring as built
            fallbackMatch(b, i)
        if !found && hasNullValue then
          if nulls == null then
            nulls = new Array[Boolean](n)
          nulls(i) = true
        else
          o(i) = found != negate
      end if
      i += 1
    end while
    BoolVec(o, nulls)

  end eval

  // Whether the region of s at [from, from + length), whose hash is h, is a value
  private def contains(s: String, from: Int, length: Int, h: Int): Boolean =
    var slot  = SubstringInList.mix(h) & mask
    var found = false
    var done  = false
    while !done do
      val c = slots(slot)
      if c == null then
        done = true
      else if hashes(slot) == h && c.length == length && s.regionMatches(from, c, 0, length) then
        found = true
        done = true
      else
        slot = (slot + 1) & mask
    found

  // The substring of row i built by the substring function, for strings with surrogates (rare)
  private def fallbackMatch(b: Batch, i: Int): Boolean = values.contains(
    fallback.eval(b.take(Array(i), 1)).get(0)
  )

end SubstringInList

object SubstringInList:
  private def mix(h: Int): Int = h ^ (h >>> 16)

  /**
    * The region lookup for an InList over `child` with `values`, or null when `child` isn't
    * `substring(s, start, len)` with constants `start >= 1` and `len >= 0`, or a value isn't a
    * non-null string, or there are no values
    */
  def of(child: VExpr, values: Set[Any]): SubstringInList =
    child match
      case RowFn("substring", Seq(s, Const(k: Long, _), Const(l: Long, _)), PType.Str, _, false)
          if k >= 1 && l >= 0 && s.ptype == PType.Str && values.nonEmpty &&
            values.forall(_.isInstanceOf[String]) =>
        // At most half full, so that every probe reaches a free slot
        val size   = Integer.highestOneBit(math.max(1, values.size) * 2) * 2
        val slots  = new Array[String](size)
        val hashes = new Array[Int](size)
        values.foreach { x =>
          val str  = x.asInstanceOf[String]
          val h    = str.hashCode
          var slot = mix(h) & (size - 1)
          while slots(slot) != null do
            slot = (slot + 1) & (size - 1)
          slots(slot) = str
          hashes(slot) = h
        }
        SubstringInList(s, k, l, child, values, slots, hashes)
      case _ =>
        null

end SubstringInList

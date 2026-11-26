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
  * `inner`, a condition over the string column `column` only, for aggregate FILTERs
  * (`dictionary_agg_filters`). When the column carries codes into a `dict` shared across batches
  * ([[StrVec.dict]], a hash-join build's `join_build_string_codes`), `local` (`inner` reading the
  * column as column 0) runs once over the dictionary's values and a NULL, the result is kept for
  * that dictionary, and each row takes its code's result. TPC-DS q59's seven `sum(...) filter
  * (where d_day_name = '...')` compare each sale row's day name seven times otherwise.
  *
  * Without a shared dictionary, or for batches of a dictionary other than the first one seen, it
  * runs as a [[PerDictEntry]] does. The signature is `inner`'s: the values are the same, only the
  * evaluation differs.
  */
case class PerSharedDictEntry(inner: VExpr, column: Int, local: VExpr) extends PerEntryCondition:
  def ptype: PType                  = inner.ptype
  def sig: String                   = inner.sig
  override def children: Seq[VExpr] = Seq(inner)
  override def isConstant: Boolean  = false

  // The results of `local` over the first shared dictionary seen. Racing first uses compute equal
  // results; the holder's final fields publish them safely
  @volatile
  private var cached: PerSharedDictEntry.Results = null

  def eval(b: Batch): Vec =
    b.column(column) match
      case s: StrVec if s.dict != null && s.hasCodes =>
        val r = resultsFor(s.dict, b.size)
        if r == null then
          perBatch.eval(b)
        else
          r.forCodes(s.codes, s.nulls != null, b.size)
      case _ =>
        perBatch.eval(b)

  def selectPositions(b: Batch, sel: Array[Int]): Array[Int] =
    b.column(column) match
      case s: StrVec if s.dict != null && s.hasCodes =>
        val r = resultsFor(s.dict, b.size)
        if r == null then
          perBatch.selectPositions(b, sel)
        else
          r.select(s.codes, sel, b.size)
      case _ =>
        perBatch.selectPositions(b, sel)

  // Without a shared dictionary: once per dictionary entry of the batch, or per row
  private val perBatch = PerDictEntry(inner, column, local)

  /**
    * The results over `dict`, computed for the first dictionary seen when it has at most
    * [[PerSharedDictEntry.MaxEntriesPerRow]] entries per row of the batch; null otherwise. Only the
    * first dictionary is kept, so batches alternating between builds (a union of joins, partitioned
    * builds) run per batch instead of recomputing the results for each
    */
  private[engine] def resultsFor(dict: Array[String], rows: Int): PerSharedDictEntry.Results =
    val c = cached
    if c != null && (c.dict eq dict) then
      c
    else if c != null || dict.length.toLong > PerSharedDictEntry.MaxEntriesPerRow.toLong * rows then
      null
    else
      val r = PerSharedDictEntry.Results.of(local, dict)
      cached = r
      r

end PerSharedDictEntry

object PerSharedDictEntry:
  /**
    * The most dictionary entries per row of the first batch that reads a dictionary: the results
    * serve every later batch with the same dictionary
    */
  inline val MaxEntriesPerRow = 4

  /**
    * The condition's result for each code of `dict`, and at `dict.length` for NULL: `values` reads
    * a NULL result as FALSE, `nulls` (null without NULL results) marks them, and `entryNulls` tells
    * whether a value of `dict` gives NULL
    */
  final class Results(
      val dict: Array[String],
      val values: Array[Boolean],
      val nulls: Array[Boolean],
      val entryNulls: Boolean
  ):
    /**
      * The result of each of n rows with these codes (-1 for NULL); `hasNulls` tells whether a row
      * may be NULL. Without NULL results among the rows, the vector has no nulls, so a mask of its
      * TRUE rows is its values
      */
    def forCodes(codes: Array[Int], hasNulls: Boolean, n: Int): Vec =
      val nullAt = dict.length
      val o      = new Array[Boolean](n)
      var i      = 0
      while i < n do
        val c = codes(i)
        o(i) = values(
          if c >= 0 then
            c
          else
            nullAt
        )
        i += 1
      if nulls == null || !(entryNulls || hasNulls && nulls(nullAt)) then
        BoolVec(o, null)
      else
        val on = new Array[Boolean](n)
        var j  = 0
        while j < n do
          val c = codes(j)
          on(j) = nulls(
            if c >= 0 then
              c
            else
              nullAt
          )
          j += 1
        BoolVec(o, on)

    /**
      * The positions of `sel` (all of 0 until `size` when null), in order, whose code (-1 for NULL)
      * has a TRUE result
      */
    def select(codes: Array[Int], sel: Array[Int], size: Int): Array[Int] =
      val nullAt = dict.length
      val n      =
        if sel == null then
          size
        else
          sel.length
      val out = new Array[Int](n)
      var k   = 0
      var i   = 0
      while i < n do
        val p =
          if sel == null then
            i
          else
            sel(i)
        val c = codes(p)
        if values(
            if c >= 0 then
              c
            else
              nullAt
          )
        then
          out(k) = p
          k += 1
        i += 1
      if k == n then
        out
      else
        java.util.Arrays.copyOf(out, k)

    end select

  end Results

  object Results:
    /** `local` evaluated over the values of `dict`, then over NULL */
    def of(local: VExpr, dict: Array[String]): Results =
      val m      = dict.length
      val values = java.util.Arrays.copyOf(dict, m + 1)
      val nulls  = new Array[Boolean](m + 1)
      nulls(m) = true
      val r = Kernels.asBool(local.eval(Batch(Array[Vec](StrVec(values, nulls)), m + 1)))
      // Read a NULL result as FALSE values, so that a mask of TRUE rows reads the values only
      val vs = new Array[Boolean](m + 1)
      var i  = 0
      while i < m + 1 do
        vs(i) = r.values(i) && (r.nulls == null || !r.nulls(i))
        i += 1
      val entryNulls = r.nulls != null && (0 until m).exists(r.nulls(_))
      Results(dict, vs, r.nulls, entryNulls)

end PerSharedDictEntry

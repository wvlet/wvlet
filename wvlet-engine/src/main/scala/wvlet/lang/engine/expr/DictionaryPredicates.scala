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
  * `inner`, a condition over the string column `column` only, evaluated once per distinct
  * dictionary entry of the batch when that column carries Parquet dictionary codes
  * ([[StrVec.codes]], `dictionary_filters`). `local` is `inner` reading the column as column 0.
  *
  * The rows' distinct codes, NULL, and each PLAIN-encoded row without a code, form a small batch of
  * one row each; `local` runs over it, and every row takes the result of its entry. With more
  * entries than half the rows (or no codes), `inner` runs over the batch as is. The signature is
  * `inner`'s: the values are the same, only the evaluation differs.
  */
case class PerDictEntry(inner: VExpr, column: Int, local: VExpr) extends VExpr:
  def ptype: PType                  = inner.ptype
  def sig: String                   = inner.sig
  override def children: Seq[VExpr] = Seq(inner)
  override def isConstant: Boolean  = false

  def eval(b: Batch): Vec =
    b.column(column) match
      // A dictionary larger than the batch costs more to index than per-row evaluation saves
      case s: StrVec if s.hasCodes && s.dictSize > 0 && s.dictSize <= b.size =>
        val r = DictionaryPredicates.evalPerEntry(local, s, b.size)
        if r == null then
          inner.eval(b)
        else
          r
      case _ =>
        inner.eval(b)

  /**
    * The positions of `sel` (every row of `b` when null), in order, at which the condition is TRUE,
    * computed once per dictionary entry of those rows; null when the column carries no codes (or a
    * dictionary larger than the batch), so the caller evaluates it per row
    */
  def selectPositions(b: Batch, sel: Array[Int]): Array[Int] =
    b.column(column) match
      case s: StrVec if s.hasCodes && s.dictSize > 0 && s.dictSize <= b.size =>
        DictionaryPredicates.selectPerEntry(local, s, sel, b.size)
      case _ =>
        null

end PerDictEntry

object DictionaryPredicates:

  /**
    * `cond` with each AND / OR part that reads only one string column, made of comparisons, IN
    * lists, LIKE, IS NULL, and NOT over them, as a [[PerDictEntry]]. ANDs and ORs stay outside, so
    * [[Selection]] still short-circuits them when a part runs per row; anything else stays as is.
    * With `whole`, an AND / OR reading only one string column becomes one [[PerDictEntry]] too (`x =
    * 'a' or x = 'b'` runs once per entry, not twice)
    */
  def wrap(cond: VExpr, whole: Boolean = false): VExpr =
    cond match
      case _: AndExpr | _: OrExpr
          if whole && cond.ptype == PType.Bool && !cond.isConstant &&
            singleStrColumn(cond).isDefined =>
        val c = singleStrColumn(cond).get
        PerDictEntry(cond, c, rebase(cond, c))
      case AndExpr(l, r) =>
        AndExpr(wrap(l, whole), wrap(r, whole))
      case OrExpr(l, r) =>
        OrExpr(wrap(l, whole), wrap(r, whole))
      case _ =>
        singleStrColumn(cond) match
          case Some(c) if cond.ptype == PType.Bool && !cond.isConstant =>
            PerDictEntry(cond, c, rebase(cond, c))
          case _ =>
            cond match
              case NotExpr(c) =>
                NotExpr(wrap(c, whole))
              case other =>
                other

  // The one string column `e` reads, when every node of it is one `wrap` evaluates per entry
  private def singleStrColumn(e: VExpr): Option[Int] =
    var col                      = -1
    def visit(x: VExpr): Boolean =
      x match
        case c: ColRef =>
          if c.ptype != PType.Str || (col >= 0 && col != c.index) then
            false
          else
            col = c.index
            true
        case _: Const =>
          true
        case _: Compare | _: NullSafeDistinct | _: IsNullExpr | _: InList | _: LikeExpr |
            _: AndExpr | _: OrExpr | _: NotExpr =>
          x.children.forall(visit)
        case _ =>
          false
    if visit(e) && col >= 0 then
      Some(col)
    else
      None

  // `e` (accepted by singleStrColumn) reading column `col` as column 0
  private def rebase(e: VExpr, col: Int): VExpr =
    def r(x: VExpr): VExpr = rebase(x, col)
    e match
      case c: ColRef =>
        c.copy(index = 0)
      case c: Const =>
        c
      case x: Compare =>
        x.copy(left = r(x.left), right = r(x.right))
      case x: NullSafeDistinct =>
        x.copy(left = r(x.left), right = r(x.right))
      case x: IsNullExpr =>
        x.copy(child = r(x.child))
      case x: InList =>
        x.copy(child = r(x.child))
      case x: LikeExpr =>
        x.copy(child = r(x.child))
      case AndExpr(a, b) =>
        AndExpr(r(a), r(b))
      case OrExpr(a, b) =>
        OrExpr(r(a), r(b))
      case NotExpr(a) =>
        NotExpr(r(a))
      case other =>
        throw IllegalStateException(s"Unexpected per-entry expression: ${other.sig}")

  /**
    * `local` over the `n` rows of `s`, evaluated once per distinct code, once for NULL, and once
    * per PLAIN row, or null when there are more than n / 2 of those. `s` has at most n dictionary
    * entries
    */
  private[expr] def evalPerEntry(local: VExpr, s: StrVec, n: Int): Vec =
    val codes = s.codes
    val limit = n / 2
    // slot(code) is 1 + the entry's position in `reps`, 0 when not seen yet
    val slot = new Array[Int](s.dictSize)
    val reps = new Array[Int](limit)
    val map  = new Array[Int](n)
    // NULL rows all share one entry
    var nullEntry = -1
    var m         = 0
    var i         = 0
    while i < n do
      val c      = codes(i)
      val isNull = c < 0 && s.isNull(i)
      val seen   =
        if c >= 0 then
          slot(c) - 1
        else if isNull then
          nullEntry
        else
          -1
      if seen >= 0 then
        map(i) = seen
      else
        if m >= limit then
          return null
        reps(m) = i
        if c >= 0 then
          slot(c) = m + 1
        else if isNull then
          nullEntry = m
        map(i) = m
        m += 1
      i += 1
    // The entries' values only: `local` reads no codes
    val values = new Array[String](m)
    val nulls  =
      if nullEntry >= 0 then
        new Array[Boolean](m)
      else
        null
    var j = 0
    while j < m do
      values(j) = s.values(reps(j))
      j += 1
    if nullEntry >= 0 then
      nulls(nullEntry) = true
    val entries = Batch(Array[Vec](StrVec(values, nulls)), m)
    local.eval(entries).gather(map, n)

  end evalPerEntry

  /**
    * The positions of `sel` (all of 0 until `size` when null) at which `local`, evaluated once per
    * dictionary entry of `s` found there, is TRUE; null when a selected non-NULL row has no code
    */
  private[expr] def selectPerEntry(
      local: VExpr,
      s: StrVec,
      sel: Array[Int],
      size: Int
  ): Array[Int] =
    val codes = s.codes
    val n     =
      if sel == null then
        size
      else
        sel.length
    // slot(code) is 1 + the entry's position in `reps`, 0 when not seen yet
    val slot = new Array[Int](s.dictSize)
    // One entry per code seen, and one for NULL rows
    val reps      = new Array[Int](math.min(n, s.dictSize + 1))
    var nullEntry = -1
    var m         = 0
    var i         = 0
    while i < n do
      val p =
        if sel == null then
          i
        else
          sel(i)
      val c = codes(p)
      if c >= 0 then
        if slot(c) == 0 then
          reps(m) = p
          m += 1
          slot(c) = m
      else if s.isNull(p) then
        if nullEntry < 0 then
          nullEntry = m
          reps(m) = p
          m += 1
      else
        return null
      i += 1
    val values = new Array[String](m)
    val nulls  =
      if nullEntry >= 0 then
        new Array[Boolean](m)
      else
        null
    var j = 0
    while j < m do
      values(j) = s.values(reps(j))
      j += 1
    if nullEntry >= 0 then
      nulls(nullEntry) = true
    val accepted = Kernels.trueMask(local.eval(Batch(Array[Vec](StrVec(values, nulls)), m)))
    val nullTrue = nullEntry >= 0 && accepted(nullEntry)
    val out      = new Array[Int](n)
    var k        = 0
    i = 0
    while i < n do
      val p =
        if sel == null then
          i
        else
          sel(i)
      val c = codes(p)
      if (c >= 0 && accepted(slot(c) - 1)) || (c < 0 && nullTrue) then
        out(k) = p
        k += 1
      i += 1
    if k == n then
      out
    else
      java.util.Arrays.copyOf(out, k)

  end selectPerEntry

end DictionaryPredicates

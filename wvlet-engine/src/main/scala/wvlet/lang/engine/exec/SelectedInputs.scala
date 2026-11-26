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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * Hash aggregation inputs read in place from the deferred columns of a batch
  * (`aggregate_selected_rows`): a filter or a join probe hands its output columns on as source
  * vectors and row positions ([[Batch.take]]), and gathering them copies every selected value into
  * a new array that the aggregation reads once. Instead, aggregates over a column read its source
  * at the positions ([[Accumulator.updateSelected]]), `+` / `-` subtrees of arguments over columns
  * compute one output array from the sources ([[eval]], also for the one-row partial results of
  * passed-through rows), and packed group keys pack from the sources
  * ([[PackedGroupTable.groupIdsAt]]). Fused decimal `x * (c ± y)` products ([[FusedDecimalMul]])
  * read their column operands in place too. A position of -1 (a row of NULLs of an outer join)
  * reads as NULL.
  */
private[exec] object SelectedInputs:

  /**
    * Fold the rows of b into acc when its only argument `e` reads in place: a deferred column the
    * accumulator reads at its positions, or a `+` / `-` expression over columns. False when the
    * argument is evaluated as usual
    */
  def update(acc: Accumulator, e: VExpr, groups: Array[Int], b: Batch): Boolean =
    e match
      case c: ColRef =>
        val src = b.deferredSource(c.index)
        src != null && acc.updateSelected(groups, b.size, src, b.deferredPositions(c.index))
      case a: Arith if readsInPlace(a, b) =>
        acc.update(groups, b.size, Array(eval(a, b)))
        true
      case f: FusedDecimalMul if readsInPlace(f, b) =>
        acc.update(groups, b.size, Array(eval(f, b)))
        true
      case _ =>
        false

  /**
    * The value of `e` over b, with its `+` / `-` subtrees over deferred columns computed from the
    * columns' sources ([[arith]]) and the other arithmetic over them as [[Arith.eval]] does; other
    * expressions evaluate as usual
    */
  def eval(e: VExpr, b: Batch): Vec =
    e match
      case a: Arith if readsInPlace(a, b) =>
        val v =
          if a.op == ArithOp.Add || a.op == ArithOp.Sub then
            arith(a, b)
          else
            null
        if v != null then
          v
        else
          Kernels.arith(a.op, eval(a.left, b), eval(a.right, b), a.ptype)
      case f: FusedDecimalMul if readsInPlace(f, b) =>
        val v = fusedMul(f, b)
        if v != null then
          v
        else
          f.evalOver(eval(f.x, b), eval(f.y, b))
      case _ =>
        e.eval(b)

  /** Whether arithmetic `e` reads a deferred column of b, which [[eval]] may read in place */
  private def readsInPlace(e: VExpr, b: Batch): Boolean =
    e match
      case a: Arith =>
        readsInPlace(a.left, b) || readsInPlace(a.right, b)
      case f: FusedDecimalMul =>
        readsInPlace(f.x, b) || readsInPlace(f.y, b)
      case c: ColRef =>
        b.deferredSource(c.index) != null
      case _ =>
        false

  /**
    * Group ids of the rows of b when its keys are packed (`table`): deferred key columns pack from
    * their sources
    */
  def groupIds(table: PackedGroupTable, keys: IndexedSeq[VExpr], b: Batch): Array[Int] =
    val m                       = keys.size
    val vs                      = new Array[Vec](m)
    var sels: Array[Array[Int]] = null
    var k                       = 0
    while k < m do
      keys(k) match
        case c: ColRef if b.deferredSource(c.index) != null =>
          if sels == null then
            sels = new Array[Array[Int]](m)
          vs(k) = b.deferredSource(c.index)
          sels(k) = b.deferredPositions(c.index)
        case e =>
          vs(k) = e.eval(b)
      k += 1
    if sels == null then
      table.groupIds(vs, b.size)
    else
      table.groupIdsAt(vs, sels, b.size)

  /** The values of `vs(k)` at `sels(k)` (all of them where `sels(k)` is null) */
  def gathered(vs: Array[Vec], sels: Array[Array[Int]], n: Int): Array[Vec] =
    Array.tabulate(vs.length) { k =>
      if sels(k) == null then
        vs(k)
      else
        vs(k).gather(sels(k), n)
    }

  /**
    * A leaf of an [[arith]] tree: a column's values, at `sel` when deferred (null: row i is value
    * i)
    */
  private final class Leaf(val vec: Vec, val sel: Array[Int])

  /** How the values of a supported tree are stored */
  private enum Kind:
    // Long values: Int64, or unscaled decimals at one scale
    case Longs
    case Doubles

  /**
    * The value of `e`, an Add / Sub tree over columns, of which at least one is deferred, computed
    * from the columns' sources into one array; null when `e` has another form, a column isn't
    * stored as the tree's values, or a decimal overflows a Long (then `e` is evaluated as usual).
    * Int64 overflow throws as [[Kernels.arith]] does
    */
  def arith(e: Arith, b: Batch): Vec =
    val kind =
      e.ptype match
        case PType.Int64 =>
          Kind.Longs
        case PType.Decimal(_, _) =>
          Kind.Longs
        case PType.Float64 =>
          Kind.Doubles
        case _ =>
          return null
    val leaves   = List.newBuilder[Leaf]
    var deferred = false
    // Whether e is a supported tree, collecting its leaves (left to right)
    def supported(x: VExpr): Boolean =
      x match
        case a: Arith =>
          (a.op == ArithOp.Add || a.op == ArithOp.Sub) && sameStorage(a.ptype, e.ptype) &&
          supported(a.left) && supported(a.right)
        case c: ColRef =>
          val src  = b.deferredSource(c.index)
          val leaf =
            if src != null then
              deferred = true
              Leaf(src, b.deferredPositions(c.index))
            else
              Leaf(b.column(c.index), null)
          leaves += leaf
          stores(leaf.vec, kind, e.ptype)
        case _ =>
          false
    if !supported(e) || !deferred then
      return null
    val ls = leaves.result()
    val n  = b.size
    kind match
      case Kind.Longs =>
        val o = longs(e, ls.iterator, n, e.ptype.isDecimal)
        if o == null then
          null
        else
          LongVec(e.ptype, o, nullsOf(ls, n))
      case Kind.Doubles =>
        DoubleVec(doubles(e, ls.iterator, n), nullsOf(ls, n))

  end arith

  /**
    * The value of fused `x * (c ± y)` ([[FusedDecimalMul]]) with deferred column operands read at
    * their positions, and other operands computed as [[eval]] does; null when an operand isn't held
    * as unscaled Longs at its scale or a value overflows a Long (then f is evaluated as usual)
    */
  private def fusedMul(f: FusedDecimalMul, b: Batch): Vec =
    def operand(e: VExpr): Leaf =
      e match
        case c: ColRef =>
          val src = b.deferredSource(c.index)
          if src != null then
            Leaf(src, b.deferredPositions(c.index))
          else
            Leaf(b.column(c.index), null)
        case _ =>
          Leaf(eval(e, b), null)
    val xl = operand(f.x)
    val yl = operand(f.y)
    val xa = f.unscaledOf(xl.vec, f.xValueScale)
    val ya = f.unscaledOf(yl.vec, f.yValueScale)
    if xa == null || ya == null then
      return null
    val n = b.size
    val o = f.compute(xa, xl.sel, ya, yl.sel, n)
    if o == null then
      null
    else
      LongVec(f.ptype, o, nullsOf(List(xl, yl), n))

  /** Whether values of types a and b are stored alike: Int64, Float64, or decimals at one scale */
  private def sameStorage(a: PType, b: PType): Boolean =
    (a, b) match
      case (PType.Decimal(_, s), PType.Decimal(_, t)) =>
        s == t
      case _ =>
        a == b

  /** Whether v holds its values as `kind` arrays of type t (unscaled at t's scale for decimals) */
  private def stores(v: Vec, kind: Kind, t: PType): Boolean =
    (kind, v) match
      case (Kind.Longs, l: LongVec) =>
        (l.ptype, t) match
          case (PType.Decimal(_, s), PType.Decimal(_, ts)) =>
            s == ts
          case (PType.Int64, PType.Int64) =>
            true
          case _ =>
            false
      case (Kind.Doubles, _: DoubleVec) =>
        true
      case _ =>
        false

  /** NULL where a leaf's row is NULL or at position -1; null when no row is */
  private def nullsOf(leaves: List[Leaf], n: Int): Array[Boolean] =
    var out: Array[Boolean] = null
    leaves.foreach { leaf =>
      val vn  = leaf.vec.nulls
      val sel = leaf.sel
      if sel == null then
        if vn != null then
          if out == null then
            out = new Array[Boolean](n)
          var i = 0
          while i < n do
            out(i) |= vn(i)
            i += 1
      else
        var i = 0
        while i < n do
          val k = sel(i)
          if k < 0 || (vn != null && vn(k)) then
            if out == null then
              out = new Array[Boolean](n)
            out(i) = true
          i += 1
    }
    out

  /**
    * The Long values of tree e over the leaves `it` (in e's order), computed into one array: a node
    * reuses the array of a child it computed, and reads leaves in place. Null on a decimal overflow
    */
  private def longs(e: VExpr, it: Iterator[Leaf], n: Int, decimal: Boolean): Array[Long] =
    e match
      case a: Arith =>
        // Leaves are read in place; subtrees give an array this node may write into
        def side(x: VExpr): Either[Leaf, Array[Long]] =
          x match
            case _: Arith =>
              Right(longs(x, it, n, decimal))
            case _ =>
              Left(it.next())
        val l = side(a.left)
        if l.exists(_ == null) then
          return null
        val r = side(a.right)
        if r.exists(_ == null) then
          return null
        val out =
          l match
            case Right(arr) =>
              arr
            case _ =>
              r match
                case Right(arr) =>
                  arr
                case _ =>
                  new Array[Long](n)
        val (lv, ls) = sideValues(l)
        val (rv, rs) = sideValues(r)
        val ok       =
          if a.op == ArithOp.Add then
            addLongs(lv, ls, rv, rs, out, n, decimal)
          else
            subLongs(lv, ls, rv, rs, out, n, decimal)
        if ok then
          out
        else
          null
      case _ =>
        null

  private def sideValues(s: Either[Leaf, Array[Long]]): (Array[Long], Array[Int]) =
    s match
      case Left(leaf) =>
        (leaf.vec.asInstanceOf[LongVec].values, leaf.sel)
      case Right(arr) =>
        (arr, null)

  // Value i of an operand: its values at sel(i) (0 at -1), or at i without sel
  private inline def at(values: Array[Long], sel: Array[Int], i: Int): Long =
    if sel == null then
      values(i)
    else
      val k = sel(i)
      if k < 0 then
        0L
      else
        values(k)

  private def addLongs(
      a: Array[Long],
      as: Array[Int],
      b: Array[Long],
      bs: Array[Int],
      out: Array[Long],
      n: Int,
      decimal: Boolean
  ): Boolean =
    var i = 0
    if decimal then
      var ok = true
      while ok && i < n do
        val x = at(a, as, i)
        val y = at(b, bs, i)
        val z = x + y
        // Overflow iff both operands have the same sign and the result's sign differs
        if ((x ^ z) & (y ^ z)) < 0 then
          ok = false
        out(i) = z
        i += 1
      ok
    else
      while i < n do
        out(i) = Math.addExact(at(a, as, i), at(b, bs, i))
        i += 1
      true

  private def subLongs(
      a: Array[Long],
      as: Array[Int],
      b: Array[Long],
      bs: Array[Int],
      out: Array[Long],
      n: Int,
      decimal: Boolean
  ): Boolean =
    var i = 0
    if decimal then
      var ok = true
      while ok && i < n do
        val x = at(a, as, i)
        val y = at(b, bs, i)
        val z = x - y
        if ((x ^ y) & (x ^ z)) < 0 then
          ok = false
        out(i) = z
        i += 1
      ok
    else
      while i < n do
        out(i) = Math.subtractExact(at(a, as, i), at(b, bs, i))
        i += 1
      true

  /** The Double values of tree e over the leaves `it`, as [[longs]] computes Long ones */
  private def doubles(e: VExpr, it: Iterator[Leaf], n: Int): Array[Double] =
    val a                                           = e.asInstanceOf[Arith]
    def side(x: VExpr): Either[Leaf, Array[Double]] =
      x match
        case _: Arith =>
          Right(doubles(x, it, n))
        case _ =>
          Left(it.next())
    val l   = side(a.left)
    val r   = side(a.right)
    val out =
      l match
        case Right(arr) =>
          arr
        case _ =>
          r match
            case Right(arr) =>
              arr
            case _ =>
              new Array[Double](n)
    def values(s: Either[Leaf, Array[Double]]): (Array[Double], Array[Int]) =
      s match
        case Left(leaf) =>
          (leaf.vec.asInstanceOf[DoubleVec].values, leaf.sel)
        case Right(arr) =>
          (arr, null)
    val (lv, ls)                                                    = values(l)
    val (rv, rs)                                                    = values(r)
    inline def dat(v: Array[Double], s: Array[Int], i: Int): Double =
      if s == null then
        v(i)
      else
        val k = s(i)
        if k < 0 then
          0.0
        else
          v(k)
    var i = 0
    if a.op == ArithOp.Add then
      while i < n do
        out(i) = dat(lv, ls, i) + dat(rv, rs, i)
        i += 1
    else
      while i < n do
        out(i) = dat(lv, ls, i) - dat(rv, rs, i)
        i += 1
    out

  end doubles

end SelectedInputs

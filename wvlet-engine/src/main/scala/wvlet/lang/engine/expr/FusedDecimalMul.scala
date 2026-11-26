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

import scala.util.control.NonFatal

/**
  * Decimal `x * (c ± y)` (or `(c ± y) * x`) for a constant c, computed in one loop over unscaled
  * Longs (`fused_decimal_arith`): TPC-H's `l_extendedprice * (1 - l_discount)` and
  * `... * (1 + l_tax)`. [[Arith]] evaluates the inner `c ± y` into an array first, then the product
  * with a Long division per row to check it for overflow. Here each row computes `t = c ± y` and
  * `x * t` at once, and the product needs the division only when `x` or `t` is outside the Int
  * range (their product then may not fit). The expression prints as the `Arith` it replaces.
  *
  * `inner` is the Add / Sub, one of whose operands is a constant at the inner type's scale, and the
  * other (`y`) a decimal at that scale; `x` is a decimal whose scale adds up with it to the
  * product's. When a vector isn't held as unscaled Longs at those scales, or a value overflows a
  * Long, the expression evaluates as [[Kernels.arith]] does.
  */
case class FusedDecimalMul(x: VExpr, inner: Arith, innerLeft: Boolean, ptype: PType) extends VExpr:
  override def children: Seq[VExpr] = Seq(x, inner)

  def sig: String =
    if innerLeft then
      s"(${inner.sig}*${x.sig})"
    else
      s"(${x.sig}*${inner.sig})"

  // The constant operand, as the inner Add / Sub reads it
  private val constLeft: Boolean = inner.left.isInstanceOf[Const]

  /** The non-constant operand of the inner Add / Sub */
  def y: VExpr =
    if constLeft then
      inner.right
    else
      inner.left

  private val constExpr: VExpr =
    if constLeft then
      inner.left
    else
      inner.right

  // t = c + y, or t = c - y (c - y with the constant on the left, y + (-c) on the right)
  private val subtractY: Boolean = inner.op == ArithOp.Sub && constLeft

  private val c: Long =
    val v = FusedDecimalMul.unscaled(constExpr, FusedDecimalMul.scaleOf(inner.ptype)).get
    if inner.op == ArithOp.Sub && !constLeft then
      -v
    else
      v

  private val xScale = FusedDecimalMul.scaleOf(x.ptype)
  private val yScale = FusedDecimalMul.scaleOf(inner.ptype)

  def eval(b: Batch): Vec = evalOver(x.eval(b), y.eval(b))

  /**
    * [[eval]] with the outputs of this product, and of fused products among its operands, in arrays
    * of `scratch` (`agg_scratch_buffers`): the result is valid only until the scratch is reset
    */
  private[engine] def evalWith(b: Batch, scratch: LongScratch): Vec =
    def operand(e: VExpr): Vec =
      e match
        case f: FusedDecimalMul =>
          f.evalWith(b, scratch)
        case _ =>
          e.eval(b)
    evalOver(operand(x), operand(y), scratch)

  /**
    * The product over the values xv of x and yv of y, as [[eval]] computes it, into an array of
    * `scratch` when given
    */
  private[engine] def evalOver(xv: Vec, yv: Vec, scratch: LongScratch = null): Vec =
    val n  = xv.size
    val xs = unscaledOf(xv, xScale)
    val ys = unscaledOf(yv, yScale)
    val o  =
      if xs == null || ys == null then
        null
      else
        val out =
          if scratch == null then
            new Array[Long](n)
          else
            scratch.take(n)
        compute(xs, null, ys, null, n, out)
    if o != null then
      LongVec(ptype, o, Kernels.orNulls(xv.nulls, yv.nulls, n))
    else
      fallback(xv, yv)

  /** As [[Arith.eval]] would compute it, over the already evaluated operands */
  private def fallback(xv: Vec, yv: Vec): Vec =
    val cv = constExpr.eval(Batch.ofRows(xv.size))
    val tv =
      if constLeft then
        Kernels.arith(inner.op, cv, yv, inner.ptype)
      else
        Kernels.arith(inner.op, yv, cv, inner.ptype)
    if innerLeft then
      Kernels.arith(ArithOp.Mul, tv, xv, ptype)
    else
      Kernels.arith(ArithOp.Mul, xv, tv, ptype)

  /** The unscaled values of v when held as Longs at `scale`, else null */
  private[engine] def unscaledOf(v: Vec, scale: Int): Array[Long] =
    v match
      case l: LongVec if FusedDecimalMul.scaleOf(l.ptype) == scale && l.ptype.isDecimal =>
        l.values
      case _ =>
        null

  private[engine] def xValueScale: Int = xScale
  private[engine] def yValueScale: Int = yScale

  /**
    * `x * (c ± y)` for rows 0 until n: x at `xs(i)` (`xa(i)` without xs) and likewise y, 0 at
    * position -1. Null when a value overflows a Long
    */
  private[engine] def compute(
      xa: Array[Long],
      xs: Array[Int],
      ya: Array[Long],
      ys: Array[Int],
      n: Int,
      out: Array[Long] = null
  ): Array[Long] =
    val o =
      if out == null then
        new Array[Long](n)
      else
        out
    val cc = c
    var ok = true
    var i  = 0
    if subtractY then
      while i < n do
        val xi = FusedDecimalMul.at(xa, xs, i)
        val yi = FusedDecimalMul.at(ya, ys, i)
        val t  = cc - yi
        val z  = xi * t
        if ((cc ^ yi) & (cc ^ t)) < 0 || !FusedDecimalMul.fits(xi, t, z) then
          ok = false
        o(i) = z
        i += 1
    else
      while i < n do
        val xi = FusedDecimalMul.at(xa, xs, i)
        val yi = FusedDecimalMul.at(ya, ys, i)
        val t  = cc + yi
        val z  = xi * t
        if ((cc ^ t) & (yi ^ t)) < 0 || !FusedDecimalMul.fits(xi, t, z) then
          ok = false
        o(i) = z
        i += 1
    if ok then
      o
    else
      null

  end compute

  /**
    * This expression with `f` applied to x and the inner operation; a plain [[Arith]] when `f`
    * changes the inner operation's form (a shared subexpression replaces it)
    */
  def mapChildren(f: VExpr => VExpr): VExpr =
    val nx = f(x)
    f(inner) match
      case a: Arith if FusedDecimalMul.innerForm(a, ptype, nx).isDefined =>
        FusedDecimalMul(nx, a, innerLeft, ptype)
      case other =>
        if innerLeft then
          Arith(ArithOp.Mul, other, nx, ptype)
        else
          Arith(ArithOp.Mul, nx, other, ptype)

end FusedDecimalMul

object FusedDecimalMul:

  /**
    * Some(()) when `inner` is a decimal Add / Sub of a constant and a decimal at its scale, and x a
    * non-constant decimal whose scale adds up with it to the scale of `out`
    */
  private[engine] def innerForm(inner: Arith, out: PType, x: VExpr): Option[Unit] =
    if inner.op != ArithOp.Add && inner.op != ArithOp.Sub then
      return None
    if !inner.ptype.isDecimal || !x.ptype.isDecimal || x.isConstant || !out.isDecimal then
      return None
    val s = scaleOf(inner.ptype)
    if scaleOf(x.ptype) + s != scaleOf(out) then
      return None
    val (k, y) =
      (inner.left, inner.right) match
        case (k: Const, y) if !y.isConstant =>
          (k, y)
        case (y, k: Const) if !y.isConstant =>
          (k, y)
        case _ =>
          return None
    if !y.ptype.isDecimal || scaleOf(y.ptype) != s then
      return None
    unscaled(k, s) match
      case Some(v) if v != Long.MinValue =>
        Some(())
      case _ =>
        None

  /** The unscaled Long of a non-NULL numeric constant at `scale`, when exact */
  private[expr] def unscaled(e: VExpr, scale: Int): Option[Long] =
    e match
      case Const(v, t) if v != null && (t.isIntegral || t.isDecimal) =>
        try
          val u = ValueOps.toBigDecimal(v).setScale(scale).unscaledValue()
          if u.bitLength() < 64 then
            Some(u.longValue())
          else
            None
        catch
          case NonFatal(_) =>
            None
      case _ =>
        None

  private[expr] def scaleOf(t: PType): Int =
    t match
      case PType.Decimal(_, s) =>
        s
      case _ =>
        0

  // Value i of an operand: its values at sel(i) (0 at -1), or at i without sel
  private[expr] inline def at(values: Array[Long], sel: Array[Int], i: Int): Long =
    if sel == null then
      values(i)
    else
      val k = sel(i)
      if k < 0 then
        0L
      else
        values(k)

  /**
    * Whether z = x * t is exact: always when both are in the Int range, whose product is below
    * 2^62; otherwise checked with a division
    */
  private[expr] inline def fits(x: Long, t: Long, z: Long): Boolean =
    (((x + 0x80000000L) | (t + 0x80000000L)) >>> 32) == 0 || x == 0 ||
      (z / x == t && !(x == -1 && t == Long.MinValue))

end FusedDecimalMul

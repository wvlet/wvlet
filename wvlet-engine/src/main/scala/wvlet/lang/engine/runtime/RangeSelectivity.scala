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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.PType

/**
  * Selectivity of range comparisons of integer and date columns with constants (`x < c`, `x >= c`,
  * `between`), read from the column's [min, max] in the source statistics and assuming values
  * spread evenly over it, as DuckDB and DataFusion estimate them. The bounds on one column combine
  * into one interval, so `d >= a and d < b` is the width of [a, b) rather than the product of two
  * independent estimates (`range_selectivity`)
  */
object RangeSelectivity:

  /** A bound `column op value` of an integer-valued column */
  case class Bound(column: Int, op: CmpOp, value: Long)

  /**
    * The bound a condition sets on an integer or date column (a [[ColRef]] by column id) when it
    * compares the column with a constant of the same type by `<`, `<=`, `>`, or `>=`
    */
  def bound(e: VExpr): Option[Bound] =
    def integral(c: ColRef, k: Const): Option[Long] =
      if c.ptype != k.ptype then
        None
      else
        (c.ptype, k.value) match
          case (PType.Int32 | PType.Int64 | PType.Date, v: Int) =>
            Some(v.toLong)
          case (PType.Int32 | PType.Int64 | PType.Date, v: Long) =>
            Some(v)
          case _ =>
            None
    def ordering(op: CmpOp): Boolean =
      op == CmpOp.Lt || op == CmpOp.Le || op == CmpOp.Gt || op == CmpOp.Ge
    e match
      case Compare(op, c: ColRef, k: Const) if ordering(op) =>
        integral(c, k).map(Bound(c.index, op, _))
      case Compare(op, k: Const, c: ColRef) if ordering(op) =>
        integral(c, k).map(Bound(c.index, op.flip, _))
      case _ =>
        None

  /**
    * The bounds of a condition that is a range comparison or a conjunction of them (`between` is
    * one condition of two bounds); None when any part is something else
    */
  def bounds(e: VExpr): Option[Seq[Bound]] =
    e match
      case AndExpr(l, r) =>
        for
          a <- bounds(l)
          b <- bounds(r)
        yield a ++ b
      case _ =>
        bound(e).map(Seq(_))

  /**
    * The fraction of the integers of [min, max] inside the interval the bounds (on one column)
    * leave, at least one value's share: an empty interval still estimates one value, as a filter
    * estimate of zero rows would make any join look free
    */
  def fraction(bounds: Seq[Bound], min: Long, max: Long): Double =
    var lo = min.toDouble
    var hi = max.toDouble
    bounds.foreach { b =>
      val v = b.value.toDouble
      b.op match
        case CmpOp.Lt =>
          hi = math.min(hi, v - 1)
        case CmpOp.Le =>
          hi = math.min(hi, v)
        case CmpOp.Gt =>
          lo = math.max(lo, v + 1)
        case CmpOp.Ge =>
          lo = math.max(lo, v)
        case _ =>
          ()
    }
    val width = max.toDouble - min.toDouble + 1
    math.min(1.0, math.max(1.0 / width, (hi - lo + 1) / width))

  /**
    * The selectivity of the conjunction of `conds`: the range bounds of each column with known
    * [min, max] (`range`) as one [[fraction]], times `others` of each remaining condition
    */
  def conjunction[C](
      conds: Seq[C],
      expr: C => VExpr,
      range: Int => Option[(Long, Long)],
      others: C => Double
  ): Double =
    // A condition counts as ranges when the [min, max] of every column it bounds is known
    val bounded = conds.map(c =>
      c ->
        bounds(expr(c)).flatMap { bs =>
          val withRange = bs.map(b => range(b.column).map(r => (b, r)))
          Option.when(withRange.forall(_.isDefined))(withRange.flatten)
        }
    )
    val ranges =
      bounded
        .collect { case (_, Some(brs)) =>
          brs
        }
        .flatten
        .groupBy(_._1.column)
        .values
        .map { brs =>
          val (min, max) = brs.head._2
          fraction(brs.map(_._1), min, max)
        }
        .product
    val rest =
      bounded
        .collect { case (c, None) =>
          others(c)
        }
        .product
    ranges * rest

  end conjunction

end RangeSelectivity

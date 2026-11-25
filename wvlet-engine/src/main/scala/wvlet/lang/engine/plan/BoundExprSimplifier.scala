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
package wvlet.lang.engine.plan

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.runtime.FoldConstants
import wvlet.lang.engine.runtime.PushDownFilters
import wvlet.lang.engine.vector.*

import java.math.BigDecimal as JBigDecimal

/**
  * Simplifications the [[Binder]] applies to each expression as it binds it, so that the optimizer
  * passes (filter pushdown, scan filters, estimates) see literals and plain column comparisons:
  *
  *   - `fold_bound_constants` ([[fold]]): a node over literal operands becomes a literal (`1200 +
  *     11`, `cast(1 + 10 as decimal(21,2))`, `1 < 2`), and `and`/`or` with a literal `true`/`false`
  *     operand simplify as three-valued logic allows.
  *   - `unwrap_casts` ([[unwrapCompare]], [[unwrapInList]]): a comparison of a lossless cast of an
  *     expression with literals compares the expression itself, with the literals converted to its
  *     type, when every literal converts exactly.
  */
object BoundExprSimplifier:

  /**
    * The node folded into a literal when all of its operands are literals and it is deterministic,
    * or `and`/`or` simplified by a literal operand. Children are bound, so already folded, before
    * their parent. An evaluation that fails (`1 / 0`, a bad cast) keeps the expression, so that the
    * error still occurs only when rows are evaluated
    */
  def fold(e: VExpr): VExpr =
    e match
      case _: Const | _: ColRef =>
        e
      // Children are already folded, so a deterministic node over literals reads no input
      case _
          if e.children.nonEmpty && e.children.forall(_.isInstanceOf[Const]) &&
            PushDownFilters.isDeterministic(e) =>
        FoldConstants.evaluated(e).getOrElse(e)
      // `false and x` is false and `true or x` is true even when x is NULL; `true and x` and
      // `false or x` are x
      case AndExpr(Const(false, PType.Bool), _) | AndExpr(_, Const(false, PType.Bool)) =>
        Const(false, PType.Bool)
      case AndExpr(Const(true, PType.Bool), x) if x.ptype == PType.Bool =>
        x
      case AndExpr(x, Const(true, PType.Bool)) if x.ptype == PType.Bool =>
        x
      case OrExpr(Const(true, PType.Bool), _) | OrExpr(_, Const(true, PType.Bool)) =>
        Const(true, PType.Bool)
      case OrExpr(Const(false, PType.Bool), x) if x.ptype == PType.Bool =>
        x
      case OrExpr(x, Const(false, PType.Bool)) if x.ptype == PType.Bool =>
        x
      case _ =>
        e

  /**
    * `cast(x as T) op k` as `x op k'` when the cast is lossless and order-preserving and the
    * literal k is exactly k' of x's type; the comparison as is otherwise. Either operand may be the
    * cast
    */
  def unwrapCompare(op: CmpOp, l: VExpr, r: VExpr): VExpr =
    (l, r) match
      case (CastExpr(x, t, false), Const(v, kt)) if v != null && kt == t =>
        narrowed(x.ptype, t, v)
          .map(c => Compare(op, x, Const(c, x.ptype)))
          .getOrElse(Compare(op, l, r))
      case (Const(v, kt), CastExpr(x, t, false)) if v != null && kt == t =>
        narrowed(x.ptype, t, v)
          .map(c => Compare(op, Const(c, x.ptype), x))
          .getOrElse(Compare(op, l, r))
      case _ =>
        Compare(op, l, r)

  /**
    * `cast(x as T) [not] in (values)` as `x [not] in (values')` for an integral x, when every value
    * converts exactly to x's type; None otherwise. Decimal lists stay cast, since list values match
    * by equality including their scale
    */
  def unwrapInList(
      target: VExpr,
      values: Set[Any],
      hasNull: Boolean,
      negate: Boolean
  ): Option[VExpr] =
    target match
      case CastExpr(x, t, false) if x.ptype.isIntegral =>
        val converted = values.toSeq.map(v => narrowed(x.ptype, t, v))
        Option.when(converted.forall(_.isDefined)) {
          InList(x, converted.flatten.toSet, hasNull, negate)
        }
      case _ =>
        None

  /**
    * Whether a cast from `from` to `to` keeps every value and its order: integer widening, an
    * integer to a decimal with enough integer digits, and a decimal to a decimal with no fewer
    * integer and fraction digits
    */
  private[plan] def isLossless(from: PType, to: PType): Boolean =
    (from, to) match
      case (PType.Int32, PType.Int64) =>
        true
      case (PType.Int32, PType.Decimal(p, s)) =>
        p - s >= 10
      case (PType.Int64, PType.Decimal(p, s)) =>
        p - s >= 19
      case (PType.Decimal(p1, s1), PType.Decimal(p2, s2)) =>
        s2 >= s1 && p2 - s2 >= p1 - s1
      case _ =>
        false

  /** The literal `v` of type `to` as a value of `from`, when the cast is lossless and v is exact */
  private def narrowed(from: PType, to: PType, v: Any): Option[Any] =
    if !isLossless(from, to) then
      None
    else
      try
        val d = ValueOps.toBigDecimal(v)
        from match
          case PType.Int32 =>
            Some(d.intValueExact())
          case PType.Int64 =>
            Some(d.longValueExact())
          case PType.Decimal(p, s) =>
            // Throws when the value has more fraction digits than s
            val x: JBigDecimal = d.setScale(s)
            Option.when(x.precision() <= p)(x)
          case _ =>
            None
      catch
        case _: ArithmeticException =>
          None

end BoundExprSimplifier

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
  *     type, when every literal converts exactly. With `round_unwrapped_literals`, a cast that only
  *     adds fraction digits also unwraps, with an inexact literal rounded ([[rounded]]).
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
    * cast. With `round`, a cast that only adds fraction digits also unwraps when k has more
    * fraction digits than x, with k' rounded towards the side that keeps the comparison's result
    * ([[rounded]])
    */
  def unwrapCompare(op: CmpOp, l: VExpr, r: VExpr, round: Boolean = false): VExpr =
    (l, r) match
      case (CastExpr(x, t, false), Const(v, kt)) if v != null && kt == t =>
        unwrapped(op, x, t, v, round)
          .map(c => Compare(op, x, Const(c, x.ptype)))
          .getOrElse(Compare(op, l, r))
      case (Const(v, kt), CastExpr(x, t, false)) if v != null && kt == t =>
        // `k op x` is `x op' k` with the operator flipped
        unwrapped(op.flip, x, t, v, round)
          .map(c => Compare(op, Const(c, x.ptype), x))
          .getOrElse(Compare(op, l, r))
      case _ =>
        Compare(op, l, r)

  private def unwrapped(op: CmpOp, x: VExpr, t: PType, v: Any, round: Boolean): Option[Any] =
    narrowed(x.ptype, t, v).orElse(
      if round then
        rounded(op, x.ptype, t, v)
      else
        None
    )

  /**
    * The literal `v` of type `to` rounded to a value k' of `from`, so that `x op v` equals `x op
    * k'` for every x of `from`, when the cast from `from` to `to` only adds fraction digits (an
    * integer or decimal to a decimal of no smaller scale; the engine's casts keep the value
    * whatever the precision). x lies on the grid of from's scale, so `x > v` is `x > floor(v)`, `x
    * <= v` is `x <= floor(v)`, `x >= v` is `x >= ceil(v)`, and `x < v` is `x < ceil(v)`.
    * (In)equality with a literal off the grid stays cast
    */
  private[plan] def rounded(op: CmpOp, from: PType, to: PType, v: Any): Option[Any] =
    val scale =
      (from, to) match
        case (PType.Int32 | PType.Int64, PType.Decimal(_, _)) =>
          Some(0)
        case (PType.Decimal(_, s1), PType.Decimal(_, s2)) if s2 >= s1 =>
          Some(s1)
        case _ =>
          None
    val mode =
      op match
        case CmpOp.Gt | CmpOp.Le =>
          java.math.RoundingMode.FLOOR
        case CmpOp.Ge | CmpOp.Lt =>
          java.math.RoundingMode.CEILING
        case _ =>
          // Throws for a literal off the grid
          java.math.RoundingMode.UNNECESSARY
    for
      s <- scale
      k <-
        try
          val d = ValueOps.toBigDecimal(v).setScale(s, mode)
          from match
            case PType.Int32 =>
              Some(d.intValueExact())
            case PType.Int64 =>
              Some(d.longValueExact())
            case PType.Decimal(p, _) =>
              Option.when(d.precision() <= p)(d)
            case _ =>
              None
        catch
          case _: ArithmeticException =>
            None
    yield k

  end rounded

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

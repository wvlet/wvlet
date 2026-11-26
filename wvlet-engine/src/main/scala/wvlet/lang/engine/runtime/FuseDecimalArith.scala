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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.PType

/**
  * Fuse the decimal `x * (c ± y)` products of an expression into [[FusedDecimalMul]]
  * (`fused_decimal_arith`), bottom up, so that a product of such products fuses at every level.
  * Runs after [[FoldConstants]], which brings the constant to the inner operation's scale. Parts of
  * unknown expression types stay as they are
  */
object FuseDecimalArith:
  def apply(e: VExpr): VExpr =
    e match
      case _ if e.children.isEmpty =>
        e
      case _ =>
        try
          VExprs.mapChildren(e, apply) match
            case a @ Arith(ArithOp.Mul, l, r, out: PType.Decimal) =>
              r match
                case ri: Arith if FusedDecimalMul.innerForm(ri, out, l).isDefined =>
                  FusedDecimalMul(l, ri, innerLeft = false, out)
                case _ =>
                  l match
                    case li: Arith if FusedDecimalMul.innerForm(li, out, r).isDefined =>
                      FusedDecimalMul(r, li, innerLeft = true, out)
                    case _ =>
                      a
            case other =>
              other
        catch
          case _: IllegalStateException =>
            e

end FuseDecimalArith

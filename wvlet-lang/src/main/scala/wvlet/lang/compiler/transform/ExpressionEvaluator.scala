package wvlet.lang.compiler.transform

import wvlet.lang.compiler.{BoundedSymbolInfo, Context}
import wvlet.lang.ext.NativeFunction
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.ValDef

object ExpressionEvaluator:
  def eval(expr: Expression, context: Context): Expression =
    expr match
      case n: NativeExpression =>
        val v = NativeFunction.callByName(n.name)
        v match
          case s: String =>
            StringLiteral(s, n.span)
          case i: Int =>
            LongLiteral(i, n.span)
          case i: Long =>
            LongLiteral(i, n.span)
          case f: Float =>
            DoubleLiteral(f, n.span)
          case d: Double =>
            DoubleLiteral(d, n.span)
          case b: Boolean =>
            if b then
              TrueLiteral(n.span)
            else
              FalseLiteral(n.span)
          case null =>
            NullLiteral(n.span)
          case _ =>
            // TODO Support more literal types
            GenericLiteral(n.dataType, v.toString, n.span)

      case other =>
        other

end ExpressionEvaluator

package wvlet.lang.compiler.transform

import wvlet.lang.compiler.ValSymbolInfo
import wvlet.lang.compiler.Context
import wvlet.lang.ext.NativeFunction
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.ValDef

object ExpressionEvaluator:
  def eval(expr: Expression)(using context: Context): Expression =
    expr match
      case n: NativeExpression =>
        val v = NativeFunction.callByName(n.name)
        v match
          case s: String =>
            StringLiteral.fromString(s, n.span)
          case i: Int =>
            LongLiteral(i, i.toString, n.span)
          case i: Long =>
            LongLiteral(i, i.toString, n.span)
          case f: Float =>
            DoubleLiteral(f, f.toString, n.span)
          case d: Double =>
            DoubleLiteral(d, d.toString, n.span)
          case b: Boolean =>
            if b then
              TrueLiteral(n.span)
            else
              FalseLiteral(n.span)
          case null =>
            NullLiteral(n.span)
          case _ =>
            // TODO Support more literal types
            val stringLiteral = StringLiteral.fromString(v.toString, n.span)
            GenericLiteral(n.dataType, stringLiteral, n.span)

      case other =>
        other

end ExpressionEvaluator

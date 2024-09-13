package wvlet.lang.compiler.transform

import wvlet.lang.compiler.*
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.Query
import wvlet.lang.model.expr.*

object RewriteExpr extends Phase("rewrite-expr"):

  def rewriteRules: List[ExpressionRewriteRule] =
    RewriteStringConcat :: RewriteStringInterpolation :: Nil

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val resolvedPlan = unit.resolvedPlan
    val newPlan = resolvedPlan.transformUp { case q: Query =>
      RewriteRule.rewriteExpr(q, rewriteRules, context)
    }
    unit.resolvedPlan = newPlan
    unit

  /**
    * 'a' + 'b' -> concat('a', 'b')
    */
  object RewriteStringConcat extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case a @ ArithmeticBinaryExpr(BinaryExprType.Add, left, right, _)
          if left.dataType == DataType.StringType =>
        FunctionApply(
          base = NameExpr.fromString("concat"),
          args = List(
            FunctionArg(None, left, left.nodeLocation),
            FunctionArg(None, right, left.nodeLocation)
          ),
          window = None,
          nodeLocation = a.nodeLocation
        )

  object RewriteStringInterpolation extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case s: InterpolatedString =>
        Expression.concat(s.parts) { (left, right) =>
          def quote(e: Expression): Expression =
            e match
              case s: StringPart =>
                StringLiteral(s.value, s.nodeLocation)
              case _ =>
                e

          FunctionApply(
            base = NameExpr.fromString("concat"),
            args = List(
              FunctionArg(None, quote(left), left.nodeLocation),
              FunctionArg(None, quote(right), left.nodeLocation)
            ),
            window = None,
            nodeLocation = s.nodeLocation
          )
        }

end RewriteExpr

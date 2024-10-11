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
          args = List(FunctionArg(None, left, left.span), FunctionArg(None, right, left.span)),
          window = None,
          span = a.span
        )

  object RewriteStringInterpolation extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case s: InterpolatedString if s.prefix.fullName == "s" =>
        // Replace only s"... " strings
        Expression.concat(s.parts) { (left, right) =>
          def quote(e: Expression): Expression =
            e match
              case s: StringPart =>
                StringLiteral(s.value, s.span)
              case _ =>
                e

          FunctionApply(
            base = NameExpr.fromString("concat"),
            args = List(
              FunctionArg(None, quote(left), left.span),
              FunctionArg(None, quote(right), right.span)
            ),
            window = None,
            span = s.span
          )
        }

end RewriteExpr

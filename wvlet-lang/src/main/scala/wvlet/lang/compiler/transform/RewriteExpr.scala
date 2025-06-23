package wvlet.lang.compiler.transform

import wvlet.lang.compiler.*
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.Query
import wvlet.lang.model.expr.*

object RewriteExpr extends Phase("rewrite-expr"):

  def rewriteRules: List[ExpressionRewriteRule] =
    RewriteStringConcat :: RewriteStringInterpolation :: WarnNullComparison :: Nil

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
            FunctionArg(None, left, false, left.span),
            FunctionArg(None, right, false, left.span)
          ),
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
                StringLiteral.fromString(s.value, s.span)
              case _ =>
                e

          FunctionApply(
            base = NameExpr.fromString("concat"),
            args = List(
              FunctionArg(None, quote(left), false, left.span),
              FunctionArg(None, quote(right), false, right.span)
            ),
            window = None,
            span = s.span
          )
        }

  /**
    * Warn about null comparisons that may yield undefined results
    */
  object WarnNullComparison extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case eq @ Eq(left, right, span) =>
        checkNullComparison(eq, left, right, "=", context)
        eq
      case neq @ NotEq(left, right, span) =>
        checkNullComparison(neq, left, right, "!=", context)
        neq

    private def checkNullComparison(
        expr: Expression,
        left: Expression,
        right: Expression,
        op: String,
        context: Context
    ): Unit =
      (left, right) match
        case (_: NullLiteral, _) | (_, _: NullLiteral) =>
          val suggestion =
            (left, right) match
              case (_: NullLiteral, _) if op == "=" =>
                s"${right.pp} is null"
              case (_, _: NullLiteral) if op == "=" =>
                s"${left.pp} is null"
              case (_: NullLiteral, _) if op == "!=" =>
                s"${right.pp} is not null"
              case (_, _: NullLiteral) if op == "!=" =>
                s"${left.pp} is not null"
              case _ =>
                "Use 'is null' or 'is not null' for null comparisons"

          val loc = context.sourceLocationAt(expr.span)
          context
            .workEnv
            .warn(
              s"${loc}: Comparison with null using '${op}' may yield undefined results. Consider using: ${suggestion}"
            )
        case _ =>
        // No warning needed

  end WarnNullComparison

end RewriteExpr

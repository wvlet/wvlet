package wvlet.lang.compiler.transform

import wvlet.lang.compiler.*
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.Query
import wvlet.lang.model.expr.*

object RewriteExpr extends Phase("rewrite-expr"):

  def rewriteRules: List[ExpressionRewriteRule] =
    RewriteStringConcat :: RewriteStringInterpolation :: RewriteIfExpr :: Nil

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val resolvedPlan = unit.resolvedPlan
    val newPlan      = rewriteOnly(unit.resolvedPlan, context)
    unit.resolvedPlan = newPlan
    unit

  def rewriteOnly(plan: LogicalPlan, context: Context = Context.NoContext): LogicalPlan =
    val newPlan: LogicalPlan = plan.transformUp { case q: Query =>
      RewriteRule.rewriteExpr(q, rewriteRules, context)
    }
    newPlan

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
            FunctionArg(None, left, false, Nil, left.span),
            FunctionArg(None, right, false, Nil, left.span)
          ),
          window = None,
          filter = None,
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
                // Cast non-string values to ensure compatibility across database engines
                if e.dataType != DataType.StringType then
                  Cast(e, DataType.StringType, tryCast = false, e.span)
                else
                  e

          FunctionApply(
            base = NameExpr.fromString("concat"),
            args = List(
              FunctionArg(None, quote(left), false, Nil, left.span),
              FunctionArg(None, quote(right), false, Nil, right.span)
            ),
            window = None,
            filter = None,
            span = s.span
          )
        }

  /**
    * DuckDB doesn't support two-argument if expressions, so we need to populate the third argument
    * with null, following the semantics of Trino.
    */
  object RewriteIfExpr extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case f @ FunctionApply(base, args, window, filter, columnAliases, span) =>
        base match
          case i: Identifier if i.fullName.toLowerCase == "if" && args.length == 3 =>
            // Convert if(a, b, c) to IfExpr(a, b, c) if the base is "if"
            IfExpr(args(0).value, args(1).value, args(2).value, span)
          case i: Identifier if i.fullName.toLowerCase == "if" && args.length == 2 =>
            // Convert if(a,b) to IfExpr(a, b, Null) if the base is "if"
            IfExpr(args(0).value, args(1).value, NullLiteral(span), span)
          case _ =>
            f

end RewriteExpr

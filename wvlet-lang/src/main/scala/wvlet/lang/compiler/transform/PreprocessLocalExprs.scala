package wvlet.lang.compiler.transform

import wvlet.lang.compiler.{CompilationUnit, Context, ExpressionRewriteRule, Phase, RewriteRule}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.Query
import wvlet.log.{LogSupport, Logger}

/**
  * Preprocess compile-time expressions, such as backquote strings and native expressions
  */
object PreprocessLocalExpr extends Phase("preprocess-local-expr") with LogSupport:

  private def rewriteRules: List[ExpressionRewriteRule] = EvalBackquoteInterpolation :: Nil

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val newPlan = unit
      .unresolvedPlan
      .transformUp { case q: Query =>
        RewriteRule.rewriteExpr(q, rewriteRules, context)
      }
    unit.unresolvedPlan = newPlan
    unit

  object EvalBackquoteInterpolation extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case b: BackquoteInterpolatedString =>
        val evaluatedParts = b.parts.map(eval(_, context))
        if evaluatedParts.exists(_.isEmpty) then
          // There is a parts that can't be resolved yet
          b
        else
          val str = evaluatedParts.map(_.get).mkString
          BackQuotedIdentifier(str, DataType.UnknownType, b.span)

  def eval(e: Expression, ctx: Context): Option[String] =
    e match
      case i: Identifier =>
        if i.resolved then
          Some(i.unquotedValue)
        else
          None
      case s: StringPart =>
        Some(s.value)
      case s: Literal =>
        Some(s.unquotedValue)
      case other =>
        ctx.workEnv.errorLogger.warn(s"can't evaluate expression: ${other}")
        None

end PreprocessLocalExpr

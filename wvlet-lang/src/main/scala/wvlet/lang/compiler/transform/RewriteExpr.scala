package wvlet.lang.compiler.transform

import wvlet.lang.compiler.*
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.Query
import wvlet.lang.model.expr.*
import wvlet.lang.api.Span

/**
  * Wvlet's string `+` and `s"..."` skip NULL operands: a NULL operand or part is the empty string,
  * so `NULL + 'a'` is `'a'`. SQL engines disagree on `concat` with NULL arguments (DuckDB skips
  * them, Trino, Snowflake, and Hive return NULL) and Trino's `concat` rejects non-string arguments,
  * so the rules below only pass `concat` string arguments that are never NULL:
  * `coalesce(cast(x as varchar), '')`, with the cast and the coalesce left out where they are not
  * needed. On such arguments every engine computes the same value.
  *
  * The Lean model in formal/WvletFormal/Rewrite proves these rules keep the value of every
  * expression on every engine; RewriteExprConformanceTest replays its traces.
  */
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

  private def functionName(f: FunctionApply): Option[String] =
    f.base match
      case i: Identifier =>
        Some(i.fullName.toLowerCase)
      case _ =>
        None

  /**
    * True for expressions that are never NULL on any engine: string, integer, and boolean literals,
    * casts of them, `concat` of such arguments, and `coalesce` with such an argument
    */
  private def nonNull(e: Expression): Boolean =
    e match
      case _: StringLiteral | _: LongLiteral | _: BooleanLiteral =>
        true
      case c: Cast if !c.tryCast =>
        nonNull(c.expr)
      case f: FunctionApply if f.window.isEmpty && f.filter.isEmpty =>
        functionName(f) match
          case Some("concat") =>
            f.args.forall(a => nonNull(a.value))
          case Some("coalesce") =>
            f.args.exists(a => nonNull(a.value))
          case _ =>
            false
      case _ =>
        false

  private def call(name: String, args: List[Expression], span: Span): FunctionApply =
    val f = FunctionApply(
      base = NameExpr.fromString(name),
      args = args.map { a =>
        val arg = FunctionArg(None, a, false, Nil, a.span)
        arg.tpe = a.dataType
        arg
      },
      window = None,
      filter = None,
      span = span
    )
    // These rules run after the Typer, so stamp the known result type on the new node
    f.tpe = DataType.StringType
    f

  /**
    * Cast a non-string expression to varchar
    */
  private def asVarchar(e: Expression): Expression =
    if e.dataType == DataType.StringType then
      e
    else
      val c = Cast(e, DataType.StringType, tryCast = false, e.span)
      c.tpe = DataType.StringType
      c

  /**
    * The text of an expression as a string that is never NULL: NULL becomes ''
    */
  private def toText(e: Expression): Expression =
    if nonNull(e) then
      asVarchar(e)
    else
      call("coalesce", List(asVarchar(e), StringLiteral.fromString("", e.span)), e.span)

  /**
    * `a + b` with a string side -> concat(text(a), text(b))
    */
  object RewriteStringConcat extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case a @ ArithmeticBinaryExpr(BinaryExprType.Add, left, right, _)
          if left.dataType == DataType.StringType || right.dataType == DataType.StringType =>
        call("concat", List(toText(left), toText(right)), a.span)

  /**
    * s"...${x}..." -> concat('...', text(x), '...')
    */
  object RewriteStringInterpolation extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case s: InterpolatedString if s.prefix.fullName == "s" =>
        def quote(e: Expression): Expression =
          e match
            case p: StringPart =>
              StringLiteral.fromString(p.value, p.span)
            case other =>
              toText(other)

        s.parts.map(quote) match
          case Nil =>
            StringLiteral.fromString("", s.span)
          case head :: tail =>
            tail.foldLeft(head) { (acc, part) =>
              call("concat", List(acc, part), s.span)
            }

  end RewriteStringInterpolation

  /**
    * DuckDB doesn't support two-argument if expressions, so we need to populate the third argument
    * with null, following the semantics of Trino.
    */
  object RewriteIfExpr extends ExpressionRewriteRule:
    override def apply(context: Context) =
      case f @ FunctionApply(base, args, window, filter, columnAliases, span) =>
        functionName(f) match
          case Some("if") if args.length == 3 =>
            // Convert if(a, b, c) to IfExpr(a, b, c) if the base is "if"
            typed(IfExpr(args(0).value, args(1).value, args(2).value, span), f)
          case Some("if") if args.length == 2 =>
            // Convert if(a,b) to IfExpr(a, b, Null) if the base is "if"
            typed(IfExpr(args(0).value, args(1).value, NullLiteral(span), span), f)
          case _ =>
            f

    // Keep the type the Typer gave the if(...) call
    private def typed(e: IfExpr, f: FunctionApply): IfExpr =
      e.tpe = f.dataType
      e

end RewriteExpr

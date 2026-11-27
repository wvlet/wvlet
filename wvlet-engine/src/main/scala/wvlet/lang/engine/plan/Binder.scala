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

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.ext.NativeFunction
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.Relation

import java.math.BigDecimal as JBigDecimal
import scala.collection.mutable

/** An interval value: months plus microseconds */
case class Interval(months: Int, micros: Long):
  def negate: Interval          = Interval(-months, -micros)
  override def toString: String =
    if months != 0 && micros == 0 then
      s"${months} months"
    else
      s"${micros / ValueOps.MicrosPerSecond} seconds"

/**
  * Columns visible to an expression. Column indexes resolve to `offset + i`; when a name is not
  * found here, the parent scope (an enclosing query) is searched
  */
case class BindScope(schema: Schema, offset: Int = 0, parent: Option[BindScope] = None):
  def width: Int = schema.size

  def find(qualifier: Option[String], name: String): Option[(Int, Field)] =
    def matches(f: Field, exact: Boolean): Boolean =
      val nameOk =
        if exact then
          f.name == name
        else
          f.name.equalsIgnoreCase(name)
      nameOk && qualifier.forall(q => f.qualifiers.exists(_.equalsIgnoreCase(q)))
    val fields = schema.fields
    var i      = fields.indexWhere(f => matches(f, exact = true))
    if i < 0 then
      i = fields.indexWhere(f => matches(f, exact = false))
    if i >= 0 then
      Some((offset + i, fields(i)))
    else
      parent.flatMap(_.find(qualifier, name))

  def hasQualifier(q: String): Boolean =
    schema.fields.exists(_.qualifiers.exists(_.equalsIgnoreCase(q))) ||
      parent.exists(_.hasQualifier(q))

/**
  * A column that doesn't resolve in the binding scope. Planning also binds expressions to test
  * where they resolve (`Planner.bindsIn`, `isCorrelated`) and catches this, so without `stackTrace`
  * it is thrown without filling in one
  */
class ColumnNotFound(msg: String, stackTrace: Boolean = true)
    extends Exception(msg, null, true, stackTrace)

/**
  * Aggregation context: grouping keys (bound over the input) and the aggregate calls collected
  * while binding select items. Post-aggregation column layout: keys, then aggregates
  */
class AggContext(
    val input: BindScope,
    val keyNames: IndexedSeq[String],
    val keys: IndexedSeq[VExpr],
    val groupingId: Option[Int] = None
):
  val calls = mutable.ArrayBuffer.empty[AggCall]

  def addCall(c: AggCall): VExpr =
    val i =
      calls.indexWhere(_.sig == c.sig) match
        case -1 =>
          calls += c
          calls.size - 1
        case existing =>
          existing
    ColRef(keys.size + i, c.ptype, c.name)

  def keyRef(i: Int): VExpr = ColRef(i, keys(i).ptype, keyNames(i))

  def outputSchema: Schema = Schema(
    keys.indices.map(i => Field(keyNames(i), keys(i).ptype)) ++
      calls.map(c => Field(c.sig, c.ptype))
  )

object Binder:
  private val templateCache = mutable.HashMap.empty[String, Expression]

  private val PlaceholderPrefix = "__wv_p"

  val IntervalFunction = "__wv_interval"

  /** List functions that take a lambda argument, bound by `bindLambdaCall` */
  val LambdaFunctions: Set[String] = Set(
    "list_transform",
    "array_transform",
    "list_apply",
    "array_apply",
    "apply",
    "list_filter",
    "array_filter",
    "filter",
    "list_reduce",
    "array_reduce",
    "reduce",
    "zip_with"
  )

  private val IntervalPattern = """(?i)\binterval\s*\(([^()]*)\)\s*([a-z]+)""".r

  val ConcatFunction = "__wv_concat"

  // Operands of a `||` chain: placeholders, string literals, and calls without nested parens
  private val ConcatOperand =
    s"""(?:${PlaceholderPrefix}[0-9]+|'(?:[^']|'')*'|[A-Za-z_][A-Za-z_0-9]*\\([^()]*\\))"""

  private val ConcatOperandPattern = ConcatOperand.r
  private val ConcatChainPattern   = s"""${ConcatOperand}(?:\\s*\\|\\|\\s*${ConcatOperand})+""".r

  /**
    * The SQL parser has no `||` operator: rewrite chains `a || b || f(x)` over simple operands into
    * an engine-internal n-ary concat function with the same NULL semantics
    */
  def rewriteConcat(sql: String): String = ConcatChainPattern.replaceAllIn(
    sql,
    m =>
      scala
        .util
        .matching
        .Regex
        .quoteReplacement(
          ConcatOperandPattern.findAllIn(m.matched).mkString(s"${ConcatFunction}(", ", ", ")")
        )
  )

  private val ExtractPattern = """(?i)\bextract\s*\(\s*([a-z_]+)\s+from\s+""".r

  /** IN-list items with list arguments expanded in place */
  private def flattenList(list: List[Expression]): List[Expression] = list.flatMap { x =>
    unwrapArg(x) match
      case l: ListExpr =>
        flattenList(l.exprs)
      case other =>
        List(other)
  }

  private def unwrapArg(e: Expression): Expression =
    e match
      case f: FunctionArg =>
        unwrapArg(f.value)
      case t: TypedExpression =>
        unwrapArg(t.child)
      case other =>
        other

  /** Parse a `sql"..."` template into an expression whose `${...}` parts are placeholder names */
  def parseTemplate(parts: List[Expression]): (Expression, Map[String, Expression]) =
    val sb   = StringBuilder()
    val args = mutable.LinkedHashMap.empty[String, Expression]
    parts.foreach {
      case s: StringPart =>
        sb.append(s.value)
      case e =>
        unwrapArg(e) match
          // A field name of extract(<field> from ...) must be inlined as a keyword
          case lit: StringLiteral if sb.toString.trim.toLowerCase.endsWith("extract(") =>
            sb.append(lit.unquotedValue)
          case _ =>
            val name = s"${PlaceholderPrefix}${args.size}"
            args(name) = e
            sb.append(name)
    }
    // extract(<field> from x) becomes date_part('<field>', x) so that engine-specific fields (e.g.,
    // isodow) need no SQL-parser support
    // INTERVAL (n) unit becomes an engine-internal function for non-literal amounts
    var sql = IntervalPattern.replaceAllIn(
      ExtractPattern.replaceAllIn(sb.toString, m => s"date_part('${m.group(1)}', "),
      m => s"${IntervalFunction}(${m.group(1)}, '${m.group(2)}')"
    )
    sql = rewriteConcat(sql)
    val parsed = templateCache.synchronized {
      templateCache.getOrElseUpdate(
        sql, {
          val unit = CompilationUnit.fromSqlString(sql)
          SqlParser(unit).expression()
        }
      )
    }
    (parsed, args.toMap)

  end parseTemplate

  /**
    * Replace `sql"..."` templates with the parsed SQL expression, arguments substituted. `x in
    * (${v})` expands list arguments in place and becomes an IN-subquery for a subquery argument
    */
  def expandTemplates(e: Expression): Expression = e.transformUpExpression(expandRule)

  /**
    * A plan with the templates in its expressions expanded, as [[expandTemplates]] expands a
    * subquery's plan: equal to the copy of `p` in a condition expanded around it
    */
  def expandTemplatesIn(p: LogicalPlan): LogicalPlan = p.transformUpExpressions(expandRule)

  private lazy val expandRule: PartialFunction[Expression, Expression] = {
    case s: InterpolatedString if isSqlTemplate(s) =>
      val (parsed, args) = parseTemplate(s.parts)
      val expandedArgs   = args.map((k, v) => k -> expandTemplates(unwrapArg(v)))
      substitute(parsed, expandedArgs)
    case t: TypedExpression =>
      t.child
    case in: In =>
      val items = flattenList(in.list)
      items match
        case List(s: SubQueryExpression) =>
          InRelation(in.a, s.query, in.span)
        case _ =>
          in.copy(list = items)
    case in: NotIn =>
      val items = flattenList(in.list)
      items match
        case List(s: SubQueryExpression) =>
          NotInRelation(in.a, s.query, in.span)
        case _ =>
          in.copy(list = items)
  }

  private def substitute(e: Expression, args: Map[String, Expression]): Expression =
    def listItems(list: List[Expression]): List[Expression] = flattenList(
      list.map {
        case i: Identifier if args.contains(i.leafName) =>
          args(i.leafName)
        case other =>
          other
      }
    )
    def subqueryOf(items: List[Expression]): Option[Relation] =
      items.map(unwrapArg) match
        case List(s: SubQueryExpression) =>
          Some(s.query)
        case _ =>
          None
    e.transformUpExpression {
      case i: Identifier if args.contains(i.leafName) =>
        args(i.leafName)
      case in: In =>
        val items = listItems(in.list)
        subqueryOf(items) match
          case Some(q) =>
            InRelation(in.a, q, in.span)
          case None =>
            in.copy(list = items)
      case in: NotIn =>
        val items = listItems(in.list)
        subqueryOf(items) match
          case Some(q) =>
            NotInRelation(in.a, q, in.span)
          case None =>
            in.copy(list = items)
    } match
      case i: Identifier if args.contains(i.leafName) =>
        args(i.leafName)
      case other =>
        other

  end substitute

  def isSqlTemplate(e: InterpolatedString): Boolean =
    e.prefix.leafName == "sql" || e.prefix.leafName == "expr"

  /** The element type of lists produced by a constant expression, or Any */
  def listElementType(e: VExpr): PType =
    if !e.isConstant && !e.isInstanceOf[Const] then
      PType.AnyT
    else
      e.eval(Batch.ofRows(1)).get(0) match
        case s: Seq[?] =>
          elementTypeOf(s)
        case _ =>
          PType.AnyT

  /** The common type of list element values: Int32 for an empty list, Any for mixed kinds */
  def elementTypeOf(s: Seq[?]): PType =
    val ts =
      s.filter(_ != null)
        .map {
          case _: Int =>
            PType.Int32
          case _: Long =>
            PType.Int64
          case _: Double =>
            PType.Float64
          case _: String =>
            PType.Str
          case _: Boolean =>
            PType.Bool
          case _ =>
            PType.AnyT
        }
        .distinct
    ts match
      case Seq() =>
        PType.Int32
      case Seq(t) =>
        t
      case many if many.forall(_.isNumeric) =>
        many.reduce(PType.common)
      case _ =>
        PType.AnyT

  /** True when the expression has an aggregate call outside subqueries and window functions */
  def containsAggregate(e: Expression): Boolean =
    e match
      case f: FunctionApply
          if f.window.isEmpty &&
            (Aggregates.isAggregate(functionName(f)) || functionName(f) == "grouping") =>
        // grouping() reads the grouping id of aggregated rows
        true
      case f: FunctionApply if f.window.isDefined =>
        // Window arguments may still contain aggregates (e.g. sum(sum(x)) over ...)
        f.args.exists(a => containsAggregate(a.value))
      case w: WindowApply =>
        w.base match
          case f: FunctionApply =>
            f.args.exists(a => containsAggregate(a.value))
          case _ =>
            false
      case s: InterpolatedString if isSqlTemplate(s) =>
        val (parsed, args) = parseTemplate(s.parts)
        containsAggregate(parsed) || args.values.exists(containsAggregate)
      case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
        false
      case a: AllColumns =>
        false
      case other =>
        other.children.exists(containsAggregate)

  def containsWindow(e: Expression): Boolean =
    e match
      case f: FunctionApply if f.window.isDefined =>
        true
      case _: WindowApply =>
        true
      case s: InterpolatedString if isSqlTemplate(s) =>
        val (parsed, args) = parseTemplate(s.parts)
        containsWindow(parsed) || args.values.exists(containsWindow)
      case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
        false
      case other =>
        other.children.exists(containsWindow)

  def functionName(f: FunctionApply): String =
    f.base match
      case i: Identifier =>
        i.leafName.toLowerCase
      case d: DotRef =>
        d.name.leafName.toLowerCase
      case other =>
        other.toString.toLowerCase

  def intervalOf(i: IntervalLiteral): Interval =
    val raw = i.value.trim
    // The value may carry its own unit: interval '3 days'
    val parts       = raw.split("\\s+")
    val (num, unit) =
      if parts.length >= 2 then
        (parts(0), parts(1))
      else
        (raw, i.startField.name)
    val n =
      try
        num.toDouble
      catch
        case _: NumberFormatException =>
          throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid interval: ${i.value}")
    val sign =
      if i.sign == Sign.Negative then
        -1
      else
        1
    val v = intervalOf(unit, n)
    if sign < 0 then
      v.negate
    else
      v

  end intervalOf

  /** An interval of `n` units */
  def intervalOf(unit: String, n: Double): Interval =
    unit.toLowerCase.stripSuffix("s") match
      case "year" =>
        Interval((n * 12).toInt, 0)
      case "quarter" =>
        Interval((n * 3).toInt, 0)
      case "month" =>
        Interval(n.toInt, 0)
      case "week" =>
        Interval(0, (n * 7 * ValueOps.MicrosPerDay).toLong)
      case "day" =>
        Interval(0, (n * ValueOps.MicrosPerDay).toLong)
      case "hour" =>
        Interval(0, (n * 3600 * ValueOps.MicrosPerSecond).toLong)
      case "minute" =>
        Interval(0, (n * 60 * ValueOps.MicrosPerSecond).toLong)
      case "second" =>
        Interval(0, (n * ValueOps.MicrosPerSecond).toLong)
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unsupported interval unit: ${other}")

end Binder

/**
  * Binds wvlet expressions to vectorized expressions over a [[BindScope]].
  *
  * @param subst
  *   placeholder names of a `sql"..."` template being bound, mapped to their argument expressions
  * @param agg
  *   when set, bind in post-aggregation mode: aggregate calls are collected into the context and
  *   other references must be grouping keys
  */
class Binder(
    planner: Planner,
    val scope: BindScope,
    agg: Option[AggContext] = None,
    subst: Map[String, Expression] = Map.empty
):
  import Binder.*

  def plannerRef: Planner = planner

  /** The aggregation context when binding over aggregated rows */
  def aggContext: Option[AggContext] = agg

  private def withSubst(m: Map[String, Expression]): Binder = Binder(
    planner,
    scope,
    agg,
    subst ++ m
  )

  /** A binder over the aggregation input, for aggregate arguments and grouping keys */
  private def inputBinder: Binder =
    agg match
      case Some(a) =>
        Binder(planner, a.input, None, subst)
      case None =>
        this

  private def error(msg: String, e: Expression): Nothing =
    throw StatusCode.INVALID_ARGUMENT.newException(msg)

  def bind(e: Expression): VExpr =
    agg match
      case Some(a) =>
        bindInAgg(e, a)
      case None =>
        bindExpr(e)

  // --- aggregation mode -------------------------------------------------------------------

  private def bindInAgg(e: Expression, a: AggContext): VExpr =
    e match
      case f: FunctionApply if f.window.isEmpty && functionName(f) == "grouping" =>
        bindGrouping(f, a)
      case f: FunctionApply if f.window.isEmpty && Aggregates.isAggregate(functionName(f)) =>
        a.addCall(aggCall(f))
      case s: InterpolatedString if isSqlTemplate(s) =>
        bindInAgg(expandTemplates(s), a)
      case t: TypedExpression =>
        bindInAgg(t.child, a)
      case p: ParenthesizedExpression =>
        bindInAgg(p.child, a)
      case s: SingleColumn =>
        bindInAgg(s.expr, a)
      case al: Alias =>
        bindInAgg(al.expr, a)
      case g: GroupingKey =>
        bindInAgg(g.child, a)
      case i: Identifier if subst.contains(i.leafName) =>
        bindInAgg(subst(i.leafName), a)
      case _ =>
        // A grouping key, referenced by name or by the same expression
        val byName: Option[Int] =
          e match
            case i: Identifier =>
              val n = a.keyNames.indexWhere(_.equalsIgnoreCase(i.leafName))
              Option.when(n >= 0)(n).filter(k => keyNameIsUnambiguous(a, k, i.leafName))
            case _ =>
              None
        byName match
          case Some(k) =>
            a.keyRef(k)
          case None =>
            val asInput: Option[VExpr] =
              if containsAggregate(e) || containsWindow(e) then
                None
              else
                try
                  Some(inputBinder.bindExpr(e))
                catch
                  case _: ColumnNotFound =>
                    None
            asInput.flatMap(x => matchKey(a, x)) match
              case Some(k) =>
                a.keyRef(k)
              case None =>
                asInput match
                  case Some(c) if c.isConstant =>
                    c
                  case Some(c: ColRef) =>
                    // A non-key column: take an arbitrary value of the group (lenient, like
                    // DuckDB's any_value)
                    a.addCall(
                      AggCall("arbitrary", Seq(c), distinct = false, filter = None, c.ptype)
                    )
                  case _ =>
                    bindExpr(e)

  /**
    * grouping(k1, ..., kn) over grouping sets: a bit per argument, most significant first, set when
    * the row does not group by that key
    */
  private def bindGrouping(f: FunctionApply, a: AggContext): VExpr =
    val gid = a
      .groupingId
      .getOrElse(error("grouping() needs GROUP BY ROLLUP, CUBE, or GROUPING SETS", f))
    val bits =
      f.args
        .map { arg =>
          bindInAgg(arg.value, a) match
            case ColRef(i, _, _) if i < gid =>
              i
            case _ =>
              error(s"grouping() argument is not a grouping key: ${arg.value}", f)
        }
        .toIndexedSeq
    RowFn(
      "grouping",
      Seq(a.keyRef(gid)),
      PType.Int64,
      { v =>
        val mask = v(0).asInstanceOf[Long]
        bits.foldLeft(0L)((acc, k) => (acc << 1) | ((mask >> k) & 1L))
      }
    )

  private def keyNameIsUnambiguous(a: AggContext, k: Int, name: String): Boolean = true

  private def matchKey(a: AggContext, x: VExpr): Option[Int] =
    val i = a.keys.indexWhere(_.sig == x.sig)
    Option.when(i >= 0)(i)

  /** Bind an aggregate function call (arguments over the aggregation input) */
  def aggCall(f: FunctionApply): AggCall =
    val name  = functionName(f)
    val input = inputBinder
    val args0 = f
      .args
      .filter {
        case FunctionArg(_, _: AllColumns, _, _, _) =>
          false
        case FunctionArg(_, w: Wildcard, _, _, _) =>
          false
        case _ =>
          true
      }
    val distinct = f.args.exists(_.isDistinct)
    val args     = args0.map(a => input.bindExpr(a.value))
    val fname    =
      if (name == "count") && args.isEmpty then
        "count_star"
      else
        name
    val filter = f.filter.map(x => input.bindExpr(x))
    // ORDER BY inside the call (array_agg(x ORDER BY y)): the keys follow the values in args
    val keys = f.args.flatMap(_.orderBy).map(s => planner.sortKey(s, input.bindExpr(s.sortKey)))
    // Some aggregates take constant arguments after the value (quantiles, string_agg separator)
    AggCall(
      fname,
      args ++ keys.map(_.expr),
      distinct,
      filter,
      Aggregates.resultType(fname, args.map(_.ptype)),
      keys.map(k => SortDir(k.ascending, k.nullsFirst))
    )

  end aggCall

  // --- plain expressions ------------------------------------------------------------------

  private def column(qualifier: Option[String], name: String, e: Expression): VExpr =
    scope.find(qualifier, name) match
      case Some((idx, f)) =>
        ColRef(idx, f.ptype, f.name)
      case None =>
        throw ColumnNotFound(
          s"Column not found: ${qualifier
              .map(q => s"${q}.")
              .getOrElse("")}${name} (available: ${scope.schema.names.mkString(", ")})",
          stackTrace = !planner.enabled(Optimization.PlanningMemo)
        )

  private def castTo(e: VExpr, t: PType): VExpr =
    if e.ptype == t then
      e
    else
      e match
        case Const(v, from) =>
          Const(ValueOps.cast(v, from, t), t)
        case _ =>
          CastExpr(e, t)

  /**
    * Concatenate argument values as strings, with NULL as an empty string. Each value is formatted
    * by its argument type, so integers stay integers on Scala.js.
    */
  private def concatStrings(args: Seq[VExpr]): Array[Any] => String =
    val types = args.map(_.ptype).toArray
    a =>
      val sb = StringBuilder()
      var i  = 0
      while i < a.length do
        val x = a(i)
        if x != null then
          sb.append(ValueOps.toStr(x, types(i)))
        i += 1
      sb.toString

  private val foldBoundConstants = planner.enabled(Optimization.FoldBoundConstants)
  private val unwrapCasts        = planner.enabled(Optimization.UnwrapCasts)
  private val roundLiterals      = planner.enabled(Optimization.RoundUnwrappedLiterals)

  /** Bind an expression, folded by [[BoundExprSimplifier]] when `fold_bound_constants` is on */
  def bindExpr(e: Expression): VExpr =
    val x = bindPlain(e)
    if foldBoundConstants then
      BoundExprSimplifier.fold(x)
    else
      x

  private def bindPlain(e: Expression): VExpr =
    e match
      case i: Identifier if subst.contains(i.leafName) =>
        bind(subst(i.leafName))
      case p: ParenthesizedExpression =>
        bind(p.child)
      case t: TypedExpression =>
        bind(t.child)
      case a: SingleColumn =>
        bind(a.expr)
      case a: Alias =>
        bind(a.expr)
      case m: MultiSourceColumn =>
        val bound = m.inputs.map(bindExpr)
        Coalesce(bound, bound.map(_.ptype).reduce(PType.common))
      case r: ResolvedIdentifier =>
        column(None, r.leafName, e)
      case i: Identifier =>
        i.leafName match
          case "true" =>
            Const(true, PType.Bool)
          case "false" =>
            Const(false, PType.Bool)
          case _ =>
            column(None, i.leafName, e)
      case d: DotRef =>
        bindDotRef(d)
      case _: NullLiteral =>
        Const(null, PType.Null)
      case _: TrueLiteral =>
        Const(true, PType.Bool)
      case _: FalseLiteral =>
        Const(false, PType.Bool)
      case l: LongLiteral =>
        if l.value >= Int.MinValue && l.value <= Int.MaxValue then
          Const(l.value.toInt, PType.Int32)
        else
          Const(l.value, PType.Int64)
      case d: DoubleLiteral =>
        Const(d.value, PType.Float64)
      case d: DecimalLiteral =>
        val v = JBigDecimal(d.value)
        Const(v, PType.decimal(math.max(v.precision(), v.scale()), v.scale()))
      case s: StringLiteral =>
        Const(s.unquotedValue, PType.Str)
      case s: StringPart =>
        Const(s.value, PType.Str)
      case g: GenericLiteral =>
        val t = PType.fromDataType(g.literalType)
        Const(ValueOps.cast(g.value.unquotedValue, PType.Str, t), t)
      case t: TimestampLiteral =>
        Const(ValueOps.parseTimestamp(t.value), PType.Timestamp)
      case i: IntervalLiteral =>
        Const(intervalOf(i), PType.AnyT)
      case j: JsonLiteral =>
        Const(j.value, PType.Str)
      case c: Cast =>
        val child = bind(c.expr)
        val t     = PType.fromDataType(c.castType)
        child match
          case Const(v, from) if !c.tryCast =>
            Const(ValueOps.cast(v, from, t), t)
          case _ =>
            if child.ptype == t then
              child
            else
              CastExpr(child, t, c.tryCast)
      case a: ArithmeticBinaryExpr =>
        bindArith(a.exprType, bind(a.left), bind(a.right))
      case u: ArithmeticUnaryExpr =>
        val child = bind(u.child)
        u.sign match
          case Sign.Negative =>
            child match
              case Const(v: Interval, t) =>
                Const(v.negate, t)
              case Const(v, t) if v != null =>
                Const(Kernels.negate(Vec.constant(t, v, 1)).get(0), t)
              case _ =>
                Negate(child)
          case _ =>
            child
      case n: Not =>
        NotExpr(bind(n.child))
      case a: And =>
        AndExpr(bind(a.left), bind(a.right))
      case o: Or =>
        OrExpr(bind(o.left), bind(o.right))
      // Wvlet's `x = null` and `x != null` are null checks
      case Eq(l, _: NullLiteral, _) =>
        IsNullExpr(bind(l), negate = false)
      case NotEq(l, _: NullLiteral, _) =>
        IsNullExpr(bind(l), negate = true)
      case c: Eq =>
        compare(CmpOp.Eq, c.left, c.right)
      case c: NotEq =>
        compare(CmpOp.Ne, c.left, c.right)
      case c: LessThan =>
        compare(CmpOp.Lt, c.left, c.right)
      case c: LessThanOrEq =>
        compare(CmpOp.Le, c.left, c.right)
      case c: GreaterThan =>
        compare(CmpOp.Gt, c.left, c.right)
      case c: GreaterThanOrEq =>
        compare(CmpOp.Ge, c.left, c.right)
      case b: Between =>
        AndExpr(compare(CmpOp.Ge, b.e, b.a), compare(CmpOp.Le, b.e, b.b))
      case b: NotBetween =>
        NotExpr(AndExpr(compare(CmpOp.Ge, b.e, b.a), compare(CmpOp.Le, b.e, b.b)))
      case d: DistinctFrom =>
        val (l, r) = unifyPair(bind(d.left), bind(d.right))
        NullSafeDistinct(l, r, negate = false)
      case d: NotDistinctFrom =>
        val (l, r) = unifyPair(bind(d.left), bind(d.right))
        NullSafeDistinct(l, r, negate = true)
      case n: IsNull =>
        IsNullExpr(bind(n.child), negate = false)
      case n: IsNotNull =>
        IsNullExpr(bind(n.child), negate = true)
      case in: In =>
        bindIn(in.a, in.list, negate = false)
      case in: NotIn =>
        bindIn(in.a, in.list, negate = true)
      case t: TupleIn =>
        bindTupleIn(t.tuple, t.list, negate = false)
      case t: TupleNotIn =>
        bindTupleIn(t.tuple, t.list, negate = true)
      case t: TupleInRelation =>
        planner.bindTupleInSubquery(this, tupleElements(t.tuple), t.in, negate = false)
      case t: TupleNotInRelation =>
        planner.bindTupleInSubquery(this, tupleElements(t.tuple), t.in, negate = true)
      case in: InRelation =>
        planner.bindInSubquery(this, in.a, in.in, negate = false)
      case in: NotInRelation =>
        planner.bindInSubquery(this, in.a, in.in, negate = true)
      case ex: Exists =>
        planner.bindExists(this, ex)
      case s: SubQueryExpression =>
        planner.bindScalarSubquery(this, s.query)
      case l: Like =>
        bindLike(l.left, l.right, l.escape, negate = false)
      case l: NotLike =>
        bindLike(l.left, l.right, l.escape, negate = true)
      case r: RLike =>
        ScalarFunctions.lookup("regexp_matches", Seq(bind(r.left), bind(r.right))).get
      case r: NotRLike =>
        NotExpr(ScalarFunctions.lookup("regexp_matches", Seq(bind(r.left), bind(r.right))).get)
      case c: Contains =>
        ScalarFunctions.lookup("contains", Seq(bind(c.left), bind(c.right))).get
      case i: IfExpr =>
        val t = PType.common(bind(i.onTrue).ptype, bind(i.onFalse).ptype)
        CaseWhen(Seq(bind(i.cond) -> bind(i.onTrue)), Some(bind(i.onFalse)), resultType(t))
      case c: CaseExpr =>
        bindCase(c)
      case s: InterpolatedString if isSqlTemplate(s) =>
        bind(expandTemplates(s))
      case g: GroupingKey =>
        bind(g.child)
      case f: FunctionArg =>
        bind(f.value)
      case s: InterpolatedString =>
        // A string interpolation s"...": concatenation with NULL as an empty string
        val parts = s.parts.map(bindExpr)
        RowFn("string_interpolation", parts, PType.Str, concatStrings(parts), nullSafe = true)
      case f: FunctionApply if f.window.isDefined =>
        planner.windowRef(this, f)
      case w: WindowApply =>
        planner.windowRef(this, w)
      case f: FunctionApply =>
        bindFunction(f)
      case a: ArrayConstructor =>
        val vs = a.values.map(bindExpr)
        RowFn("array", vs, PType.AnyT, x => x.toList, nullSafe = true)
      case l: ListExpr =>
        val vs = l.exprs.map(bindExpr)
        RowFn("list", vs, PType.AnyT, x => x.toList, nullSafe = true)
      case a: ArrayAccess =>
        val arr = bind(a.arrayExpr)
        val idx = bind(a.index)
        RowFn(
          "array_access",
          Seq(arr, idx),
          PType.AnyT,
          x =>
            x(0) match
              case s: Seq[?] =>
                val i = ValueOps.toLong(x(1)).toInt
                if i >= 1 && i <= s.size then
                  s(i - 1)
                else
                  null
              case m: Map[?, ?] =>
                m.asInstanceOf[Map[Any, Any]].getOrElse(x(1), null)
              case _ =>
                null
        )
      case s: StructValue =>
        val names = s.fields.map(_.name)
        val vs    = s.fields.map(f => bind(f.value))
        RowFn(
          "struct",
          vs,
          PType.AnyT,
          x => scala.collection.immutable.ListMap.from(names.zip(x)),
          nullSafe = true
        )
      case m: MapValue =>
        val ks = m
          .entries
          .map { e =>
            e.key match
              // map {"a": 1} keys are strings
              case d: DoubleQuotedIdentifier =>
                Const(d.unquotedValue, PType.Str)
              case k =>
                bind(k)
          }
        val vs = m.entries.map(e => bind(e.value))
        RowFn(
          "map",
          ks ++ vs,
          PType.AnyT,
          x =>
            val n = ks.size
            scala.collection.immutable.ListMap.from((0 until n).map(i => x(i) -> x(n + i)))
          ,
          nullSafe = true
        )
      case x: Extract =>
        val child = bind(x.expr)
        val t     = child.ptype
        RowFn(
          s"extract_${x.interval.name}",
          Seq(child),
          PType.Int64,
          a =>
            ScalarFunctions
              .lookup(x.interval.name, Seq(Materialized(Vec.constant(t, a(0), 1))))
              .get
              .eval(Batch.ofRows(1))
              .get(0)
        )
      case _: CurrentDate =>
        Const((currentMicros() / ValueOps.MicrosPerDay).toInt, PType.Date)
      case _: CurrentTimestamp =>
        Const(currentMicros(), PType.TimestampTz)
      case _: CurrentLocalTimeStamp =>
        Const(currentMicros(), PType.Timestamp)
      case a: AtTimeZone =>
        atTimeZone(bind(a.expr), bind(a.timezone))
      case n: NativeExpression if NativeFunction.isImplemented(n.name) =>
        // Functions evaluated inside the compiler (e.g., ulid_string)
        NativeFunction.callByName(n.name) match
          case s: String =>
            Const(s, PType.Str)
          case i: Int =>
            Const(i, PType.Int32)
          case l: Long =>
            Const(l, PType.Int64)
          case d: Double =>
            Const(d, PType.Float64)
          case b: Boolean =>
            Const(b, PType.Bool)
          case other =>
            error(s"Unsupported native value: ${other}", e)
      case n: NativeExpression =>
        error(s"Native expression ${n.name} is not supported by the engine", e)
      case other =>
        error(s"Unsupported expression in the engine: ${other.nodeName} (${other})", e)

  end bindPlain

  private def currentMicros(): Long = System.currentTimeMillis() * 1000L

  /** Micros of a date, timestamp, or timestamp string value */
  private def timestampMicros(t: PType, v: Any): Long =
    t match
      case PType.Date =>
        v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay
      case PType.Str =>
        ValueOps.parseTimestamp(v.asInstanceOf[String])
      case _ =>
        ValueOps.toLong(v)

  /**
    * `x AT TIME ZONE zone`, as in DuckDB: a plain timestamp is read as wall-clock time in the zone
    * and becomes an instant (TIMESTAMPTZ); an instant becomes its wall-clock time in the zone
    */
  private def atTimeZone(x: VExpr, zone: VExpr): VExpr =
    val z = castTo(zone, PType.Str)
    x.ptype match
      case PType.TimestampTz =>
        RowFn(
          "timezone",
          Seq(x, z),
          PType.Timestamp,
          a => TimeZones.toLocal(a(0).asInstanceOf[Long], a(1).asInstanceOf[String])
        )
      case PType.Null =>
        Const(null, PType.TimestampTz)
      case _ =>
        RowFn(
          "timezone",
          Seq(castTo(x, PType.Timestamp), z),
          PType.TimestampTz,
          a => TimeZones.toInstant(a(0).asInstanceOf[Long], a(1).asInstanceOf[String])
        )

  private def resultType(t: PType): PType =
    if t == PType.Null then
      PType.Str
    else
      t

  private def bindDotRef(d: DotRef): VExpr =
    val name = d.name.leafName
    d.qualifier match
      case q: Identifier if subst.contains(q.leafName) =>
        structField(bind(subst(q.leafName)), name)
      case q: Identifier if scope.find(Some(q.leafName), name).isDefined =>
        column(Some(q.leafName), name, d)
      case q: DotRef =>
        // catalog.schema.table.column or struct.field.field
        val full = qualifiedName(d)
        val qual = full.dropRight(1)
        scope.find(Some(qual.last), name) match
          case Some((idx, f)) =>
            ColRef(idx, f.ptype, f.name)
          case None =>
            structField(bind(q), name)
      case q: Identifier =>
        // A struct column or a qualifier unknown to this scope
        scope.find(None, q.leafName) match
          case Some((idx, f)) =>
            structField(ColRef(idx, f.ptype, f.name), name)
          case None =>
            column(Some(q.leafName), name, d)
      case other =>
        structField(bind(other), name)

  private def isNamePath(e: Expression): Boolean =
    e match
      case _: Identifier =>
        true
      case d: DotRef =>
        isNamePath(d.qualifier)
      case _ =>
        false

  private def qualifiedName(e: Expression): List[String] =
    e match
      case d: DotRef =>
        qualifiedName(d.qualifier) :+ d.name.leafName
      case i: Identifier =>
        List(i.leafName)
      case _ =>
        Nil

  private def structField(e: VExpr, name: String): VExpr = RowFn(
    s"field_${name}",
    Seq(e),
    PType.AnyT,
    a =>
      a(0) match
        case m: Map[?, ?] =>
          m.asInstanceOf[Map[Any, Any]].getOrElse(name, null)
        case other =>
          null
  )

  private def unifyPair(l: VExpr, r: VExpr): (VExpr, VExpr) =
    val t = PType.common(l.ptype, r.ptype)
    (castTo(l, t), castTo(r, t))

  private def compare(op: CmpOp, left: Expression, right: Expression): VExpr =
    val l = bind(left)
    val r = bind(right)
    if l.ptype == PType.Null || r.ptype == PType.Null then
      return Const(null, PType.Bool)
    val t =
      (l.ptype, r.ptype) match
        case (PType.AnyT, _) | (_, PType.AnyT) =>
          PType.AnyT
        case (a, b) =>
          PType.common(a, b)
    if t == PType.AnyT then
      Compare(op, l, r)
    else if unwrapCasts then
      BoundExprSimplifier.unwrapCompare(op, castTo(l, t), castTo(r, t), roundLiterals)
    else
      Compare(op, castTo(l, t), castTo(r, t))

  def bindArith(op: BinaryExprType, l: VExpr, r: VExpr): VExpr =
    val aop =
      op match
        case BinaryExprType.Add =>
          ArithOp.Add
        case BinaryExprType.Subtract =>
          ArithOp.Sub
        case BinaryExprType.Multiply =>
          ArithOp.Mul
        case BinaryExprType.Divide =>
          ArithOp.Div
        case BinaryExprType.DivideInt =>
          ArithOp.IntDiv
        case BinaryExprType.Modulus =>
          ArithOp.Mod
    (l, r) match
      // date/timestamp +- interval
      case (_, Const(iv: Interval, _)) if aop == ArithOp.Add || aop == ArithOp.Sub =>
        val i =
          if aop == ArithOp.Sub then
            iv.negate
          else
            iv
        addInterval(l, i)
      case (Const(iv: Interval, _), _) if aop == ArithOp.Add =>
        addInterval(r, iv)
      case (_, RowFn("interval", _, _, _, _)) if aop == ArithOp.Add || aop == ArithOp.Sub =>
        addDynamicInterval(l, r, negate = aop == ArithOp.Sub)
      case (RowFn("interval", _, _, _, _), _) if aop == ArithOp.Add =>
        addDynamicInterval(r, l, negate = false)
      case _ =>
        (l.ptype, r.ptype) match
          case (PType.Null, _) | (_, PType.Null) =>
            Const(
              null,
              if l.ptype == PType.Null then
                r.ptype
              else
                l.ptype
            )
          case (PType.Str, _) | (_, PType.Str) if aop == ArithOp.Add =>
            // Wvlet string concatenation: NULL is an empty string
            RowFn("string_concat", Seq(l, r), PType.Str, concatStrings(Seq(l, r)), nullSafe = true)
          case (PType.Date, t) if t.isIntegral && (aop == ArithOp.Add || aop == ArithOp.Sub) =>
            val sign =
              if aop == ArithOp.Add then
                1
              else
                -1
            RowFn(
              "date_add_days",
              Seq(l, r),
              PType.Date,
              a => a(0).asInstanceOf[Int] + sign * ValueOps.toInt(a(1))
            )
          case (t, PType.Date) if t.isIntegral && aop == ArithOp.Add =>
            RowFn(
              "date_add_days",
              Seq(r, l),
              PType.Date,
              a => a(0).asInstanceOf[Int] + ValueOps.toInt(a(1))
            )
          case (PType.Date, PType.Date) if aop == ArithOp.Sub =>
            RowFn(
              "date_diff_days",
              Seq(l, r),
              PType.Int64,
              a => (a(0).asInstanceOf[Int] - a(1).asInstanceOf[Int]).toLong
            )
          case (a, b)
              if (a.isTimestamp || a == PType.Date) &&
                (b.isTimestamp || b == PType.Date) && aop == ArithOp.Sub =>
            val lt = l.ptype
            val rt = r.ptype
            RowFn(
              "timestamp_diff",
              Seq(l, r),
              PType.AnyT,
              a => Interval(0, toMicros(lt, a(0)) - toMicros(rt, a(1)))
            )
          case (a, b) if a.isNumeric && b.isNumeric =>
            Arith(aop, l, r, arithType(aop, a, b))
          case (a, b) =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Unsupported arithmetic: ${a} ${aop.symbol} ${b}")
    end match

  end bindArith

  private def toMicros(t: PType, v: Any): Long =
    t match
      case PType.Date =>
        v.asInstanceOf[Int].toLong * ValueOps.MicrosPerDay
      case _ =>
        v.asInstanceOf[Long]

  private def addInterval(base: VExpr, iv: Interval): VExpr =
    val t =
      base.ptype match
        case PType.Str =>
          PType.Timestamp
        case other =>
          other
    val b       = castTo(base, t)
    val outType =
      if t == PType.Date && iv.micros % ValueOps.MicrosPerDay != 0 then
        PType.Timestamp
      else
        t
    RowFn(
      "add_interval",
      Seq(b),
      outType,
      a =>
        t match
          case PType.Date =>
            val d = ValueOps.addMonths(a(0).asInstanceOf[Int], iv.months)
            if outType == PType.Date then
              d + (iv.micros / ValueOps.MicrosPerDay).toInt
            else
              d.toLong * ValueOps.MicrosPerDay + iv.micros
          case _ =>
            ValueOps.addMonthsToTimestamp(a(0).asInstanceOf[Long], iv.months) + iv.micros
    )

  /** base +- an interval computed per row */
  private def addDynamicInterval(base: VExpr, interval: VExpr, negate: Boolean): VExpr =
    val t =
      base.ptype match
        case PType.Date | PType.Str =>
          PType.Timestamp
        case other =>
          other
    val b = castTo(base, t)
    RowFn(
      "add_interval",
      Seq(b, interval),
      t,
      a =>
        val iv0 = a(1).asInstanceOf[Interval]
        val iv  =
          if negate then
            iv0.negate
          else
            iv0
        ValueOps.addMonthsToTimestamp(a(0).asInstanceOf[Long], iv.months) + iv.micros
    )

  private def decimalOf(t: PType): PType.Decimal =
    t match
      case d: PType.Decimal =>
        d
      case PType.Int32 =>
        PType.Decimal(10, 0)
      case _ =>
        PType.Decimal(19, 0)

  private def arithType(op: ArithOp, a: PType, b: PType): PType =
    if a == PType.Float64 || b == PType.Float64 then
      PType.Float64
    else if a.isIntegral && b.isIntegral then
      op match
        case ArithOp.Div =>
          PType.Float64
        case _ =>
          PType.Int64
    else
      // At least one decimal
      val x = decimalOf(a)
      val y = decimalOf(b)
      op match
        case ArithOp.Add | ArithOp.Sub =>
          val scale = math.max(x.scale, y.scale)
          val ints  = math.max(x.precision - x.scale, y.precision - y.scale)
          PType.decimal(ints + scale + 1, scale)
        case ArithOp.Mul =>
          PType.decimal(x.precision + y.precision, x.scale + y.scale)
        case ArithOp.Div =>
          PType.Float64
        case ArithOp.IntDiv =>
          PType.Int64
        case ArithOp.Mod =>
          PType.decimalUnion(x, y)

  private def bindIn(a: Expression, list: List[Expression], negate: Boolean): VExpr =
    // `${v}` placeholders bound to list expressions expand in place
    val items = list.flatMap {
      case i: Identifier if subst.contains(i.leafName) =>
        subst(i.leafName) match
          case l: ListExpr =>
            l.exprs
          case other =>
            List(other)
      case l: ListExpr =>
        l.exprs
      case other =>
        List(other)
    }
    items.map(unwrapArg) match
      case List(s: SubQueryExpression) =>
        // x [NOT] IN (subquery) outside a filter conjunct: the subquery's values, not a scalar
        planner.bindInSubquery(this, a, s.query, negate)
      case _ =>
        bindInItems(bind(a), items, negate)

  private def bindInItems(target: VExpr, items: List[Expression], negate: Boolean): VExpr =
    val bound = items.map(bindExpr)
    val t     = bound.map(_.ptype).foldLeft(target.ptype)(PType.common)
    val tgt   = castTo(target, t)
    if bound.forall(_.isInstanceOf[Const]) then
      val values    = bound.map(b => castTo(b, t).asInstanceOf[Const].value)
      val hasNull   = values.contains(null)
      val set       = values.filter(_ != null).toSet
      val unwrapped =
        if unwrapCasts then
          BoundExprSimplifier.unwrapInList(tgt, set, hasNull, negate)
        else
          None
      unwrapped.getOrElse(InList(tgt, set, hasNull, negate))
    else
      InExprs(tgt, bound.map(b => castTo(b, t)), negate)

  private def tupleElements(e: Expression): List[Expression] =
    unwrapArg(e) match
      case r: RowConstructor =>
        r.values
      case p: ParenthesizedExpression =>
        tupleElements(p.child)
      case other =>
        List(other)

  /** (a, b) [NOT] IN ((1, 'x'), ...): a disjunction of conjunctions, keeping SQL NULL semantics */
  private def bindTupleIn(tuple: Expression, list: List[Expression], negate: Boolean): VExpr =
    val target = tupleElements(tuple)
    list.map(unwrapArg) match
      case List(s: SubQueryExpression) =>
        planner.bindTupleInSubquery(this, target, s.query, negate)
      case items0 =>
        // `${v}` placeholders bound to lists expand in place, as in bindIn
        val items = items0.flatMap {
          case i: Identifier if subst.contains(i.leafName) =>
            subst(i.leafName) match
              case l: ListExpr =>
                l.exprs
              case other =>
                List(other)
          case l: ListExpr =>
            l.exprs
          case other =>
            List(other)
        }
        val disjuncts = items.map { item =>
          val values = tupleElements(item)
          if values.size != target.size then
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Tuple size mismatch: ${target.size} vs ${values.size}")
          target
            .zip(values)
            .map((t, v) => Eq(t, v, NoSpan): Expression)
            .reduce((a, b) => And(a, b, NoSpan))
        }
        val cond =
          disjuncts match
            case Nil =>
              Const(false, PType.Bool)
            case _ =>
              // A balanced OR tree keeps long tuple lists from building deep recursion
              def balanced(xs: IndexedSeq[Expression]): Expression =
                if xs.size == 1 then
                  xs.head
                else
                  val (l, r) = xs.splitAt(xs.size / 2)
                  Or(balanced(l), balanced(r), NoSpan)
              bind(balanced(disjuncts.toIndexedSeq))
        if negate then
          NotExpr(cond)
        else
          cond
    end match

  end bindTupleIn

  /**
    * list_transform / list_filter / list_reduce with a lambda. The lambda body is bound over its
    * parameters, typed by the list elements, and evaluated over all elements of a list at once
    * (sequentially for reduce). A constant list gives the element type at plan time; for other
    * lists (e.g. a column) the body is bound on first use from the element values, once per element
    * type
    */
  private def bindLambdaCall(
      name: String,
      f: FunctionApply,
      receiverArgs: List[Expression]
  ): VExpr =
    val isReduce =
      name match
        case "list_reduce" | "array_reduce" | "reduce" =>
          true
        case _ =>
          false
    val (listArg, lambda, initArg) =
      // The list may be the receiver of a method call: xs.list_transform(x -> x + 1). A reduce
      // may take an initial value last, as DuckDB's list_reduce(xs, f, init)
      (receiverArgs ++ f.args.map(_.value)) match
        case List(l, lam: LambdaExpr) =>
          (l, lam, None)
        case List(l, lam: LambdaExpr, init) if isReduce =>
          (l, lam, Some(init))
        case _ =>
          error(s"${name} expects a list and a lambda", f)
    val arity =
      if isReduce then
        2
      else
        1
    if lambda.args.size != arity then
      error(s"${name} expects a lambda with ${arity} parameter(s)", f)
    val list                               = bind(listArg)
    val init                               = initArg.map(bind)
    def bodyOver(types: Seq[PType]): VExpr =
      val schema = Schema(lambda.args.zip(types).map((p, t) => Field(p.leafName, t)).toIndexedSeq)
      Binder(planner, BindScope(schema)).bind(lambda.body)

    // The result type and the evaluator over one list and the initial value (null without one),
    // for lists of the given element type
    def compile(elemType: PType): (PType, (Seq[Any], Any) => Any) =
      name match
        case "list_filter" | "array_filter" | "filter" =>
          val body = bodyOver(Seq(elemType))
          (
            PType.AnyT,
            (elems, _) =>
              val mask = Kernels.trueMask(
                body.eval(Batch(Array(Vec.fromValues(elemType, elems)), elems.size))
              )
              elems.indices.filter(mask(_)).map(elems).toList
          )
        case _ if isReduce =>
          // The accumulator takes the common type of the seed (the initial value, or the first
          // element) and the body result
          val seedType = init.map(_.ptype).getOrElse(elemType)
          val accType  = PType.common(seedType, bodyOver(Seq(seedType, elemType)).ptype)
          val body     = bodyOver(Seq(accType, elemType))
          def fold(seed: Any, rest: Seq[Any]): Any =
            rest.foldLeft(seed) { (acc, x) =>
              val row = Batch(
                Array(Vec.fromValues(accType, Seq(acc)), Vec.fromValues(elemType, Seq(x))),
                1
              )
              ValueOps.cast(body.eval(row).get(0), body.ptype, accType)
            }
          (
            accType,
            (elems, initValue) =>
              if init.isDefined then
                fold(ValueOps.cast(initValue, seedType, accType), elems)
              else if elems.isEmpty then
                throw StatusCode.INVALID_ARGUMENT.newException(s"${name} of an empty list")
              else
                fold(ValueOps.cast(elems.head, elemType, accType), elems.tail)
          )
        case _ =>
          val body = bodyOver(Seq(elemType))
          (
            PType.AnyT,
            (elems, _) =>
              val out = body.eval(Batch(Array(Vec.fromValues(elemType, elems)), elems.size))
              (0 until elems.size).map(out.get).toList
          )

    def asList(v: Any): Seq[Any]   = v.asInstanceOf[Seq[Any]]
    val inputs                     = list +: init.toSeq
    def initOf(a: Array[Any]): Any =
      if init.isDefined then
        a(1)
      else
        null
    listElementType(list) match
      case PType.AnyT if !list.isConstant && !list.isInstanceOf[Const] =>
        val compiled = java.util.concurrent.ConcurrentHashMap[PType, (Seq[Any], Any) => Any]()
        RowFn(
          name,
          inputs,
          PType.AnyT,
          a =>
            val elems = asList(a(0))
            compiled.computeIfAbsent(elementTypeOf(elems), t => compile(t)._2)(elems, initOf(a))
        )
      case elemType =>
        val (ptype, eval) = compile(elemType)
        RowFn(name, inputs, ptype, a => eval(asList(a(0)), initOf(a)))
    end match

  end bindLambdaCall

  /**
    * zip_with(xs, ys, (x, y) -> ...): the lambda over the elements at the same position, with the
    * shorter list padded with NULL. As in bindLambdaCall, a list whose element type is unknown at
    * plan time (e.g. a column) has the body bound on first use, once per pair of element types
    */
  private def bindZipWith(f: FunctionApply, receiverArgs: List[Expression]): VExpr =
    val (xsArg, ysArg, lambda) = (receiverArgs ++ f.args.map(_.value)) match
      case List(xs, ys, lam: LambdaExpr) =>
        (xs, ys, lam)
      case _ =>
        error("zip_with expects two lists and a lambda", f)
    if lambda.args.size != 2 then
      error("zip_with expects a lambda with 2 parameter(s)", f)
    val xs                                                               = bind(xsArg)
    val ys                                                               = bind(ysArg)
    def compile(xType: PType, yType: PType): (Seq[Any], Seq[Any]) => Any =
      val schema = Schema(
        IndexedSeq(Field(lambda.args(0).leafName, xType), Field(lambda.args(1).leafName, yType))
      )
      val body = Binder(planner, BindScope(schema)).bind(lambda.body)
      (a, b) =>
        val n   = a.size max b.size
        val out = body.eval(
          Batch(
            Array(Vec.fromValues(xType, a.padTo(n, null)), Vec.fromValues(yType, b.padTo(n, null))),
            n
          )
        )
        (0 until n).map(out.get).toList

    def asList(v: Any): Seq[Any]               = v.asInstanceOf[Seq[Any]]
    val xStatic                                = listElementType(xs)
    val yStatic                                = listElementType(ys)
    def isDynamic(e: VExpr, t: PType): Boolean =
      t == PType.AnyT && !e.isConstant && !e.isInstanceOf[Const]
    val xDynamic = isDynamic(xs, xStatic)
    val yDynamic = isDynamic(ys, yStatic)
    if xDynamic || yDynamic then
      val compiled = java
        .util
        .concurrent
        .ConcurrentHashMap[(PType, PType), (Seq[Any], Seq[Any]) => Any]()
      RowFn(
        "zip_with",
        Seq(xs, ys),
        PType.AnyT,
        a =>
          val l1    = asList(a(0))
          val l2    = asList(a(1))
          val types = (
            if xDynamic then
              elementTypeOf(l1)
            else
              xStatic
            ,
            if yDynamic then
              elementTypeOf(l2)
            else
              yStatic
          )
          compiled.computeIfAbsent(types, t => compile(t._1, t._2))(l1, l2)
      )
    else
      val eval = compile(xStatic, yStatic)
      RowFn("zip_with", Seq(xs, ys), PType.AnyT, a => eval(asList(a(0)), asList(a(1))))
    end if

  end bindZipWith

  private def bindLike(
      left: Expression,
      right: Expression,
      escape: Option[Expression],
      negate: Boolean
  ): VExpr =
    val l = bind(left)
    bind(right) match
      case Const(p: String, _) =>
        val esc = escape
          .map(bindExpr)
          .collect {
            case Const(e: String, _) if e.nonEmpty =>
              e.charAt(0)
          }
        LikeExpr(l, p, negate, escape = esc)
      case Const(null, _) =>
        Const(null, PType.Bool)
      case r =>
        RowFn(
          "like",
          Seq(l, r),
          PType.Bool,
          a =>
            LikeMatcher.compile(ValueOps.toStr(a(1)), false, None)(ValueOps.toStr(a(0))) != negate
        )

  private def bindCase(c: CaseExpr): VExpr =
    val whens = c
      .whenClauses
      .map { w =>
        val cond =
          c.target match
            case Some(t) =>
              compare(CmpOp.Eq, t, w.condition)
            case None =>
              bind(w.condition)
        cond -> bind(w.result)
      }
    val elseExpr = c.elseClause.map(bindExpr)
    val t        = (whens.map(_._2.ptype) ++ elseExpr.map(_.ptype)).reduce(PType.common)
    CaseWhen(whens, elseExpr, resultType(t))

  private def bindFunction(f: FunctionApply): VExpr =
    val name = functionName(f)
    if Aggregates.isAggregate(name) then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"Aggregate function ${name} is not allowed here")
    // Method-style call on a receiver: x.fn(args) when fn is not a qualified column
    val receiverArgs: List[Expression] =
      f.base match
        case d: DotRef
            if isNamePath(d.qualifier) &&
              ScalarFunctions.isDefined(qualifiedName(d).mkString(".")) =>
          // A function with a dotted name
          Nil
        case d: DotRef =>
          List(d.qualifier)
        case _ =>
          Nil
    name match
      case "cast" | "try_cast" =>
        error(s"Unexpected cast function call", f)
      case _ =>
    // bind (not bindExpr): arguments may be aggregates in aggregation mode, e.g., x.sum.round(1)
    lazy val args = (receiverArgs ++ f.args.map(_.value)).map(bind)
    name match
      case "count_star" =>
        error("count(*) is only allowed in aggregations", f)
      case "date_sub" if args.size == 3 =>
        // DuckDB's date_sub(part, start, end): the number of complete parts between the two
        val part =
          args.head match
            case Const(v, _) =>
              v.toString
            case _ =>
              error("date_sub requires a constant part", f)
        val t1 = args(1).ptype
        val t2 = args(2).ptype
        RowFn(
          "date_sub",
          Seq(args(1), args(2)),
          PType.Int64,
          a => DateTimeFunctions.dateSub(part, timestampMicros(t1, a(0)), timestampMicros(t2, a(1)))
        )
      case "date_add" if args.size == 3 =>
        // Trino-style date_add(unit, n, date)
        val unit =
          args.head match
            case Const(v, _) =>
              v.toString
            case _ =>
              error("date_add requires a constant unit", f)
        val t = args(2).ptype
        RowFn(
          "date_add",
          Seq(args(1), args(2)),
          t,
          a => ScalarFunctions.addInterval(t, a(1), unit, ValueOps.toLong(a(0)))
        )
      case IntervalFunction =>
        val unit =
          args(1) match
            case Const(v, _) =>
              v.toString
            case _ =>
              error("interval requires a constant unit", f)
        // Validate the unit at bind time, also for per-row amounts
        intervalOf(unit, 0)
        args(0) match
          case Const(null, _) =>
            Const(null, PType.Null)
          case Const(v, t) =>
            Const(
              intervalOf(unit, ValueOps.toDouble(ValueOps.cast(v, t, PType.Float64))),
              PType.AnyT
            )
          case n =>
            RowFn(
              "interval",
              Seq(castTo(n, PType.Float64)),
              PType.AnyT,
              a => intervalOf(unit, a(0).asInstanceOf[Double])
            )
      case "zip_with" if f.args.exists(_.value.isInstanceOf[LambdaExpr]) =>
        bindZipWith(f, receiverArgs)
      case _ if LambdaFunctions.contains(name) && f.args.exists(_.value.isInstanceOf[LambdaExpr]) =>
        bindLambdaCall(name, f, receiverArgs)
      case "date_add" if args.size == 2 =>
        args(1) match
          case Const(iv: Interval, _) =>
            addInterval(args(0), iv)
          case _ =>
            bindArith(BinaryExprType.Add, args(0), args(1))
      case _ =>
        ScalarFunctions.lookup(name, args) match
          case Some(x) =>
            if args.nonEmpty && args.forall(_.isConstant) && x.isInstanceOf[RowFn] &&
              !ScalarFunctions.volatileFunctions.contains(name)
            then
              // Fold constant calls
              val v = x.eval(Batch.ofRows(1))
              Const(v.get(0), x.ptype)
            else
              x
          case None =>
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(s"Unsupported function in the engine: ${name}")
    end match

  end bindFunction

end Binder

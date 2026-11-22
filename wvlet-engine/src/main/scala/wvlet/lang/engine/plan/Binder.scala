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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
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

class ColumnNotFound(msg: String) extends Exception(msg)

/**
  * Aggregation context: grouping keys (bound over the input) and the aggregate calls collected
  * while binding select items. Post-aggregation column layout: keys, then aggregates
  */
class AggContext(
    val input: BindScope,
    val keyNames: IndexedSeq[String],
    val keys: IndexedSeq[VExpr]
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
    val sql    = sb.toString
    val parsed = templateCache.synchronized {
      templateCache.getOrElseUpdate(
        sql, {
          val unit = CompilationUnit.fromSqlString(sql)
          SqlParser(unit).expression()
        }
      )
    }
    (parsed, args.toMap)

  /**
    * Replace `sql"..."` templates with the parsed SQL expression, arguments substituted. `x in
    * (${v})` expands list arguments in place and becomes an IN-subquery for a subquery argument
    */
  def expandTemplates(e: Expression): Expression = e.transformUpExpression {
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

  /** True when the expression has an aggregate call outside subqueries and window functions */
  def containsAggregate(e: Expression): Boolean =
    e match
      case f: FunctionApply if f.window.isEmpty && Aggregates.isAggregate(functionName(f)) =>
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
    val v =
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
    if sign < 0 then
      v.negate
    else
      v

  end intervalOf

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
              if containsAggregate(e) then
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
    // Some aggregates take constant arguments after the value (quantiles, string_agg separator)
    AggCall(fname, args, distinct, filter, Aggregates.resultType(fname, args.map(_.ptype)))

  // --- plain expressions ------------------------------------------------------------------

  private def column(qualifier: Option[String], name: String, e: Expression): VExpr =
    scope.find(qualifier, name) match
      case Some((idx, f)) =>
        ColRef(idx, f.ptype, f.name)
      case None =>
        throw ColumnNotFound(
          s"Column not found: ${qualifier
              .map(q => s"${q}.")
              .getOrElse("")}${name} (available: ${scope.schema.names.mkString(", ")})"
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

  def bindExpr(e: Expression): VExpr =
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
        RowFn(
          "string_interpolation",
          parts,
          PType.Str,
          a =>
            a.iterator
              .map(x =>
                if x == null then
                  ""
                else
                  ValueOps.toStr(x)
              )
              .mkString,
          nullSafe = true
        )
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
        val ks = m.entries.map(e => bind(e.key))
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
      case _: CurrentTimestamp | _: CurrentLocalTimeStamp =>
        Const(currentMicros(), PType.Timestamp)
      case a: AtTimeZone =>
        bind(a.expr)
      case n: NativeExpression =>
        error(s"Native expression ${n.name} is not supported by the engine", e)
      case other =>
        error(s"Unsupported expression in the engine: ${other.nodeName} (${other})", e)

  end bindExpr

  private def currentMicros(): Long = System.currentTimeMillis() * 1000L

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
            RowFn(
              "string_concat",
              Seq(l, r),
              PType.Str,
              a =>
                a.iterator
                  .map(x =>
                    if x == null then
                      ""
                    else
                      ValueOps.toStr(x)
                  )
                  .mkString,
              nullSafe = true
            )
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
          case (PType.Timestamp | PType.Date, PType.Timestamp | PType.Date) if aop == ArithOp.Sub =>
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
    val target = bind(a)
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
    val bound = items.map(bindExpr)
    val t     = bound.map(_.ptype).foldLeft(target.ptype)(PType.common)
    val tgt   = castTo(target, t)
    if bound.forall(_.isInstanceOf[Const]) then
      val values  = bound.map(b => castTo(b, t).asInstanceOf[Const].value)
      val hasNull = values.contains(null)
      InList(tgt, values.filter(_ != null).toSet, hasNull, negate)
    else
      InExprs(tgt, bound.map(b => castTo(b, t)), negate)

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
        case d: DotRef if !ScalarFunctions.isDefined(qualifiedName(d).mkString(".")) =>
          List(d.qualifier)
        case _ =>
          Nil
    name match
      case "cast" | "try_cast" =>
        error(s"Unexpected cast function call", f)
      case _ =>
    val args = (receiverArgs ++ f.args.map(_.value)).map(bindExpr)
    name match
      case "count_star" =>
        error("count(*) is only allowed in aggregations", f)
      case "date_add" | "date_sub" if args.size == 3 =>
        // Trino-style date_add(unit, n, date)
        val unit =
          args.head match
            case Const(v, _) =>
              v.toString
            case _ =>
              error("date_add requires a constant unit", f)
        val t    = args(2).ptype
        val sign =
          if name == "date_sub" then
            -1
          else
            1
        RowFn(
          "date_add",
          Seq(args(1), args(2)),
          t,
          a => ScalarFunctions.addInterval(t, a(1), unit, sign * ValueOps.toLong(a(0)))
        )
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
              name != "random"
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

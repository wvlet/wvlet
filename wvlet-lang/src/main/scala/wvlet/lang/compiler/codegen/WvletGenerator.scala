package wvlet.lang.compiler.codegen

import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.log.LogSupport

object WvletGenerator:
  sealed trait WvletContext:
    def indent: Int
    def enterExpression: WvletContext

  case class Indented(indent: Int) extends WvletContext:
    def enterExpression: Indented = Indented(indent + 1)

  case class InExpression(indent: Int) extends WvletContext:
    def enterExpression: InExpression = this

end WvletGenerator

class WvletGenerator(using ctx: Context = Context.NoContext) extends LogSupport:
  import WvletGenerator.*

  def print(l: LogicalPlan): String =
    def iter(plan: LogicalPlan): String =
      plan match
        case p: PackageDef =>
          p.statements.map(stmt => iter(stmt)).mkString(";\n\n")
        case r: Relation =>
          printRelation(r)(using Indented(0))
        case other =>
          warn(s"Unsupported logical plan: ${other}")
          other.toString

    val wv = iter(l)
    trace(l.pp)
    debug(wv)
    wv

  def printRelation(r: Relation)(using wvletContext: WvletContext): String =
    r match
      case q: Query =>
        printRelation(q.child)
      case s: Sort =>
        val prev = printRelation(s.child)
        s"${prev}\norder by ${s.orderBy.map(x => printExpression(x)).mkString(", ")}"
      case p: Project =>
        val prev = printRelation(p.child)
        val cols = p.selectItems.map(x => printExpression(x)).mkString(", ")
        s"${prev}\nselect ${cols}"
      case g: GroupBy =>
        val prev = printRelation(g.child)
        val cols = g.groupingKeys.map(x => printExpression(x)).mkString(", ")
        s"${prev}\ngroup by ${cols}"
      case f: Filter =>
        val prev = printRelation(f.child)
        s"${prev}\nwhere ${printExpression(f.filterExpr)}"
      case t: TableInput =>
        s"from ${printExpression(t.sqlExpr)}"
      case other =>
        warn(s"Unsupported relation: ${other.nodeName}")
        other.nodeName

  def printExpression(
      expression: Expression
  )(using wvletContext: WvletContext = Indented(0)): String =
    expression match
      case g: UnresolvedGroupingKey =>
        printExpression(g.child)
      case f: FunctionApply =>
        val base = printExpression(f.base)
        val args = f.args.map(x => printExpression(x)).mkString(", ")
        val w    = f.window.map(x => printExpression(x))
        val stem = s"${base}(${args})"
        if w.isDefined then
          s"${stem} ${w.get}"
        else
          stem
      case w: WindowApply =>
        val base   = printExpression(w.base)
        val window = printExpression(w.window)
        Seq(s"${base}", window).mkString(" ")
      case f: FunctionArg =>
        // TODO handle arg name mapping
        if f.isDistinct then
          s"distinct ${printExpression(f.value)}"
        else
          printExpression(f.value)
      case w: Window =>
        val s = Seq.newBuilder[String]
        if w.partitionBy.nonEmpty then
          s += "partition by"
          s += w.partitionBy.map(x => printExpression(x)).mkString(", ")
        if w.orderBy.nonEmpty then
          s += "order by"
          s += w.orderBy.map(x => printExpression(x)).mkString(", ")
        w.frame
          .foreach { f =>
            s += s"${f.frameType.expr} between ${f.start.expr} and ${f.end.expr}"
          }
        s"over (${s.result().mkString(" ")})"
      case Eq(left, n: NullLiteral, _) =>
        s"${printExpression(left)} is null"
      case NotEq(left, n: NullLiteral, _) =>
        s"${printExpression(left)} is not null"
      case a: ArithmeticUnaryExpr =>
        a.sign match
          case Sign.NoSign =>
            printExpression(a.child)
          case Sign.Positive =>
            s"+${printExpression(a.child)}"
          case Sign.Negative =>
            s"-${printExpression(a.child)}"
      case b: BinaryExpression =>
        s"${printExpression(b.left)} ${b.operatorName} ${printExpression(b.right)}"
      case s: StringPart =>
        s.stringValue
      case s: StringLiteral =>
        // Escape single quotes
        val v = s.stringValue.replaceAll("'", "''")
        s"'${v}'"
      case i: IntervalLiteral =>
        s"interval ${i.stringValue}"
      case g: GenericLiteral =>
        s"${g.tpe.typeName} '${g.value}'"
      case l: Literal =>
        l.stringValue
      case bq: BackQuotedIdentifier =>
        // Need to use double quotes for back-quoted identifiers, which represents table or column names
        s"\"${bq.unquotedValue}\""
      case w: Wildcard =>
        w.strExpr
      case i: Identifier =>
        i.strExpr
      case s: SortItem =>
        s"${printExpression(s.sortKey)}${s.ordering.map(x => s" ${x.expr}").getOrElse("")}"
      case s: SingleColumn =>
        val left = printExpression(s.expr)
        if s.nameExpr.isEmpty then
          left
        else if left != s.nameExpr.toSQLAttributeName then
          s"${left} as ${s.nameExpr.toSQLAttributeName}"
        else
          left
      case a: Attribute =>
        a.fullName
      case t: TypedExpression =>
        printExpression(t.child)
      case p: ParenthesizedExpression =>
        s"(${printExpression(p.child)})"
      case i: InterpolatedString =>
        i.parts
          .map { e =>
            printExpression(e)
          }
          .mkString
      case s: SubQueryExpression =>
        val wv = printRelation(s.query)(using wvletContext.enterExpression)
        wv
      case i: IfExpr =>
        s"if(${printExpression(i.cond)}, ${printExpression(i.onTrue)}, ${printExpression(i.onFalse)})"
      case n: Not =>
        s"not ${printExpression(n.child)}"
      case l: ListExpr =>
        l.exprs.map(x => printExpression(x)).mkString(", ")
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        s"${printExpression(qual)}.${printExpression(name)}"
      case in: In =>
        val left  = printExpression(in.a)
        val right = in.list.map(x => printExpression(x)).mkString(", ")
        s"${left} in (${right})"
      case notIn: NotIn =>
        val left  = printExpression(notIn.a)
        val right = notIn.list.map(x => printExpression(x)).mkString(", ")
        s"${left} not in (${right})"
      //      case n: NativeExpression =>
      //        printExpression(ExpressionEvaluator.eval(n, ctx))
      case a: ArrayConstructor =>
        s"ARRAY[${a.values.map(x => printExpression(x)).mkString(", ")}]"
      case a: ArrayAccess =>
        s"${printExpression(a.arrayExpr)}[${printExpression(a.index)}]"
      case c: CaseExpr =>
        val s = Seq.newBuilder[String]
        s += "case"
        c.target
          .foreach { t =>
            s += s"${printExpression(t)}"
          }
        c.whenClauses
          .foreach { w =>
            s += s"when ${printExpression(w.condition)} then ${printExpression(w.result)}"
          }
        c.elseClause
          .foreach { e =>
            s += s"else ${printExpression(e)}"
          }
        s += "end"
        s.result().mkString(" ")
      case l: LambdaExpr =>
        val args = l.args.map(printExpression(_)).mkString(", ")
        if l.args.size == 1 then
          s"${args} -> ${printExpression(l.body)}"
        else
          s"(${args}) -> ${printExpression(l.body)}"
      case s: StructValue =>
        val fields = s
          .fields
          .map { f =>
            s"${f.name}: ${printExpression(f.value)}"
          }
        s"{${fields.mkString(", ")}}"
      case m: MapValue =>
        val entries = m
          .entries
          .map { e =>
            s"${printExpression(e.key)}: ${printExpression(e.value)}"
          }
        s"map{${entries.mkString(", ")}}"
      case b: Between =>
        s"${printExpression(b.e)} between ${printExpression(b.a)} and ${printExpression(b.b)}"
      case b: NotBetween =>
        s"${printExpression(b.e)} not between ${printExpression(b.a)} and ${printExpression(b.b)}"
      case c: Cast =>
        s"cast(${printExpression(c.child)} as ${c.dataType.typeName})"
      case n: NativeExpression =>
        printExpression(ExpressionEvaluator.eval(n))
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end WvletGenerator

package wvlet.lang.compiler.codegen

import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.log.LogSupport

case class WvletFormatConfig(
    indentWidth: Int = 2,
    maxWidth: Int = 100,
    newlineAfterSelection: Boolean = true,
    addTrailingComma: Boolean = true
):
  require(indentWidth > 0, "indentWidth must be positive")

object WvletGenerator:
  sealed trait WvletContext:
    def indent: Int
    def enterExpression: WvletContext = InExpression(indent + 1)
    def enterFrom: WvletContext       = InFromClause(indent + 1)
    def nested: WvletContext          = Indented(indent + 1)

    def isNested: Boolean = indent > 0

    def inExpression: Boolean =
      this match
        case InExpression(_) =>
          true
        case _ =>
          false

    def inFrom: Boolean =
      this match
        case InFromClause(_) =>
          true
        case _ =>
          false

  case class Indented(indent: Int)     extends WvletContext
  case class InExpression(indent: Int) extends WvletContext
  case class InFromClause(indent: Int) extends WvletContext

end WvletGenerator

class WvletGenerator(config: WvletFormatConfig = WvletFormatConfig())(using
    ctx: Context = Context.NoContext
) extends LogSupport:
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

  private def indent(block: String, offset: Int = 0)(using wvletContext: WvletContext): String =
    val ws = " " * ((wvletContext.indent + offset) * config.indentWidth)
    block.split("\n").map(line => s"${ws}${line}").mkString("\n")

  private def wrapWithBlockIfNecessary(body: String)(using wvletContext: WvletContext): String =
    if !wvletContext.isNested then
      body
    else
      indent(s"{${body}}")

  private def lines(lines: Any*)(using wvletContext: WvletContext): String = lines
    .collect {
      case s: String =>
        s
      case Some(s) =>
        s
    }
    .mkString("\n")

  private def fitInLine(width: Int)(using wvletContext: WvletContext): Boolean =
    width + wvletContext.indent * config.indentWidth <= config.maxWidth

  private def printOpAndSingle(op: String, child: Relation, item: String)(using
      wvletContext: WvletContext
  ): String =
    val prev = printRelation(child)
    if fitInLine(op.size + 1 + item.size) then
      val q = indent(s"${op} ${item}")
      lines(prev, q)
    else
      val q = indent(item, 1)
      lines(prev, indent(op), q)

  private def printOpAndItems(op: String, child: Relation, items: Seq[String])(using
      wvletContext: WvletContext
  ): String =
    val prev = printRelation(child)

    if fitInLine(op.size + 1 + items.map(_.size).sum) then
      val q = indent(s"${op} ${items.mkString(", ")}")
      lines(prev, q)
    else
      val endItem =
        if config.addTrailingComma then
          ","
        else
          ""
      val q = indent(items.mkString("", ",\n", endItem), 1)
      lines(prev, op, q)

  def printRelation(r: Relation)(using wvletContext: WvletContext): String =
    r match
      case q: Query =>
        printRelation(q.child)
      case s: Sort =>
        printOpAndItems("order by", s.child, s.orderBy.map(x => printExpression(x)))
      case p: Project =>
        printOpAndItems("select", p.child, p.selectItems.map(x => printExpression(x)))
      case g: GroupBy =>
        printOpAndItems("group by", g.child, g.groupingKeys.map(x => printExpression(x)))
      case a: Agg =>
        printOpAndItems("agg", a.child, a.aggExprs.map(x => printExpression(x)))
      case f: Filter =>
        printOpAndSingle("where", f.child, printExpression(f.filterExpr))
      case l: Limit =>
        printOpAndSingle("limit", l.child, printExpression(l.limit))
      case t: TableInput =>
        if wvletContext.inFrom then
          printExpression(t.sqlExpr)
        else
          indent(s"from ${printExpression(t.sqlExpr)}")
      case a: AddColumnsToRelation =>
        printOpAndItems("add", a.child, a.newColumns.map(x => printExpression(x)))
      case s: ShiftColumns =>
        printOpAndItems("shift", s.child, s.shiftItems.map(x => printExpression(x)))
      case s: ExcludeColumnsFromRelation =>
        printOpAndItems("exclude", s.child, s.columnNames.map(x => printExpression(x)))
      case r: RenameColumnsFromRelation =>
        printOpAndItems("rename", r.child, r.columnAliases.map(x => printExpression(x)))
      case j: Join =>
        val prefix =
          if j.asof then
            "asof "
          else
            ""

        val left  = printRelation(j.left)
        val right = printRelation(j.right)(using wvletContext.enterFrom)
        val cond =
          j.cond match
            case NoJoinCriteria =>
              ""
            case NaturalJoin(_) =>
              ""
            case u: JoinOnTheSameColumns =>
              if u.columns.size == 1 then
                s"on ${u.columns.head.fullName}"
              else
                s"on (${u.columns.map(x => x.fullName).mkString(", ")})"
            case JoinOn(expr, _) =>
              s"on ${printExpression(expr)}"
            case JoinOnEq(keys, _) =>
              s"on ${printExpression(Expression.concatWithEq(keys))}"

        // TODO Fix indentation of multi-table implicit joins
        val q =
          j.joinType match
            case JoinType.InnerJoin =>
              s"${left}\n${prefix}join ${right}${cond}"
            case JoinType.LeftOuterJoin =>
              s"${left}\n${prefix}left join ${right}${cond}"
            case JoinType.RightOuterJoin =>
              s"${left}\n${prefix}right join ${right}${cond}"
            case JoinType.FullOuterJoin =>
              s"${left}\n${prefix}full outer join ${right}${cond}"
            case JoinType.CrossJoin =>
              s"${left}\n${prefix}cross join ${right}${cond}"
            case JoinType.ImplicitJoin =>
              s"${left}, ${right}${cond}"
        q
      case s: SetOperation =>
        val rels: List[String] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = printRelation(head)
              val tl = tail.map(x => wrapWithBlockIfNecessary(printRelation(x)))
              hd :: tl

        val op =
          s match
            case _: Union =>
              "union"
            case _: Intersect =>
              "intersect"
            case _: Except =>
              "except"
            case _: Concat =>
              "concat"

        indent(rels.mkString(s"\n${op}\n"))
      case b: BracedRelation =>
        wrapWithBlockIfNecessary(printRelation(b.child))
      case a: AliasedRelation =>
        val tableAlias: String =
          val name = printExpression(a.alias)
          a.columnNames match
            case Some(columns) =>
              s"${name}(${columns.map(x => s"${x.toSQLAttributeName}").mkString(", ")})"
            case None =>
              name
        a.child match
          case t: TableInput =>
            s"${printExpression(t.sqlExpr)} as ${tableAlias}"
          case v: Values =>
            s"${printValues(v)} as ${tableAlias}"
          case _ =>
            s"${wrapWithBlockIfNecessary(printRelation(a.child))(using
                wvletContext.nested
              )} as ${tableAlias}"
      case p: Pivot =>
        val pivotKeys    = p.pivotKeys.map(x => printExpression(x))
        val groupingKeys = p.groupingKeys.map(x => printExpression(x))
        val prev         = printRelation(p.child)
        val q            = indent(s"pivot on ${pivotKeys.mkString(", ")}")
        lines(prev, q)
      case t: TestRelation =>
        printOpAndSingle("test", t.child, printExpression(t.testExpr))
      case other =>
        warn(s"Unsupported relation: ${other.nodeName}")
        other.nodeName

  private def printValues(values: Values)(using wvletContext: WvletContext): String =
    val rows = values
      .rows
      .map { row =>
        row match
          case a: ArrayConstructor =>
            val elems = a.values.map(x => printExpression(x)).mkString(", ")
            s"[${elems}]"
          case other =>
            printExpression(other)
      }
      .mkString(", ")
    s"[${rows}]"

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
        s"{${wv}}"
      case i: IfExpr =>
        s"if(${printExpression(i.cond)}, ${printExpression(i.onTrue)}, ${printExpression(
            i.onFalse
          )})"
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
        s"[${a.values.map(x => printExpression(x)).mkString(", ")}]"
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
      case p: PivotKey =>
        val values = p.values.map(x => printExpression(x)).mkString(", ")
        if values.isEmpty then
          s"${printExpression(p.name)}"
        else
          s"${printExpression(p.name)} in (${values})"
      case s: ShouldExpr =>
        val left  = printExpression(s.left)
        val right = printExpression(s.right)
        val op    = s.testType.expr
        s"${left} ${op} ${right}"
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end WvletGenerator

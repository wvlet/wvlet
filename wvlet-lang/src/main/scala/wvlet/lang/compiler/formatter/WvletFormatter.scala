package wvlet.lang.compiler.formatter

import wvlet.lang.compiler.Context
import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import CodeFormatter.*
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.log.LogSupport

import scala.annotation.tailrec

object WvletFormatter:

  sealed trait SyntaxContext:
    def inFrom: Boolean   = this == InFromClause
    def isNested: Boolean = this != InStatement

  case object InStatement  extends SyntaxContext
  case object InExpression extends SyntaxContext
  case object InFromClause extends SyntaxContext
  case object InSubQuery   extends SyntaxContext

end WvletFormatter

class WvletFormatter(config: CodeFormatterConfig = CodeFormatterConfig())(using ctx:Context = Context.NoContext)
    extends CodeFormatter(config)
    with LogSupport:
  import WvletFormatter.*

  def format(l: LogicalPlan): String =
    val doc: Doc = convert(l)
    render(0, doc)

  def convert(l: LogicalPlan): Doc =
    def toDoc(plan: LogicalPlan): Doc =
      plan match
        case p: PackageDef =>
          def concatStmts(lst: List[LogicalPlan]): Doc =
            lst match
              case Nil => empty
              case head :: Nil =>
                val d = toDoc(head)
                d
              case head :: tail =>
                val d = toDoc(head)
                head match
                  case q: Relation =>
                    d / newline / ";" / newline/ concatStmts(tail)
                  case _ =>
                    d / newline / concatStmts(tail)
          end concatStmts

          concatStmts(p.statements)
        case r: Relation =>
          relation(r)(using InStatement)
        case s: TopLevelStatement =>
          statement(s)(using InStatement)
        case other =>
          warn(s"Unsupported plan: ${other}")
          text(s"-- ${other.toString}")

    toDoc(l)

  private def unary(r: UnaryRelation, op: String, item: Expression)(using sc: SyntaxContext): Doc =
    unary(r, op, List(item))

  private def unary(r: UnaryRelation, op: String, items: List[Expression])(using
      sc: SyntaxContext
  ): Doc =
    val in  = relation(r.child)
    val lst = items.map(expr)
    val argBlock =
      if lst.isEmpty then
        None
      else
        Some(block(cs(lst)))
    val d = in / group(text(op) + argBlock)
    d

  private def relation(r: Relation)(using sc: SyntaxContext): Doc =
    r match
      case q: Query =>
        relation(q.child)
      case s: Sort =>
        unary(s, "order by", s.orderBy.toList)
      case p: Project =>
        unary(p, "select", p.selectItems)
      case g: GroupBy =>
        unary(g, "group by", g.groupingKeys)
      case a: Agg =>
        unary(a, "agg", a.aggExprs)
      case t: Transform =>
        unary(t, "transform", t.transformItems)
      case f: Filter =>
        unary(f, "where", f.filterExpr)
      case l: Limit =>
        unary(l, "limit", l.limit)
      case t: TableInput =>
        if sc.inFrom then
          expr(t.sqlExpr)
        else
          group(text("from") + block(expr(t.sqlExpr)))
      case a: AddColumnsToRelation =>
        unary(a, "add", a.newColumns)
      case s: ShiftColumns =>
        if s.isLeftShift then
          unary(s, "shift", s.shiftItems)
        else
          unary(s, "shift to right", s.shiftItems)
      case e: ExcludeColumnsFromRelation =>
        unary(e, "exclude", e.columnNames)
      case r: RenameColumnsFromRelation =>
        unary(r, "rename", r.columnAliases)
      case j: Join =>
        val asof: Option[Doc] =
          if j.asof then
            Some(text("asof"))
          else
            None
        val left  = relation(j.left)
        val right = relation(j.right)(using InFromClause)
        val joinType =
          j.joinType match
            case JoinType.InnerJoin =>
              text("join")
            case JoinType.LeftOuterJoin =>
              text("left join")
            case JoinType.RightOuterJoin =>
              text("right join")
            case JoinType.FullOuterJoin =>
              text("full join")
            case JoinType.CrossJoin =>
              text("cross join")
            case JoinType.ImplicitJoin =>
              text(",")

        val cond =
          j.cond match
            case NoJoinCriteria =>
              None
            case n: NaturalJoin =>
              None
            case u: JoinOnTheSameColumns =>
              if u.columns.size == 1 then
                Some(group(ws("on", expr(u.columns.head))))
              else
                Some(group(ws("on", paren(cs(u.columns.map(expr))))))
            case u: JoinOn =>
              Some(group(ws("on", expr(u.expr))))
            case u: JoinOnEq =>
              Some(group(ws("on", expr(Expression.concatWithEq(u.keys)))))

        group(left + newline + joinType + newline + right + newline + cond)
      case u: Union if u.isDistinct =>
        // union is not supported in Wvlet, so rewrite it to dedup(concat)
        relation(Dedup(Concat(u.left, u.right, u.span), u.span))
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = relation(head)
              val tl = tail.map(x => wrapWithBraceIfNecessary(relation(x)))
              hd :: tl

        // TODO union is not supported in Wvlet. Replace tree to dedup(concat)
        val op = s.toWvOp
        append(rels, text(op))
      case d: Dedup =>
        unary(d, "dedup", Nil)
      case d: Distinct =>
        unary(d, "distinct", Nil)
      case d: Describe =>
        unary(d, "describe", Nil)
      case s: SelectAsAlias =>
        unary(s, "select as", s.alias)
      case t: TestRelation =>
        unary(t, "test", t.testExpr)
      case d: Debug =>
        val r = relation(d.child)
        r / group(brace(relation(d.partialDebugExpr)(using InSubQuery)))
      case s: Sample =>
        val prev = relation(s.child)
        prev / group(ws("sample", s.method.toString, s.size.toExpr))
      case v: Values =>
        convertValues(v)
      case b: BracedRelation =>
        brace(relation(b.child)(using InSubQuery))
      case a: AliasedRelation =>
        val tableAlias: Doc =
          val name = expr(a.alias)
          a.columnNames match
            case Some(columns) =>
              val cols = cs(columns.map(c => text(c.toSQLAttributeName)))
              name + paren(cols)
            case None =>
              name

        a.child match
          case t: TableInput =>
            ws(expr(t.sqlExpr), "as", tableAlias)
          case v: Values =>
            ws(convertValues(v), "as", tableAlias)
          case _ =>
            ws(wrapWithBraceIfNecessary(relation(a.child)(using InSubQuery)), "as", tableAlias)
      case p: Pivot =>
        val prev      = relation(p.child)
        val pivotKeys = p.pivotKeys.map(expr)
        // TODO Add grouping keys
        // val groupingKeys = cs(p.groupingKeys.map(expr))
        val pivot = group(ws("pivot", "on", cs(pivotKeys)))
        val t = prev / pivot
        t
      case d: Delete =>
        unary(d, "delete", Nil)
      case a: AppendTo =>
        relation(a.child) / group(ws("append to", expr(a.target)))
      case a: AppendToFile =>
        relation(a.child) / group(ws("append to", s"'${a.path}'"))
      case s: SaveTo =>
        val prev = relation(s.child)
        val path = expr(s.target)
        val opts =
          if s.saveOptions.isEmpty then
            None
          else
            val lst = s
              .saveOptions
              .map { x =>
                expr(x)
              }
            Some(ws("with", lst))
        prev / group(ws("save to", path, opts))
      case s: SaveToFile =>
        val prev = relation(s.child)
        val path = s.targetName
        val opts =
          if s.saveOptions.isEmpty then
            None
          else
            val lst = s
              .saveOptions
              .map { x =>
                expr(x)
              }
            Some(ws("with", lst))
        prev / group(ws("save to", s"'${path}'", opts))
      case e: EmptyRelation =>
        empty
      case s: Show =>
        ws("show", s.showType.toString, s.inExpr.map(x => ws("in", expr(x))))
      case other =>
        warn(s"Unsupported relation: ${other}")
        Text(other.toString)

  end relation

  private def wrapWithBraceIfNecessary(d: Doc)(using sc: SyntaxContext): Doc =
    if sc.isNested then
      brace(d)
    else
      d

  private def convertValues(values: Values)(using sc: SyntaxContext): Doc =
    val rows: List[Doc] = values
      .rows
      .map { row =>
        row match
          case a: ArrayConstructor =>
            val elems = cs(a.values.map(x => expr(x)))
            bracket(elems)
          case other =>
            expr(other)
      }
    if sc.inFrom then
      bracket(cs(rows))
    else
      group(text("from") + block(bracket(cs(rows))))

  private def statement(s: TopLevelStatement)(using sc: SyntaxContext): Doc =
    s match
      case e: ExecuteExpr =>
        group(ws("execute", expr(e.expr)))
      case i: Import =>
        val importRef  = expr(i.importRef)
        val alias      = i.alias.map(a => ws("as", expr(a)))
        val fromSource = i.fromSource.map(expr)
        group(ws("import", importRef, alias, fromSource))
      case v: ValDef =>
        val name = v.name.name
        val body = expr(v.expr)
        group(ws("val", s"${name}:", v.dataType.typeName, "=", body))
      case m: ModelDef =>
        group(
          ws(
            "model",
            m.name.fullName,
            if m.params.isEmpty then
              None
            else
              paren(cs(m.params.map(x => expr(x))))
            ,
            m.givenRelationType.map(t => ws(": ", t.typeName)),
            "="
          )
        ) / nest(relation(m.child)) / "end"
      case t: ShowQuery =>
        group(ws("show", "query", expr(t.name)))
      case other =>
        warn(s"Unsupported statement: ${other}")
        text(other.nodeName)

  private def expr(e: Expression)(using sc: SyntaxContext): Doc =
    e match
      case g: UnresolvedGroupingKey =>
        expr(g.child)
      case f: FunctionApply =>
        val base = expr(f.base)
        val args = paren(cs(f.args.map(x => expr(x))))
        val w    = f.window.map(x => expr(x))
        val stem = base + args
        ws(stem, w)
      case w: WindowApply =>
        val base   = expr(w.base)
        val window = expr(w.window)
        ws(base, window)
      case f: FunctionArg =>
        // TODO handle arg name mapping
        if f.isDistinct then
          ws("distinct", expr(f.value))
        else
          expr(f.value)
      case w: Window =>
        var s = List.newBuilder[Doc]
        if w.partitionBy.nonEmpty then
          s += ws("partition by", cs(w.partitionBy.map(x => expr(x))))
        if w.orderBy.nonEmpty then
          s += ws("order by", cs(w.orderBy.map(x => expr(x))))
        w.frame
          .foreach { f =>
            s += ws(f.frameType.expr, "between", f.start.expr, "and", f.end.expr)
          }
        ws("over", paren(ws(s.result())))
      case Eq(left, n: NullLiteral, _) =>
        ws(expr(left), "is null")
      case NotEq(left, n: NullLiteral, _) =>
        ws(expr(left), "is not null")
      case a: ArithmeticUnaryExpr =>
        a.sign match
          case Sign.NoSign =>
            expr(a.child)
          case Sign.Positive =>
            text("+") + expr(a.child)
          case Sign.Negative =>
            text("-") + expr(a.child)
      case b: BinaryExpression =>
        ws(expr(b.left), b.operatorName, expr(b.right))
      case s: StringPart =>
        text(s.stringValue)
      case s: StringLiteral =>
        // Escape single quotes
        val v = s.stringValue.replaceAll("'", "''")
        text(s"'${v}'")
      case i: IntervalLiteral =>
        text(s"interval ${i.stringValue}")
      case g: GenericLiteral =>
        text(s"${g.tpe.typeName} '${g.value}'")
      case l: Literal =>
        text(l.stringValue)
      case bq: BackQuotedIdentifier =>
        // Need to use double quotes for back-quoted identifiers, which represents table or column names
        text(s"\"${bq.unquotedValue}\"")
      case w: Wildcard =>
        text(w.strExpr)
      case i: Identifier =>
        text(i.strExpr)
      case s: SortItem =>
        ws(expr(s.sortKey), s.ordering.map(x => s" ${x.expr}"))
      case s: SingleColumn =>
        val left    = expr(s.expr)
        val leftStr = render(0, left)
        if s.nameExpr.isEmpty then
          left
        else if leftStr != s.nameExpr.toSQLAttributeName then
          ws(left, "as", s.nameExpr.toSQLAttributeName)
        else
          left
      case a: Attribute =>
        text(a.fullName)
      case t: TypedExpression =>
        expr(t.child)
      case p: ParenthesizedExpression =>
        paren(expr(p.child))
      case i: InterpolatedString =>
        concat(i.parts.map(expr))
      case s: SubQueryExpression =>
        val wv = relation(s.query)(using InExpression)
        brace(wv)
      case i: IfExpr =>
        ws(
          "if",
          expr(i.cond),
          "then",
          block(expr(i.onTrue)),
          "else",
          block(expr(i.onFalse))
        )
      case n: Not =>
        ws("not", expr(n.child))
      case l: ListExpr =>
        cs(l.exprs.map(x => expr(x)))
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        expr(qual) + text(".") + expr(name)
      case in: In =>
        val left  = expr(in.a)
        val right = cs(in.list.map(x => expr(x)))
        ws(left, "in", paren(right))
      case notIn: NotIn =>
        val left  = expr(notIn.a)
        val right = cs(notIn.list.map(x => expr(x)))
        ws(left, "not in", paren(right))
      case a: ArrayConstructor =>
        bracket(cs(a.values.map(x => expr(x))))
      case a: ArrayAccess =>
        expr(a.arrayExpr) + text("[") + expr(a.index) + text("]")
      case c: CaseExpr =>
        ws(
          group(ws("case", c.target.map(expr))),
          lines(
            c.whenClauses
              .map { w =>
                group(ws("when", expr(w.condition), "then", expr(w.result)))
              }
          ),
          c.elseClause
            .map { e =>
              group(ws("else", expr(e)))
            }
        )
      case l: LambdaExpr =>
        val args = paren(cs(l.args.map(expr(_))))
        ws(args, "->", expr(l.body))
      case s: StructValue =>
        val fields = s
          .fields
          .map { f =>
            ws(text(f.name) + ":", expr(f.value))
          }
        brace(cs(fields))
      case m: MapValue =>
        val entries = m
          .entries
          .map { e =>
            ws(expr(e.key) + ":", expr(e.value))
          }
        ws("map", brace(cs(entries)))
      case b: Between =>
        ws(expr(b.e), "between", expr(b.a), "and", expr(b.b))
      case b: NotBetween =>
        ws(expr(b.e), "not between", expr(b.a), "and", expr(b.b))
      case c: Cast =>
        expr(c.child) + text(".") + text(s"to_${c.dataType.typeName}")
      case n: NativeExpression =>
        expr(ExpressionEvaluator.eval(n))
      case p: PivotKey =>
        ws(
          expr(p.name),
          if p.values.isEmpty then
            None
          else
            Some(ws("in", paren(cs(p.values.map(expr)))))
        )
      case s: ShouldExpr =>
        val left  = expr(s.left)
        val right = expr(s.right)
        val op    = s.testType.expr
        ws(left, op, right)
      case s: SaveOption =>
        ws(expr(s.key) + ":", expr(s.value))
      case d: DefArg =>
        ws(d.name.name, ":", d.dataType.typeName, d.defaultValue.map(x => ws("=", expr(x))))
      case other =>
        warn(s"unknown expression type: ${other}")
        text(other.toString)

end WvletFormatter

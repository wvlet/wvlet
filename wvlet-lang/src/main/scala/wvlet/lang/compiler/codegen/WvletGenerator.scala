package wvlet.lang.compiler.codegen

import wvlet.lang.compiler.Context
import wvlet.lang.compiler.formatter.CodeFormatter.*
import wvlet.lang.compiler.formatter.{CodeFormatter, CodeFormatterConfig}
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.api.Span
import wvlet.log.LogSupport
import SyntaxContext.*

import scala.annotation.tailrec

class WvletGenerator(config: CodeFormatterConfig = CodeFormatterConfig())(using
    ctx: Context = Context.NoContext
) extends CodeFormatter(config)
    with LogSupport:

  /**
    * Generate a formatted Wvlet code from the given logical plan
    * @param l
    */
  def print(l: LogicalPlan): String =
    val doc: Doc = convert(l)
    render(0, doc)

  def convert(l: LogicalPlan): Doc =
    def toDoc(plan: LogicalPlan): Doc =
      plan match
        case p: PackageDef =>
          def concatStmts(lst: List[LogicalPlan]): Doc =
            lst match
              case Nil =>
                empty
              case head :: Nil =>
                val d = toDoc(head)
                d
              case head :: tail =>
                val d = toDoc(head)
                head match
                  case q: Relation =>
                    (d + linebreak + ";" + linebreak) / concatStmts(tail)
                  case _ =>
                    d + linebreak + concatStmts(tail)
          end concatStmts

          if p.name.isEmpty then
            concatStmts(p.statements)
          else
            group(text("package") + whitespace + expr(p.name)(using InStatement)) + linebreak +
              concatStmts(p.statements)
        case r: Relation =>
          relation(r)(using InStatement)
        case s: TopLevelStatement =>
          statement(s)(using InStatement)
        case other =>
          warn(s"Unsupported plan: ${other}")
          text(s"-- ${other.toString}")

    toDoc(l)

  end convert

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

  private def tableAliasOf(a: AliasedRelation)(using sc: SyntaxContext): Doc =
    val name = expr(a.alias)
    a.columnNames match
      case Some(columns) =>
        val cols = cs(columns.map(c => text(c.toSQLAttributeName)))
        name + paren(cols)
      case None =>
        name

  private def relation(r: Relation)(using sc: SyntaxContext): Doc =
    r match
      case q: Query =>
        relation(q.child)
      case w: WithQuery =>
        val defs = w
          .queryDefs
          .map { d =>
            val alias = tableAliasOf(d)
            val body  = relation(d.child)(using InSubQuery)
            text("with") + whitespace + alias + whitespace + text("as") + whitespace +
              indentedBrace(body)
          }
        lines(defs) + linebreak + relation(w.queryBody)
      case s: Sort =>
        unary(s, "order by", s.orderBy.toList)
      case p: Project =>
        unary(p, "select", p.selectItems)
      case g: GroupBy =>
        unary(g, "group by", g.groupingKeys)
      case a: Agg =>
        unary(a, "agg", a.aggExprs)
      case f: Filter =>
        unary(f, "where", f.filterExpr)
      case l: Limit =>
        unary(l, "limit", l.limit)
      case o: Offset =>
        unary(o, "offset", o.rows)
      case c: Count =>
        unary(c, "count", Nil)
      case t: TableInput =>
        if sc.inFromClause then
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
      case a: AliasedRelation =>
        val tableAlias: Doc = tableAliasOf(a)
        a.child match
          case t: TableInput if sc.isNested =>
            group(expr(t.sqlExpr) + whitespaceOrNewline + "as" + whitespace + tableAlias)
          case v: Values if sc.isNested =>
            group(values(v) + whitespaceOrNewline + "as" + whitespace + tableAlias)
          case _ =>
            group(
              wrapWithBraceIfNecessary(relation(a.child)(using InSubQuery)) +
                nest(whitespaceOrNewline + "as" + whitespace + tableAlias)
            )
      case j: Join =>
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

        val joinOp: Doc =
          if j.asof then
            ws(text("asof"), joinType)
          else
            joinType

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

        group(left + whitespaceOrNewline + joinOp + whitespace + right + whitespaceOrNewline + cond)
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
        unary(s, "select as", s.target)
      case t: TestRelation =>
        unary(t, "test", t.testExpr)
      case d: Debug =>
        val r = relation(d.child)
        // Render the debug expr like a top-level query
        val body      = relation(d.partialDebugExpr)(using InStatement)
        val debugExpr = text("debug") + whitespace + indentedBrace(body)
        r / debugExpr
      case s: Sample =>
        val prev = relation(s.child)
        prev / group(ws("sample", s.method.toString, s.size.toExpr))
      case v: Values =>
        values(v)
      case b: BracedRelation =>
        codeBlock(relation(b.child)(using InSubQuery))
      case p: Pivot =>
        val prev      = relation(p.child)
        val pivotKeys = p.pivotKeys.map(expr)
        // TODO Add grouping keys
        // val groupingKeys = cs(p.groupingKeys.map(expr))
        val pivot = group(ws("pivot", "on", cs(pivotKeys)))
        val t     = prev / pivot
        t
      case d: Delete =>
        unary(d, "delete", Nil)
      case a: AppendTo =>
        relation(a.child) / group(ws("append to", expr(a.target)))
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
            Some(whitespace + "with" + nest(whitespaceOrNewline + cs(lst)))
        prev / group(ws("save to", path) + opts)
      case e: EmptyRelation =>
        empty
      case s: Show =>
        ws("show", s.showType.toString, s.inExpr.map(x => ws("in", expr(x))))
      case other =>
        unsupportedNode(s"relation ${other.nodeName}", other.span)

  end relation

  private def unsupportedNode(nodeType: String, span: Span): Doc =
    val loc = ctx.sourceLocationAt(span)
    val msg = s"Unsupported ${nodeType} (${loc})"
    warn(msg)
    text(s"-- ${msg}")

  private def wrapWithBraceIfNecessary(d: Doc)(using sc: SyntaxContext): Doc =
    if sc.isNested then
      codeBlock(d)
    else
      d

  private def values(values: Values)(using sc: SyntaxContext): Doc =
    val rows: List[Doc] = values.rows.map(expr)
    def newBlock        = bracket(cs(rows))
    if sc.inFromClause then
      newBlock
    else
      group(text("from") + whitespace + newBlock)

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
        val nameAndType: Doc =
          if v.dataType.isUnknownType then
            text(name)
          else
            text(name) + ": " + v.dataType.typeName.toString
        group(ws("val", nameAndType, "=", body))
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
        ) + nest(linebreak + relation(m.child)) + linebreak + "end"
      case t: ShowQuery =>
        group(ws("show", "query", expr(t.name)))
      case other =>
        unsupportedNode(s"statement ${other.nodeName}", other.span)

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
        val s = List.newBuilder[Doc]
        if w.partitionBy.nonEmpty then
          s += ws("partition by", cs(w.partitionBy.map(x => expr(x))))
        if w.orderBy.nonEmpty then
          s += ws("order by", cs(w.orderBy.map(x => expr(x))))
        w.frame
          .foreach { f =>
            s += text(f.frameType.expr) + "[" + f.start.wvExpr + ":" + f.end.wvExpr + "]"
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
      case c: LogicalConditionalExpression =>
        expr(c.left) + whitespaceOrNewline + text(c.operatorName) + whitespace + expr(c.right)
      case b: BinaryExpression =>
        ws(expr(b.left), b.operatorName, expr(b.right))
      case s: StringPart =>
        text(s.stringValue)
      case i: IntervalLiteral =>
        val s = StringLiteral.fromString(i.stringValue, i.span)
        expr(s) + text(":interval")
      case g: GenericLiteral =>
        text(s"${g.value}:${g.tpe.typeName}")
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
        ws(
          expr(s.sortKey),
          s.ordering.map(x => text(x.expr)),
          s.nullOrdering.map(x => text(x.expr))
        )
      case s: SingleColumn =>
        val left    = expr(s.expr)
        val leftStr = render(0, left)
        if s.nameExpr.isEmpty then
          left
        else if leftStr != s.nameExpr.toSQLAttributeName then
          group(left + whitespaceOrNewline + "as" + whitespace + s.nameExpr.toSQLAttributeName)
        else
          left
      case a: Attribute =>
        text(a.fullName)
      case t: TypedExpression =>
        expr(t.child)
      case p: ParenthesizedExpression =>
        paren(expr(p.child))
      case i: InterpolatedString =>
        // TODO: Switch sql"..." (single quote) or sql""" ... """ (triple quote)
        def doc(e: Expression): Doc =
          e match
            case s: StringPart =>
              text(s.value)
            case other =>
              text("$") + brace(expr(e))

        def loop(lst: List[Expression]): Doc =
          lst match
            case Nil =>
              empty
            case head :: Nil =>
              doc(head)
            case head :: tail =>
              doc(head) + loop(tail)

        val prefix = i
          .prefix
          .map { name =>
            expr(name)
          }
          .getOrElse(empty)

        val quote =
          if i.isTripleQuote then
            text("\"\"\"")
          else
            text("\"")
        prefix + quote + loop(i.parts) + quote
      case s: SubQueryExpression =>
        val wv = relation(s.query)(using InSubQuery)
        codeBlock(wv)
      case i: IfExpr =>
        ws("if", expr(i.cond), "then", block(expr(i.onTrue)), "else", block(expr(i.onFalse)))
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
      case e: Exists =>
        ws("exists", expr(e.child))
      case a: ArrayConstructor =>
        bracket(cs(a.values.map(x => expr(x))))
      case a: ArrayAccess =>
        expr(a.arrayExpr) + text("[") + expr(a.index) + text("]")
      case c: CaseExpr =>
        ws(
          ws("case", c.target.map(expr)),
          c.whenClauses
            .map { w =>
              nest(newline + ws("when", expr(w.condition), "then", expr(w.result)))
            },
          c.elseClause
            .map { e =>
              nest(newline + ws("else", expr(e)))
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
        expr(c.child) + text(".") + text(s"to_${c.tpe.typeName}")
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
        ws((expr(s.key) + ":"), expr(s.value))
      case d: DefArg =>
        ws(d.name.name, ":", d.dataType.typeName, d.defaultValue.map(x => ws("=", expr(x))))
      case other =>
        unsupportedNode(s"expression ${other}", other.span)

end WvletGenerator

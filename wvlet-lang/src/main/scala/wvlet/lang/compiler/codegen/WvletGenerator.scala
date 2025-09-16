package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.codegen.SyntaxContext.*
import wvlet.lang.compiler.codegen.{CodeFormatter, CodeFormatterConfig}
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.SyntaxTreeNode
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.log.LogSupport

class WvletGenerator(config: CodeFormatterConfig = CodeFormatterConfig())(using
    ctx: Context = Context.NoContext
) extends QueryPrinter(CodeFormatter(config))
    with LogSupport:

  import CodeFormatter.*

  /**
    * Shared helper for formatting map keys to ensure Wvlet compatibility
    */
  private def mapKeyDoc(k: Expression)(using sc: SyntaxContext): Doc =
    k match
      case s: SingleQuoteString =>
        text(s"\"${s.unquotedValue}\"")
      case d: DoubleQuoteString =>
        text(s"\"${d.unquotedValue}\"")
      case l: LongLiteral =>
        text(s"\"${l.stringValue}\"")
      case d: DoubleLiteral =>
        text(s"\"${d.stringValue}\"")
      case bq: BackQuotedIdentifier =>
        expr(bq)
      case i: Identifier =>
        expr(i)
      case other =>
        expr(other)

  override def render(l: LogicalPlan): Doc =
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

          code(p) {
            if p.name.isEmpty then
              concatStmts(p.statements)
            else
              group(text("package") + ws + expr(p.name)(using InStatement)) + linebreak +
                concatStmts(p.statements)
          }
        case d: DDL =>
          ddl(d)(using InStatement)
        case u: Update =>
          update(u)(using InStatement)
        case r: Relation =>
          relation(r)(using InStatement)
        case s: TopLevelStatement =>
          statement(s)(using InStatement)
        case other =>
          warn(s"Unsupported plan: ${other}")
          code(other) {
            text(s"-- ${other.toString}")
          }

    toDoc(l)

  end render

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
        Some(nest(maybeNewline + cl(lst)))

    val opBlock =
      code(r) {
        group(wl(text(op), argBlock))
      }
    val d = in / opBlock
    d

  private def tableAliasOf(a: AliasedRelation)(using sc: SyntaxContext): Doc =
    val name = expr(a.alias)
    a.columnNames match
      case Some(columns) =>
        val cols = cl(columns.map(c => text(c.toWvletAttributeName)))
        name + paren(cols)
      case None =>
        name

  private def code(n: SyntaxTreeNode)(d: Doc): Doc =
    val dd =
      if n.comments.isEmpty then
        d
      else
        lines(n.comments.reverse.map(c => text(c.str))) / d

    if n.postComments.isEmpty then
      dd
    else if n.postComments.size > 1 then
      dd + wsOrNL + concat(n.postComments.reverse.map(c => text(c.str)), linebreak)
    else
      wl(dd, wl(n.postComments.map(c => text(c.str))))

  private def ddl(d: DDL)(using sc: SyntaxContext): Doc =
    d match
      case _ =>
        val sqlGen = SqlGenerator(formatter.config)
        val doc    = sqlGen.render(d)
        group(wl("execute", "sql\"\"\"" + linebreak + doc + linebreak + "\"\"\""))

  private def update(u: Update)(using sc: SyntaxContext): Doc =
    u match
      case c: CreateTableAs if c.createMode == CreateMode.Replace =>
        val query  = relation(c.child)(using InStatement)
        val target = expr(c.target)
        val stmt =
          code(c) {
            query / group(wl("save to", target))
          }
        stmt
      case i: InsertInto =>
        val query  = relation(i.child)(using InStatement)
        val target = expr(i.target)
        val cols =
          if i.columns.isEmpty then
            None
          else
            Some(paren(cl(i.columns.map(expr))))
        // TODO:
        //  Translating `insert into` to `append to` is not always the correct behavior
        //  especially when the user wans to protect a wrong insertion to a non-existent table
        val stmt =
          code(i) {
            query / group(wl("append to", target + cols))
          }
        stmt
      case s: SaveTo =>
        relation(s)
      case a: AppendTo =>
        relation(a)
      case _ =>
        val sqlGen = SqlGenerator(formatter.config)
        val d      = sqlGen.render(u)
        group(wl("execute", "sql\"\"\"" + linebreak + d + linebreak + "\"\"\""))

  private def relation(r: Relation)(using sc: SyntaxContext): Doc =
    r match
      case q: Query =>
        code(q) {
          relation(q.child)
        }
      case w: WithQuery =>
        code(w) {
          val defs = w
            .queryDefs
            .map { d =>
              val alias = tableAliasOf(d)
              val body =
                d.child match
                  case v: Values =>
                    // Values inside with clause is supported
                    values(v)(using InFromClause)
                  case _ =>
                    indentedBrace(relation(d.child)(using InStatement))
              text("with") + ws + alias + ws + text("as") + ws + body
            }
          lines(defs) + linebreak + relation(w.queryBody)
        }
      case s: Sort =>
        unary(s, "order by", s.orderBy.toList)
      case h: PartitioningHint =>
        // Ignore Hive partition hints in Wvlet and just process the child
        relation(h.child)
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
        code(t) {
          if sc.inFromClause then
            tableInput(t)
          else
            group(text("from") + block(tableInput(t)))
        }
      case a: AddColumnsToRelation =>
        unary(a, "add", a.newColumns)
      case p: PrependColumnsToRelation =>
        unary(p, "prepend", p.newColumns)
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
        code(a) {
          val tableAlias: Doc = tableAliasOf(a)
          a.child match
            case t: TableInput if sc.inFromClause =>
              group(tableInput(t) + wsOrNL + "as" + ws + tableAlias)
            case t: TableInput if !sc.isNested =>
              group(wl("from", tableInput(t)) + wsOrNL + "as" + ws + tableAlias)
            case v: Values =>
              group(values(v) + wsOrNL + "as" + ws + tableAlias)
            case _ =>
              group(
                wl("from", indentedBrace(relation(a.child)(using InSubQuery))) +
                  nest(ws + "as" + ws + tableAlias)
              )
        }
      case j: Join =>
        val left  = relation(j.left)
        val right = relation(j.right)(using InFromClause)

        val asof: Option[Doc] =
          if j.asof then
            Some(text("asof") + ws)
          else
            None

        val joinType =
          j.joinType match
            case JoinType.InnerJoin =>
              wsOrNL + asof + text("join")
            case JoinType.LeftOuterJoin =>
              wsOrNL + asof + text("left join")
            case JoinType.RightOuterJoin =>
              wsOrNL + asof + text("right join")
            case JoinType.FullOuterJoin =>
              wsOrNL + asof + text("full join")
            case JoinType.CrossJoin =>
              wsOrNL + asof + text("cross join")
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
                Some(wl("on", expr(u.columns.head)))
              else
                Some(wl("on", paren(cl(u.columns.map(expr)))))
            case u: JoinOn =>
              Some(wl("on", expr(u.expr)))
            case u: JoinOnEq =>
              Some(wl("on", expr(Expression.concatWithEq(u.keys))))

        code(j) {
          group(left + joinType + ws + right + nest(wsOrNL + cond))
        }
      case u: Union if u.isDistinct =>
        // union is not supported in Wvlet, so rewrite it to dedup(concat)
        code(u) {
          relation(Dedup(Concat(u.left, u.right, u.span), u.span))
        }
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = relation(head)
              val tl = tail.map(x => indentedBrace(relation(x)))
              hd :: tl

        // TODO union is not supported in Wvlet. Replace tree to dedup(concat)
        val op = s.toWvOp
        code(s) {
          verticalAppend(rels, text(op))
        }
      case d: Dedup =>
        unary(d, "dedup", Nil)
      case d: Distinct =>
        unary(d.child, "select distinct", d.child.selectItems)
      case d: Describe =>
        unary(d, "describe", Nil)
      case s: SelectAsAlias =>
        unary(s, "select as", s.target)
      case t: TestRelation =>
        unary(t, "test", t.testExpr)
      case d: Debug =>
        val r = relation(d.child)
        // Render the debug expr like a top-level query
        val body = relation(d.partialDebugExpr)(using InStatement)
        val debugExpr =
          code(d) {
            text("debug") + ws + indentedBrace(body)
          }
        r / debugExpr
      case s: Sample =>
        val prev = relation(s.child)
        val opts =
          val sizeDoc =
            s.size match
              case SamplingSize.Rows(n) =>
                text(n.toString)
              case SamplingSize.Percentage(p) =>
                text(s"${p}%")
              case SamplingSize.PercentageExpr(e) =>
                expr(e)
          s.method match
            case Some(m) =>
              text(m.toString) + paren(sizeDoc)
            case None =>
              sizeDoc
        prev /
          code(s) {
            group(wl("sample", opts))
          }
      case v: Values =>
        values(v)
      case b: BracedRelation =>
        code(b) {
          codeBlock(relation(b.child)(using InSubQuery))
        }
      case p: Pivot =>
        val prev      = relation(p.child)
        val pivotKeys = p.pivotKeys.map(expr)
        // TODO Add grouping keys
        // val groupingKeys = cs(p.groupingKeys.map(expr))
        val pivot =
          code(p) {
            group(wl("pivot", "on", cl(pivotKeys)))
          }
        val t = prev / pivot
        t
      case u: Unpivot =>
        val prev = relation(u.child)
        val unpivot =
          code(u) {
            group(
              wl(
                "unpivot",
                expr(u.unpivotKey.valueColumnName),
                "for",
                expr(u.unpivotKey.unpivotColumnName),
                "in",
                paren(cl(u.unpivotKey.targetColumns.map(expr)))
              )
            )
          }
        prev / unpivot
      case d: Delete =>
        unary(d, "delete", Nil)
      case a: AppendTo =>
        relation(a.child) /
          code(a) {
            val targetExpr =
              if a.columns.nonEmpty then
                group(wl("append to", expr(a.target)) + paren(cl(a.columns.map(_.fullName))))
              else
                group(wl("append to", expr(a.target)))
            targetExpr
          }
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
            Some(ws + "with" + nest(wsOrNL + cl(lst)))
        prev /
          code(s) {
            group(wl("save to", path) + opts)
          }
      case e: EmptyRelation =>
        empty
      case s: Show =>
        code(s) {
          wl("show", s.showType.toString, s.inExpr.map(x => wl("in", expr(x))))
        }
      case other =>
        unsupportedNode(s"relation ${other.nodeName}", other.span)

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
    def newBlock        = bracket(cl(rows))

    code(values) {
      if sc.inFromClause then
        newBlock
      else
        group(text("from") + ws + newBlock)
    }

  private def statement(s: TopLevelStatement)(using sc: SyntaxContext): Doc =
    code(s) {
      s match
        case e: ExecuteExpr =>
          group(wl("execute", expr(e.expr)))
        case i: Import =>
          val importRef  = expr(i.importRef)
          val alias      = i.alias.map(a => wl("as", expr(a)))
          val fromSource = i.fromSource.map(x => wl("from", expr(x)))
          group(wl("import", importRef, alias, fromSource))
        case v: ValDef =>
          val name = v.name.name
          val body = expr(v.expr)
          val nameAndType: Doc =
            if v.dataType.isUnknownType then
              text(name)
            else
              text(name) + ": " + v.dataType.typeName.toString
          group(wl("val", nameAndType, "=", body))
        case m: ModelDef =>
          group(
            wl(
              "model",
              text(m.name.fullName) + {
                if m.params.isEmpty then
                  None
                else
                  Some(paren(cl(m.params.map(x => expr(x)))))
              },
              m.givenRelationType.map(t => wl(": ", t.typeName)),
              "= {"
            )
          ) + nest(linebreak + relation(m.child)) + linebreak + "}" + linebreak
        case t: TypeDef =>
          val typeParams =
            if t.params.isEmpty then
              empty
            else
              bracket(cl(t.params.map(_.wvExpr)))
          val defContexts = wl(t.defContexts.map(x => wl("in", expr(x.tpe))))
          val parent      = t.parent.map(p => wl("extends", expr(p)))

          val sep =
            if t.elems.isEmpty then
              empty
            else
              "= "
          group(wl("type", text(t.name.name) + typeParams, defContexts, parent, sep)) +
            indentedBrace(concat(t.elems.map(e => group(expr(e))), linebreak))
        case t: ShowQuery =>
          group(wl("show", "query", expr(t.name)))
        case u: UseSchema =>
          group(wl("use", expr(u.schema)))
        case e: ExplainPlan =>
          group(wl("explain", render(e.child)))
        case other =>
          unsupportedNode(s"statement ${other.nodeName}", other.span)
    }

  private def expr(e: Expression)(using sc: SyntaxContext): Doc =
    code(e) {
      e match
        case g: UnresolvedGroupingKey =>
          expr(g.child)
        case f: FunctionApply =>
          // Special handling for MAP(...) so we can emit Wvlet-native map { key: value, ... }
          f.base match
            case id: Identifier if id.unquotedValue.equalsIgnoreCase("map") =>

              val entries: List[Doc] =
                val args = f.args.map(_.value)
                args match
                  case List(ArrayConstructor(keys, _), ArrayConstructor(values, _)) =>
                    keys
                      .zip(values)
                      .map { case (k, v) =>
                        wl(mapKeyDoc(k) + ":", expr(v))
                      }
                  case _ if args.size % 2 == 0 =>
                    args
                      .grouped(2)
                      .collect { case List(k, v) =>
                        wl(mapKeyDoc(k) + ":", expr(v))
                      }
                      .toList
                  case _ =>
                    // Fallback: render as-is (function call)
                    val base    = expr(f.base)
                    val argsDoc = paren(cl(f.args.map(x => expr(x))))
                    val w       = f.window.map(x => expr(x))
                    return wl(base + argsDoc, w)

              wl("map", brace(cl(entries)))
            case _ =>
              val base =
                f.base match
                  case d: DoubleQuoteString =>
                    // Some SQL engines like Trino, DuckDB allow using double-quoted identifiers for function names, but
                    // Wvlet doesn't support double-quoted identifiers, so convert it to a backquoted identifier
                    expr(BackQuotedIdentifier(d.unquotedValue, d.dataType, d.span))
                  case other =>
                    expr(other)
              val args = paren(cl(f.args.map(x => expr(x))))
              val w    = f.window.map(x => expr(x))
              val stem = base + args
              wl(stem, w)
        case w: WindowApply =>
          val base   = expr(w.base)
          val window = expr(w.window)
          wl(base, window)
        case f: FunctionArg =>
          // TODO handle arg name mapping
          if f.isDistinct then
            wl("distinct", expr(f.value))
          else
            expr(f.value)
        case w: Window =>
          val s = List.newBuilder[Doc]
          if w.partitionBy.nonEmpty then
            s += wl("partition by", cl(w.partitionBy.map(x => expr(x))))
          if w.orderBy.nonEmpty then
            s += wl("order by", cl(w.orderBy.map(x => expr(x))))
          w.frame
            .foreach { f =>
              s += text(f.frameType.expr) + "[" + f.start.wvExpr + "," + f.end.wvExpr + "]"
            }
          wl("over", paren(wl(s.result())))
        case Eq(left, n: NullLiteral, _) =>
          wl(expr(left), "is", expr(n))
        case NotEq(left, n: NullLiteral, _) =>
          wl(expr(left), "is not", expr(n))
        case IsNull(child, _) =>
          wl(expr(child), "is null")
        case IsNotNull(child, _) =>
          wl(expr(child), "is not null")
        case DistinctFrom(left, right, _) =>
          wl(expr(left), "!=", expr(right))
        case NotDistinctFrom(left, right, _) =>
          wl(expr(left), "=", expr(right))
        case a: ArithmeticUnaryExpr =>
          a.sign match
            case Sign.NoSign =>
              expr(a.child)
            case Sign.Positive =>
              text("+") + expr(a.child)
            case Sign.Negative =>
              text("-") + expr(a.child)
        case l: LikeExpression =>
          // Handle LIKE and NOT LIKE with optional ESCAPE clause
          val escapeClause = l.escape.map(e => ws + text("escape") + ws + expr(e)).getOrElse(empty)
          expr(l.left) + ws + text(l.operatorName) + ws + expr(l.right) + escapeClause
        case r: RLikeExpression =>
          // Wvlet doesn't have native RLIKE, translate to regexp_matches
          r match
            case _: RLike =>
              text("regexp_matches") + paren(cl(expr(r.left), expr(r.right)))
            case _: NotRLike =>
              text("not") + ws + text("regexp_matches") + paren(cl(expr(r.left), expr(r.right)))
        case c: LogicalConditionalExpression =>
          expr(c.left) + wsOrNL + text(c.operatorName) + ws + expr(c.right)
        case b: BinaryExpression =>
          wl(expr(b.left), b.operatorName, expr(b.right))
        case s: StringPart =>
          text(s.stringValue)
        case i: IntervalLiteral =>
          val s = StringLiteral.fromString(i.stringValue, i.span)
          expr(s) + text(":interval")
        case g: GenericLiteral =>
          text(s"${g.value.stringValue}:${g.tpe.typeName}")
        case l: Literal =>
          text(l.stringValue)
        case bq: BackquoteInterpolatedIdentifier =>
          val p = expr(bq.prefix)
          val body = bq
            .parts
            .map {
              case s: StringPart =>
                text(s.value)
              case e =>
                text("${") + expr(e) + text("}")
            }
          p + text("`") + concat(body) + text("`")
        case bq: BackQuotedIdentifier =>
          text(s"`${bq.unquotedValue}`")
        case w: Wildcard =>
          text(w.toWvletAttributeName)
        case i: Identifier =>
          text(i.toWvletAttributeName)
        case s: SortItem =>
          wl(
            expr(s.sortKey),
            s.ordering.map(x => text(x.expr)),
            s.nullOrdering.map(x => text(x.expr))
          )
        case a: Alias =>
          wl(expr(a.expr), "as", expr(a.nameExpr))
        case s: SingleColumn =>
          val left    = expr(s.expr)
          val leftStr = formatter.render(0, left)
          if s.nameExpr.isEmpty then
            left
          else if leftStr != s.nameExpr.toWvletAttributeName then
            group(left + wsOrNL + "as" + ws + s.nameExpr.toWvletAttributeName)
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
          val wv = relation(s.query)(using InStatement)
          codeBlock(wv)
        case i: IfExpr =>
          wl("if", expr(i.cond), "then", block(expr(i.onTrue)), "else", block(expr(i.onFalse)))
        case n: Not =>
          wl("not", expr(n.child))
        case l: ListExpr =>
          cl(l.exprs.map(x => expr(x)))
        case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
          expr(qual) + text(".") + expr(name)
        case in: In =>
          val left  = expr(in.a)
          val right = cl(in.list.map(x => expr(x)))
          wl(left, "in", paren(right))
        case notIn: NotIn =>
          val left  = expr(notIn.a)
          val right = cl(notIn.list.map(x => expr(x)))
          wl(left, "not in", paren(right))
        case tupleIn: TupleIn =>
          val left  = expr(tupleIn.tuple)
          val right = cl(tupleIn.list.map(x => expr(x)))
          wl(left, "in", paren(right))
        case tupleNotIn: TupleNotIn =>
          val left  = expr(tupleNotIn.tuple)
          val right = cl(tupleNotIn.list.map(x => expr(x)))
          wl(left, "not in", paren(right))
        case tupleInRel: TupleInRelation =>
          val left  = expr(tupleInRel.tuple)
          val right = codeBlock(relation(tupleInRel.in)(using InStatement))
          wl(left, "in", right)
        case tupleNotInRel: TupleNotInRelation =>
          val left  = expr(tupleNotInRel.tuple)
          val right = codeBlock(relation(tupleNotInRel.in)(using InStatement))
          wl(left, "not in", right)
        case e: Exists =>
          wl("exists", expr(e.child))
        case a: ArrayConstructor =>
          bracket(cl(a.values.map(x => expr(x))))
        case r: RowConstructor =>
          paren(cl(r.values.map(x => expr(x))))
        case a: ArrayAccess =>
          expr(a.arrayExpr) + text("[") + expr(a.index) + text("]")
        case c: CaseExpr =>
          wl(
            wl("case", c.target.map(expr)),
            c.whenClauses
              .map { w =>
                nest(newline + wl("when", expr(w.condition), "then", expr(w.result)))
              },
            c.elseClause
              .map { e =>
                nest(newline + wl("else", expr(e)))
              }
          )
        case l: LambdaExpr =>
          val args = paren(cl(l.args.map(expr(_))))
          wl(args, "->", expr(l.body))
        case s: StructValue =>
          val fields = s
            .fields
            .map { f =>
              wl(text(f.name) + ":", expr(f.value))
            }
          brace(cl(fields))
        case m: MapValue =>
          // Wvlet map syntax requires identifier-like keys. When the key is a string literal
          // (e.g., coming from SQL 'key'), render it as a double-quoted identifier
          // to keep the generated Wvlet code parseable: map { "key": value }

          val entries = m
            .entries
            .map { e =>
              wl(mapKeyDoc(e.key) + ":", expr(e.value))
            }
          wl("map", brace(cl(entries)))
        case b: Between =>
          wl(expr(b.e), "between", expr(b.a), "and", expr(b.b))
        case b: NotBetween =>
          wl(expr(b.e), "not between", expr(b.a), "and", expr(b.b))
        case c: Cast =>
          expr(c.child) + text(".") + text(s"to_${c.tpe.typeName}")
        case a: AtTimeZone =>
          expr(a.expr) + text(".atTimeZone") + paren(expr(a.timezone))
        case n: NativeExpression =>
          expr(ExpressionEvaluator.eval(n))
        case p: PivotKey =>
          wl(
            expr(p.name),
            if p.values.isEmpty then
              None
            else
              Some(wl("in", paren(cl(p.values.map(expr)))))
          )
        case s: ShouldExpr =>
          val left  = expr(s.left)
          val right = expr(s.right)
          val op    = s.testType.expr
          wl(left, op, right)
        case s: SaveOption =>
          wl((expr(s.key) + ":"), expr(s.value))
        case d: DefArg =>
          wl(
            d.name.name + ":" + d.dataType.typeName.toString,
            d.defaultValue.map(x => wl("=", expr(x)))
          )
        case f: FieldDef =>
          group(wl(f.name.name + ":", expr(f.tpe), f.body.map(b => wl("=", expr(b)))))
        case e: Extract =>
          // Convert EXTRACT(field FROM expr) to expr.extract(field)
          expr(e.expr) + text(".extract") + paren(text(s"'${e.interval.toString.toLowerCase}'"))
        case other =>
          unsupportedNode(s"expression ${other}", other.span)
    }

  private def nameExpr(e: NameExpr)(using sc: SyntaxContext): Doc =
    e match
      case DotRef(qual, name, _, _) =>
        expr(qual) + text(".") + nameExpr(name)
      case b: BackquoteInterpolatedIdentifier =>
        expr(b)
      case other =>
        text(e.toWvletAttributeName)

  private def tableInput(e: TableInput)(using sc: SyntaxContext): Doc =
    e match
      case t: TableRef =>
        nameExpr(t.name)
      case t: TableScan =>
        nameExpr(t.name.toExpr)
      case other =>
        expr(e.sqlExpr)

end WvletGenerator

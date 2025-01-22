package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.compiler.{Context, DBType, ModelSymbolInfo, SQLDialect}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.JoinType.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport
import SyntaxContext.*
import wvlet.lang.compiler.formatter.CodeFormatter
import wvlet.lang.compiler.formatter.CodeFormatter.*
import wvlet.lang.compiler.formatter.CodeFormatterConfig

import scala.collection.immutable.ListMap

object SqlGenerator:
  private val identifierPattern = "^[_a-zA-Z][_a-zA-Z0-9]*$".r
  private def doubleQuoteIfNecessary(s: String): String =
    if identifierPattern.matches(s) then
      s
    else
      s""""${s}""""

  /**
    * A block corresponding to a single SQL SELECT statement
    * @param selectItems
    * @param whereFilter
    * @param groupingKeys
    * @param having
    * @param orderBy
    * @param limit
    * @param offset
    */
  case class SQLBlock(
      child: Option[Relation] = None,
      selectItems: List[Attribute] =
        Nil, // SingleColumn(EmptyName, Wildcard(NoSpan), NoSpan) :: Nil,
      isDistinct: Boolean = false,
      whereFilter: List[Filter] = Nil,
      groupingKeys: List[GroupingKey] = Nil,
      having: List[Filter] = Nil,
      orderBy: List[SortItem] = Nil,
      limit: Option[Expression] = None,
      offset: Option[Expression] = None
  ):
    // def acceptSelectItems: Boolean = selectItems.isEmpty
    def acceptOffset: Boolean  = offset.isEmpty && limit.isEmpty
    def acceptLimit: Boolean   = limit.isEmpty && orderBy.isEmpty
    def acceptOrderBy: Boolean = orderBy.isEmpty

    def isEmpty: Boolean =
      child.isEmpty && !isDistinct && selectItems.isEmpty && whereFilter.isEmpty &&
        groupingKeys.isEmpty && having.isEmpty && orderBy.isEmpty && limit.isEmpty && offset.isEmpty

end SqlGenerator

/**
  * SqlGenerator generates a SQL query from a given LogicalPlan for the target SQL engine.
  *
  * Algorithm:
  *   - As Wvlet's LogicalPlan may not have SELECT node (e.g., Project), complement the missing
  *     SELECT node (select *)
  *   - Then recursively traverse and generate SQL strings by tracking the indentation level and SQL
  *     syntax context in SyntaxContext
  *   - As SQL requires to have a rigit structure of SELECT ... FROM .. WHERE .. GROUP BY ... HAVING
  *     .. ORDER BY ... LIMIT, some we push some plan nodes to remainingParents stack
  *   - When we find Selection node (Project, Agg, column-level operators), consolidate
  *     remainingParents and child filters to form SQLSelect node.
  *
  * @param dbType
  * @param ctx
  */
class SqlGenerator(config: CodeFormatterConfig)(using ctx: Context = Context.NoContext)
    extends CodeFormatter(config)
    with LogSupport:
  import SqlGenerator.*

  private def dbType: DBType = config.sqlDBType

  def print(l: LogicalPlan): String =
    val doc: Doc = toDoc(l)
    render(0, doc)

  def print(e: Expression): String =
    val doc = expr(e)
    render(0, doc)

  def toDoc(l: LogicalPlan): Doc =
    def iter(plan: LogicalPlan): Doc =
      plan match
        case r: Relation =>
          query(r, SQLBlock())(using InStatement)
        case p: PackageDef =>
          val stmts = p.statements.map(stmt => iter(stmt))
          concat(stmts, text(";") + linebreak)
        case other =>
          warn(s"Unsupported logical plan: ${other.nodeName}")
          text(s"-- ${other.nodeName}")

    val sql = iter(l)
    trace(l.pp)
    debug(sql)
    sql

  private def wrapWithParenIfNecessary(body: Doc)(using sc: SyntaxContext): Doc =
    if sc.isNested then
      parenBlock(body)
    else
      body

  private def hasSelection(r: Relation, nestingLevel: Int = 0): Boolean =
    r match
      case w: WithQuery =>
        true
      case s: Selection =>
        true
      case g: GroupBy =>
        // Wvlet allows GroupBy without any projection (select)
        true
      case r: RawSQL =>
        true
      case s: SetOperation =>
        // The left operand of set operation will be a selection
        true
      case j: Join if nestingLevel > 0 =>
        // The left operand of join will be a selection
        true
      case r: UnaryRelation =>
        hasSelection(r.child, nestingLevel + 1)
      case _ =>
        false

  /**
    * Print a query matching with SELECT statement in SQL
    * @param r
    * @param remainingParents
    * @param sc
    * @return
    */
  private def query(r: Relation, block: SQLBlock)(using sc: SyntaxContext): Doc = relation(r, block)

  /**
    * Print Relation nodes while tracking the parent nodes (e.g., filter, sort) for merging them
    * later into a single SELECT statement.
    * @param r
    * @param remainingParents
    *   unprocessed parent nodes
    * @param sc
    * @return
    */
  def relation(r: Relation, block: SQLBlock)(using sc: SyntaxContext): Doc =
    r match
      case q: Query =>
        query(q.body, block)
      case o: Offset if block.acceptOffset =>
        relation(o.child, block.copy(offset = Some(o.rows)))
      case o: Limit if block.acceptLimit =>
        relation(o.child, block.copy(limit = Some(o.limit)))
      case s: Sort if block.acceptOrderBy =>
        relation(s.child, block.copy(orderBy = s.orderBy))
      case d: Distinct =>
        val sql = relation(d.child, SQLBlock())
        selectAll(sql, block.copy(isDistinct = true))
      case f: Filter =>
        f.child match
          case g: GroupBy =>
            // Filter(GroupBy(...)) => Having condition
            relation(f.child, block.copy(having = f :: block.having))
          case _ =>
            relation(f.child, block.copy(whereFilter = f :: block.whereFilter))
      case g: GroupBy =>
        // If there has been no Projection or Agg before GroupBy, it reaches here
        groupBy(g, block)
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = pivotOnExpr(p)
        val aggItems = cs(a.selectItems.map(x => expr(x)))
        val pivotExpr =
          val child = relation(p.child, block)(using InSubQuery)
          group(
            text("pivot") + whitespaceOrNewline + child + whitespaceOrNewline +
              ws(text("on"), onExpr) + whitespaceOrNewline + ws(text("using"), aggItems)
          )
        val sql =
          if p.groupingKeys.isEmpty then
            wrapWithParenIfNecessary(pivotExpr)
          else
            val groupByItems = cs(p.groupingKeys.map(x => expr(x)))
            wrapWithParenIfNecessary(
              pivotExpr + whitespaceOrNewline +
                group(text("group by") + nest(whitespaceOrNewline + groupByItems))
            )
        selectAll(sql, block)
      case s: Selection =>
        select(s, block)
      case j: Join =>
        val joinType: Doc =
          j.joinType match
            case InnerJoin =>
              whitespaceOrNewline + text("join")
            case LeftOuterJoin =>
              whitespaceOrNewline + text("left join")
            case RightOuterJoin =>
              whitespaceOrNewline + text("right join")
            case FullOuterJoin =>
              whitespaceOrNewline + text("full outer join")
            case CrossJoin =>
              whitespaceOrNewline + text("cross join")
            case ImplicitJoin =>
              text(",")

        val joinOp: Doc =
          if j.asof then
            if dbType.supportAsOfJoin then
              ws("asof", joinType)
            else
              throw StatusCode.SYNTAX_ERROR.newException(s"AsOf join is not supported in ${dbType}")
          else
            joinType

        val l = relation(j.left, block)
        val r = relation(j.right, SQLBlock())(using InFromClause)
        val c: Option[Doc] =
          j.cond match
            case NoJoinCriteria =>
              None
            case NaturalJoin(_) =>
              None
            case u: JoinOnTheSameColumns =>
              Some(whitespaceOrNewline + text("using" + paren(cs(u.columns.map(_.fullName)))))
            case JoinOn(e, _) =>
              Some(whitespaceOrNewline + ws("on", expr(e)))
            case JoinOnEq(keys, _) =>
              Some(whitespaceOrNewline + ws("on", expr(Expression.concatWithEq(keys))))
        val joinSQL: Doc = group(l + joinOp + whitespaceOrNewline + r + c)
        joinSQL
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = query(head, block)(using sc)
              val tl = tail.map(x => query(x, SQLBlock())(using sc))
              hd :: tl
        val op  = text(s.toSQLOp)
        val sql = append(rels, op)
        wrapWithParenIfNecessary(sql)
      case p: Pivot => // pivot without explicit aggregations
        wrapWithParenIfNecessary(
          group(
            text("pivot") + whitespaceOrNewline +
              ws(relation(p.child, block)(using InFromClause), "on", pivotOnExpr(p))
          )
        )
      case d: Debug =>
        // Skip debug expression
        relation(d.inputRelation, block)
      case d: Dedup =>
        wrapWithParenIfNecessary(
          group(ws("select distinct *", "from", relation(d.child, block)(using InFromClause)))
        )
      case s: Sample =>
        val child = relation(s.child, block)(using InFromClause)
        val body: Doc =
          dbType match
            case DBType.DuckDB =>
              val size =
                s.size match
                  case Rows(n) =>
                    text(s"${n} rows")
                  case Percentage(percentage) =>
                    text(s"${percentage}%")
              group(
                ws(
                  "select",
                  "*",
                  "from",
                  child,
                  "using",
                  "sample",
                  text(s.method.toString.toLowerCase) + paren(size)
                )
              )
            case DBType.Trino =>
              s.size match
                case Rows(n) =>
                  // Supported only in td-trino
                  group(ws("select", cs("*", s"reservoir_sample(${n}) over()"), "from", child))
                case Percentage(percentage) =>
                  group(
                    ws(
                      "select",
                      "*",
                      "from",
                      child,
                      "TABLESAMPLE",
                      text(s.method.toString.toLowerCase) + paren(text(s"${percentage}%"))
                    )
                  )
            case _ =>
              warn(s"Unsupported sampling method: ${s.method} for ${dbType}")
              child
        selectAll(body, block)
      case a: AliasedRelation =>
        val tableAlias: Doc = tableAliasOf(a)

        a.child match
          case t: TableInput =>
            selectAll(group(ws(expr(t.sqlExpr), "as", tableAlias)), block)
          case v: Values =>
            selectAll(group(ws(values(v), "as", tableAlias)), block)
//          case v: Values if sc.nestingLevel > 0 && sc.withinJoin =>
//            // For joins, expose table column aliases to the outer scope
//            s"${selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")} as ${a.alias.fullName}"
          case _ =>
            selectAll(
              group(ws(relation(a.child, block)(using InSubQuery), "as", tableAlias)),
              block
            )
      case p: BracedRelation =>
        def inner = relation(p.child, block)
        p.child match
          case v: Values =>
            // No need to wrap values query
            inner
          case AliasedRelation(v: Values, _, _, _) =>
            inner
          case _ =>
            val body = query(p.child, block)(using InSubQuery)
            wrapWithParenIfNecessary(body)
      case t: TestRelation =>
        // Skip test expression
        relation(t.inputRelation, block)
      case q: WithQuery =>
        val subQueries: List[Doc] = q
          .queryDefs
          .map { w =>
            val subQuery = query(w.child, SQLBlock())(using InStatement)
            ws(tableAliasOf(w), "as", parenBlock(subQuery))
          }
        val body     = query(q.queryBody, block)
        val withStmt = ws("with", concat(subQueries, text(",") + linebreak))
        withStmt + linebreak + body
      case t: TableInput =>
        select(t, block)
      case v: Values =>
        selectAll(values(v), block)
      case s: SelectAsAlias =>
        // Just generate the inner bodSQL
        relation(s.child, block)
      case d: Describe =>
        // TODO: Compute schema only from local DataType information without using connectors
        // Trino doesn't support nesting describe statement, so we need to generate raw values as SQL
        val fields = d.child.relationType.fields

        val sql: Doc =
          if fields.isEmpty then
            ws(
              "select",
              cs(
                // empty values
                "'' as column_name",
                "'' as column_type"
              ),
              "limit 0"
            )
          else
            val values = cs(
              fields.map { f =>
                paren(
                  cs(
                    text("'") + f.name.name + text("'"),
                    text("'") + f.dataType.typeName.toString + text("'")
                  )
                )
              }
            )
            ws(
              "select * from",
              paren(ws("values", values)),
              "as table_schema(column_name, column_type)"
            )
        selectAll(sql, block)
      case r: RelationInspector =>
        // Skip relation inspector
        // TODO Dump output to the file logger
        relation(r.child, block)
      case s: Show if s.showType == ShowType.tables =>
        val sql: Doc = ws("select", "table_name as name", "from", "information_schema.tables")
        val cond     = List.newBuilder[Expression]

        val opts                    = ctx.global.compilerOptions
        var catalog: Option[String] = opts.catalog
        var schema: Option[String]  = opts.schema

        s.inExpr match
          case i: Identifier if i.nonEmpty =>
            schema = Some(i.leafName)
          case DotRef(q: Identifier, name, _, _) =>
            catalog = Some(q.leafName)
            schema = Some(name.leafName)
          case _ =>

        catalog.foreach { c =>
          cond +=
            Eq(
              UnquotedIdentifier("table_catalog", NoSpan),
              StringLiteral.fromString(c, NoSpan),
              NoSpan
            )
        }
        schema.foreach { s =>
          cond +=
            Eq(
              UnquotedIdentifier("table_schema", NoSpan),
              StringLiteral.fromString(s, NoSpan),
              NoSpan
            )
        }

        val conds = cond.result()
        val body =
          if conds.size == 0 then
            sql
          else
            ws(sql, "where", expr(Expression.concatWithAnd(conds)))
        selectAll(wrapWithParenIfNecessary(ws(body, "order by name")), block)
      case s: Show if s.showType == ShowType.schemas =>
        val sql: Doc = ws(
          "select",
          cs("catalog_name as \"catalog\"", "schema_name as name"),
          "from",
          "information_schema.schemata"
        )
        val cond                    = List.newBuilder[Expression]
        val opts                    = ctx.global.compilerOptions
        var catalog: Option[String] = opts.catalog

        s.inExpr match
          case i: Identifier if i.nonEmpty =>
            catalog = Some(i.leafName)
          case _ =>

        catalog.foreach { c =>
          cond +=
            Eq(
              UnquotedIdentifier("catalog_name", NoSpan),
              StringLiteral.fromString(c, NoSpan),
              NoSpan
            )
        }
        val conds = cond.result()
        val body =
          if conds.size == 0 then
            sql
          else
            ws(sql, "where", expr(Expression.concatWithAnd(conds)))
        selectAll(wrapWithParenIfNecessary(ws(body, "order by name")), block)
      case s: Show if s.showType == ShowType.catalogs =>
        val sql = ws(
          "select",
          "distinct",
          "catalog_name as name",
          "from",
          "information_schema.schemata",
          "order by name"
        )
        selectAll(wrapWithParenIfNecessary(sql), block)
      case s: Show if s.showType == ShowType.models =>
        // TODO: Show models should be handled outside of GenSQL
        val models: Seq[ListMap[String, Any]] = ctx
          .global
          .getAllContexts
          .flatMap { ctx =>
            ctx
              .compilationUnit
              .knownSymbols
              .map(_.symbolInfo)
              .collect { case m: ModelSymbolInfo =>
                m
              }
              .map { m =>
                val e = ListMap.newBuilder[String, Any]
                e += "name" -> s"'${m.name.name}'"

                // model argument types
                m.symbol.tree match
                  case md: ModelDef if md.params.nonEmpty =>
                    val args = md.params.map(arg => s"${arg.name}:${arg.dataType.typeDescription}")
                    e += "args" -> args.mkString("'", ", ", "'")
                  case _ =>
                    e += "args" -> "cast(null as varchar)"

                // package_name
                if !m.owner.isNoSymbol && m.owner != ctx.global.defs.RootPackage then
                  e += "package_name" -> s"'${m.owner.name}'"
                else
                  e += "package_name" -> "cast(null as varchar)"

                e.result()
              }
          }

        val modelValues = models
          .map { x =>
            List(x("name"), x.getOrElse("args", ""), x.getOrElse("package_name", "")).mkString(",")
          }
          .map(x => s"(${x})")
        val sql =
          if modelValues.isEmpty then
            ws(
              "select",
              cs(
                "cast(null as varchar) as name",
                "cast(null as varchar) as args",
                "cast(null as varchar) as package_name"
              ),
              "limit 0"
            )
          else
            ws(
              "select",
              "*",
              "from",
              paren(ws("values", cs(modelValues))),
              "as",
              "__models(name, args, package_name)"
            )

        selectAll(wrapWithParenIfNecessary(sql), block)
      case other =>
        unsupportedNode(s"relation ${other.nodeName}", other.span)
    end match

  end relation

  private def tableAliasOf(a: AliasedRelation)(using sc: SyntaxContext): Doc =
    val name = expr(a.alias)
    a.columnNames match
      case Some(columns) =>
        val cols = cs(columns.map(c => text(c.toSQLAttributeName)))
        name + paren(cols)
      case None =>
        name

  private def pullUpChildFilters(r: Relation, block: SQLBlock): SQLBlock =
    def collectFilters(plan: Relation, filters: List[Filter]): SQLBlock =
      plan match
        case f: Filter =>
          collectFilters(f.child, f :: filters)
        case g: GroupBy =>
          pullUpChildFilters(
            g.child,
            block.copy(groupingKeys = g.groupingKeys, having = filters ++ block.having)
          )
        case other =>
          block.copy(child = Some(other), whereFilter = filters ++ block.whereFilter)

    r match
      case s: Selection if block.selectItems.isEmpty =>
        pullUpChildFilters(s.child, block.copy(selectItems = s.selectItems))
      case other =>
        collectFilters(r, Nil)
  end pullUpChildFilters

  private def indented(d: Doc): Doc = nest(maybeNewline + d)

  private def printBlock(fromStmt: Doc, block: SQLBlock)(using sc: SyntaxContext): Doc =
    val selectItems =
      if block.selectItems.isEmpty then
        if block.isDistinct then
          text("distinct *")
        else
          text("*")
      else
        cs(block.selectItems.map(x => expr(x)))

    val s = List.newBuilder[Doc]
    s += group(ws("select", selectItems))
    s += fromStmt
    if block.whereFilter.nonEmpty then
      s += group(ws("where", indented(cs(block.whereFilter.map(x => expr(x.filterExpr))))))
    if block.groupingKeys.nonEmpty then
      s += group(ws("group by", indented(cs(block.groupingKeys.map(x => expr(x))))))
    if block.having.nonEmpty then
      s += group(ws("having", indented(cs(block.having.map(x => expr(x.filterExpr))))))
    if block.orderBy.nonEmpty then
      s += group(ws("order by", indented(cs(block.orderBy.map(x => expr(x))))))
    if block.limit.nonEmpty then
      s += group(ws("limit", indented(expr(block.limit.get))))

    wrapWithParenIfNecessary(lines(s.result()))

  private def selectAll(body: Doc, block: SQLBlock)(using sc: SyntaxContext): Doc =
    if block.isEmpty then
      body
    else
      printBlock(ws("from", parenBlock(body)), block)

  private def select(r: Relation, block: SQLBlock)(using sc: SyntaxContext): Doc =
    // Pull up child filters to the parent
    val newBlock: SQLBlock = pullUpChildFilters(r, block)

    val child = newBlock
      .child
      .getOrElse {
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(s"Child relation not found in ${r}", r.sourceLocation)
      }

    val fromStmt =
      child match
        case e: EmptyRelation =>
          // Do not add from clause for empty inputs
          empty
        case t: TableInput =>
          group(ws("from", indented(expr(t.sqlExpr))))
        case _ =>
          // Start a new SELECT statement inside FROM
          group(ws("from", indented(relation(child, SQLBlock())(using InFromClause))))

    printBlock(fromStmt, newBlock)
  end select

  private def groupBy(g: GroupBy, block: SQLBlock)(using sc: SyntaxContext): Doc =
    // Translate GroupBy node without any projection (select) to Agg node
    val keys: List[Attribute] = g
      .groupingKeys
      .map { k =>
        val keyName = render(0, expr(k))
        SingleColumn(NameExpr.fromString(keyName), k.name, NoSpan)
      }
    val aggExprs: List[Attribute] =
      g.inputRelationType
        .fields
        .map { f =>
          // Use arbitrary(expr) for efficiency
          val ex: Expression = FunctionApply(
            NameExpr.fromString("arbitrary"),
            args = List(
              FunctionArg(None, NameExpr.fromString(f.toSQLAttributeName), false, NoSpan)
            ),
            None,
            NoSpan
          )
          val name: NameExpr =
            dbType match
              case DBType.DuckDB =>
                // DuckDB generates human-friendly column name
                EmptyName
              case _ =>
                val exprStr = expr(ex)
                NameExpr.fromString(render(0, exprStr))
          SingleColumn(name, ex, NoSpan)
        }
        .toList

    val agg = Agg(g, keys, aggExprs, g.span)
    relation(agg, block.copy(groupingKeys = g.groupingKeys))
  end groupBy

  private def values(values: Values)(using sc: SyntaxContext): Doc =
    val rows = cs(
      values
        .rows
        .map { row =>
          row match
            case a: ArrayConstructor =>
              paren(cs(a.values.map(x => expr(x))))
            case other =>
              expr(other)
        }
    )
    paren(ws("values", rows))

  private def pivotOnExpr(p: Pivot)(using sc: SyntaxContext): Doc = cs(
    p.pivotKeys
      .map { k =>
        val values = cs(k.values.map(v => expr(v)))
        if k.values.isEmpty then
          expr(k.name)
        else
          ws(expr(k.name), "in", paren(values))
      }
  )
  end pivotOnExpr

  def expr(expression: Expression)(using sc: SyntaxContext = InStatement): Doc =
    expression match
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
            s += ws(text(f.frameType.expr), "between", f.start.expr, "and", f.end.expr)
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
        // For adding optional newlines for AND/OR
        expr(c.left) + whitespaceOrNewline + text(c.operatorName) + whitespace + expr(c.right)
      case b: BinaryExpression =>
        ws(expr(b.left), b.operatorName, expr(b.right))
      case s: StringPart =>
        text(s.stringValue)
      case l: Literal =>
        text(l.sqlExpr)
      case bq: BackQuotedIdentifier =>
        // SQL needs to use double quotes for back-quoted identifiers, which represents table or column names
        text("\"") + bq.unquotedValue + text("\"")
      case i: Identifier =>
        text(i.strExpr)
      case s: SortItem =>
        expr(s.sortKey) + s.ordering.map(x => whitespace + text(x.expr))
      case s: SingleColumn =>
        expr(s.expr) match
          case left if s.nameExpr.isEmpty =>
            // no alias
            left
          case t @ Text(str) if str == s.nameExpr.toSQLAttributeName =>
            // alias is the same with the expression
            t
          case left =>
            ws(left, "as", s.nameExpr.toSQLAttributeName)
      case a: Attribute =>
        text(a.fullName)
      case t: TypedExpression =>
        expr(t.child)
      case p: ParenthesizedExpression =>
        paren(expr(p.child))
      case i: InterpolatedString =>
        concat(i.parts.map(expr))
      case s: SubQueryExpression =>
        // Generate the sub query as a top-level statement and wrap it later
        val sql = query(s.query, SQLBlock())(using InStatement)
        parenBlock(sql)
      case i: IfExpr =>
        text("if") + paren(cs(expr(i.cond), expr(i.onTrue), expr(i.onFalse)))
      case n: Not =>
        ws("not", expr(n.child))
      case l: ListExpr =>
        cs(l.exprs.map(x => expr(x)))
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        expr(qual) + "." + expr(name)
      case in: In =>
        val left = expr(in.a)
        val right =
          in.list match
            case (s: SubQueryExpression) :: Nil =>
              expr(s)
            case _ =>
              paren(cs(in.list.map(x => expr(x))))
        ws(left, "in", right)
      case notIn: NotIn =>
        val left = expr(notIn.a)
        val right =
          notIn.list match
            case (s: SubQueryExpression) :: Nil =>
              expr(s)
            case _ =>
              paren(cs(notIn.list.map(x => expr(x))))
        ws(left, "not in", right)
      case a: ArrayConstructor =>
        text("ARRAY") + bracket(cs(a.values.map(expr(_))))
      case a: ArrayAccess =>
        expr(a.arrayExpr) + text("[") + expr(a.index) + text("]")
      case c: CaseExpr =>
        ws(
          ws("case", c.target.map(expr)),
          c.whenClauses
            .map { w =>
              nest(newline + ws("when", group(expr(w.condition)), "then", expr(w.result)))
            },
          c.elseClause
            .map { e =>
              nest(newline + ws("else", group(expr(e))))
            },
          maybeNewline + "end"
        )
      case l: LambdaExpr =>
        val args = cs(l.args.map(expr(_)))
        if l.args.size == 1 then
          args + whitespace + "->" + whitespaceOrNewline + expr(l.body)
        else
          paren(args) + whitespace + "->" + whitespaceOrNewline + expr(l.body)
      case s: StructValue if dbType.supportRowExpr =>
        // For Trino
        val fields = s
          .fields
          .map { f =>
            expr(f.value)
          }
        val schema = s
          .fields
          .map { f =>
            val sqlType = DataType.toSQLType(f.value.dataType, dbType)
            group(text(f.name) + whitespace + sqlType)
          }
        text("cast") + paren(cs(fields)) + whitespaceOrNewline + text("as") + whitespace +
          text("row") + paren(cs(schema))
      case s: StructValue =>
        val fields = s
          .fields
          .map { f =>
            group(text(f.name) + ":" + whitespace + expr(f.value))
          }
        brace(cs(fields))
      case m: MapValue =>
        dbType.mapConstructorSyntax match
          case SQLDialect.MapSyntax.KeyValue =>
            val entries: List[Doc] = m
              .entries
              .map { e =>
                group(ws(expr(e.key) + ":", expr(e.value)))
              }
            ws("MAP", brace(cs(entries)))
          case SQLDialect.MapSyntax.ArrayPair =>
            val keys   = ArrayConstructor(m.entries.map(_.key), m.span)
            val values = ArrayConstructor(m.entries.map(_.value), m.span)
            text("MAP") + paren(cs(List(expr(keys), expr(values))))
      case b: Between =>
        ws(expr(b.e), "between", expr(b.a), "and", expr(b.b))
      case b: NotBetween =>
        ws(expr(b.e), "not between", expr(b.a), "and", expr(b.b))
      case c: Cast =>
        group(ws(text("cast") + paren(ws(expr(c.child), "as", text(c.dataType.typeName.toString)))))
      case n: NativeExpression =>
        expr(ExpressionEvaluator.eval(n))
      case e: Exists =>
        ws(text("exists"), expr(e.child))
      case other =>
        unsupportedNode(s"Expression ${other.nodeName}", other.span)

  private def unsupportedNode(nodeType: String, span: Span): Doc =
    val loc = ctx.sourceLocationAt(span)
    val msg = s"Unsupported ${nodeType} (${loc})"
    warn(msg)
    text(s"-- ${msg}")

end SqlGenerator

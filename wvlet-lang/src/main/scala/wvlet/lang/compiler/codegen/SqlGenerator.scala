package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.compiler.{Context, DBType, ModelSymbolInfo, SQLDialect}
import wvlet.lang.model.{DataType, SyntaxTreeNode}
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.JoinType.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport
import SyntaxContext.*
import wvlet.lang.compiler.codegen.CodeFormatter
import wvlet.lang.compiler.codegen.CodeFormatter.*
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.model.plan.SamplingMethod.reservoir

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
    def acceptOffset: Boolean       = offset.isEmpty && limit.isEmpty
    def acceptLimit: Boolean        = limit.isEmpty && orderBy.isEmpty
    def acceptOrderBy: Boolean      = orderBy.isEmpty
    def acceptSelectItems: Boolean  = selectItems.isEmpty
    def acceptGroupingKeys: Boolean = groupingKeys.isEmpty

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
    extends LogSupport:
  import SqlGenerator.*
  import CodeFormatter.*

  private val formatter      = CodeFormatter(config)
  private def dbType: DBType = config.sqlDBType

  def print(l: LogicalPlan): String =
    val doc: Doc = toDoc(l)
    formatter.render(0, doc)

  def print(e: Expression): String =
    val doc = expr(e)
    formatter.render(0, doc)

  def toDoc(l: LogicalPlan): Doc =
    def iter(plan: LogicalPlan): Doc =
      plan match
        case d: DDL =>
          ddl(d)(using InStatement)
        case u: Update =>
          update(u)(using InStatement)
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
      indentedParen(body)
    else
      body

  /**
    * Print a query matching with SELECT statement in SQL
    * @param r
    * @param remainingParents
    * @param sc
    * @return
    */
  private def query(r: Relation, block: SQLBlock)(using sc: SyntaxContext): Doc = relation(r, block)

  /**
    * Propagate the comments to the generated SQL
    * @param n
    * @param d
    * @return
    */
  private def code(n: SyntaxTreeNode)(d: Doc): Doc =
    def toSQLComment(s: String): Doc =
      if s.startsWith("---") && s.endsWith("---") then
        val comment = s.stripPrefix("---").stripSuffix("---").trim
        text("/*") + linebreak + comment + linebreak + text("*/")
      else
        text(s)

    // warn(s"${n.nodeName} ${n.comments}")
    if n.comments.isEmpty then
      d
    else
      lines(n.comments.reverse.map(c => toSQLComment(c.str))) + linebreak + d

  def ddl(d: DDL)(using sc: SyntaxContext): Doc =
    d match
      case d: DropTable =>
        group(
          wl(
            "drop",
            "table",
            if d.ifExists then
              Some("if exists")
            else
              None
            ,
            expr(d.table)
          )
        )
      case c: CreateTable =>
        val columns =
          if c.tableElems.isEmpty then
            None
          else
            Some(paren(cl(c.tableElems.map(x => expr(x)))))
        group(
          wl(
            "create",
            "table",
            if c.ifNotExists then
              Some("if not exists")
            else
              None
            ,
            expr(c.table) + columns
          )
        )
      case _ =>
        unsupportedNode(s"DDL ${d.nodeName}", d.span)

  def update(u: Update)(using sc: SyntaxContext): Doc =
    u match
      case c: CreateTableAs =>
        val sql = query(c.child, SQLBlock())(using InStatement)
        group(
          wl(
            "create",
            "table",
            if c.createMode == CreateMode.IfNotExists then
              Some("if not exists")
            else
              None
            ,
            expr(c.target),
            "as",
            linebreak + sql
          )
        )
      case i: InsertInto =>
        val columns =
          if i.columns.isEmpty then
            empty
          else
            text(" ") + paren(cl(i.columns.map(c => expr(c))))
        // Handle VALUES specially for INSERT INTO
        val childSQL =
          i.child match
            case v: Values =>
              // For INSERT INTO, use VALUES directly without SELECT wrapper
              if dbType.requireParenForValues then
                paren(values(v))
              else
                values(v)
            case _ =>
              // For other expressions, use the regular query generation
              query(i.child, SQLBlock())(using InStatement)
        group(wl("insert", "into", expr(i.target) + columns, linebreak + childSQL))
      case _ =>
        unsupportedNode(s"Update ${u.nodeName}", u.span)

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
    def selectExpr(d: Doc): Doc =
      if block.isEmpty then
        if sc.isNested && !sc.inFromClause then
          indentedParen(d)
        else
          d
      else
        selectAll(indentedParen(d), block)

    r match
      case q: Query =>
        code(q) {
          relation(q.body, block)
        }
      case e: EmptyRelation =>
        selectAll(empty, block)
      case q: WithQuery =>
        val subQueries: List[Doc] = q
          .queryDefs
          .map { w =>
            val subQuery = query(w.child, SQLBlock())(using InStatement)
            wl(tableAliasOf(w), "as", indentedParen(subQuery))
          }
        val body     = query(q.queryBody, SQLBlock())
        val withStmt = wl("with", concat(subQueries, text(",") + linebreak))
        selectExpr(withStmt + linebreak + body)
      case o: Offset if block.acceptOffset =>
        relation(o.child, block.copy(offset = Some(o.rows)))
      case l: Limit if block.acceptLimit =>
        relation(l.child, block.copy(limit = Some(l.limit)))
      case s: Sort if block.acceptOrderBy =>
        relation(s.child, block.copy(orderBy = s.orderBy))
      case d: Distinct =>
        relation(d.child, block.copy(isDistinct = true))
      case f: Filter =>
        def addHaving(in: Relation): Doc =
          if block.acceptGroupingKeys then
            relation(in, block.copy(having = f :: block.having))
          else
            // Start a new SELECT block
            val sql = relation(in, SQLBlock(having = List(f)))
            selectAll(sql, block)

        f.child match
          case g: GroupBy =>
            // Filter(GroupBy(...)) => Having condition
            addHaving(g)
          case a: Agg =>
            // Cut the scope explicitly to apply the filter to the aggregated expressions
            // {{{
            //  wvlet: agg v.max as vmax where vmax > 10
            //   sql: select * from (select max(v) as vmax) where vmax > 10
            // }}}
            val c = indentedParen(relation(a, SQLBlock()))
            selectAll(c, block.copy(whereFilter = f :: block.whereFilter))
          case child =>
            relation(child, block.copy(whereFilter = f :: block.whereFilter))
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = pivotOnExpr(p)
        val aggItems = cl(a.selectItems.map(x => expr(x)))
        val pivotExpr =
          val child = relation(p.child, SQLBlock())(using InFromClause)
          group(
            group(text("pivot") + wsOrNL + child) + nest(wsOrNL + wl(text("on"), onExpr)) +
              nest(wsOrNL + wl(text("using"), aggItems))
          )
        val sql =
          if p.groupingKeys.isEmpty then
            pivotExpr
          else
            val groupByItems = cl(p.groupingKeys.map(x => expr(x)))
            pivotExpr + wsOrNL + group(text("group by") + nest(wsOrNL + groupByItems))
        selectExpr(sql)
      case g: GroupBy =>
        groupBy(g, block)
      case c: Count =>
        val d = relation(c.child, block)
        selectAll(indentedParen(d), SQLBlock(selectItems = c.selectItems))
      case s: Selection =>
        if block.acceptSelectItems then
          relation(s.child, block.copy(selectItems = s.selectItems))
        else
          // Start a new SQLBlock with the given select items
          val r = relation(s.child, SQLBlock(selectItems = s.selectItems))(using InSubQuery)
          // Wrap with a SELECT statement
          val d = selectAll(r, block)
          d
      case r: RawSQL =>
        selectExpr(expr(r.sqlExpr))
      case t: TableInput =>
        selectAll(expr(t.sqlExpr), block)
      case v: Values =>
        // VALUES in FROM clause needs parentheses
        selectAll(paren(values(v)), block)
      case a: AliasedRelation =>
        val tableAlias: Doc = tableAliasOf(a)

        // For a simple table input,
        a.child match
          case t: TableInput =>
            selectAll(group(wl(expr(t.sqlExpr), "as", tableAlias)), block)
          case v: Values =>
            // VALUES with alias needs parentheses
            selectAll(group(wl(paren(values(v)), "as", tableAlias)), block)
          case _ =>
            selectAll(
              group(wl(relation(a.child, SQLBlock())(using InSubQuery), "as", tableAlias)),
              block
            )
      case p: BracedRelation =>
        def inner = relation(p.child, block)
        p.child match
          case v: Values =>
            // No need to wrap values query
            selectExpr(inner)
          case AliasedRelation(v: Values, _, _, _) =>
            selectExpr(inner)
          case _ =>
            val body = query(p.child, SQLBlock())(using InSubQuery)
            selectExpr(body)
      case j: Join =>
        val asof: Option[Doc] =
          if j.asof then
            if dbType.supportAsOfJoin then
              Some(text("asof") + ws)
            else
              throw StatusCode.SYNTAX_ERROR.newException(s"AsOf join is not supported in ${dbType}")
          else
            None

        val joinType: Doc =
          j.joinType match
            case InnerJoin =>
              wsOrNL + asof + text("join")
            case LeftOuterJoin =>
              wsOrNL + asof + text("left join")
            case RightOuterJoin =>
              wsOrNL + asof + text("right join")
            case FullOuterJoin =>
              wsOrNL + asof + text("full outer join")
            case CrossJoin =>
              wsOrNL + asof + text("cross join")
            case ImplicitJoin =>
              text(",")

        // Generate SQL for left and right relations in a flat structure
        val l = relation(j.left, SQLBlock())(using InFromClause)
        val r = relation(j.right, SQLBlock())(using InFromClause)
        val c: Option[Doc] =
          j.cond match
            case NoJoinCriteria =>
              None
            case NaturalJoin(_) =>
              None
            case u: JoinOnTheSameColumns =>
              Some(wsOrNL + text("using") + ws + paren(cl(u.columns.map(expr))))
            case JoinOn(e, _) =>
              Some(wsOrNL + wl("on", expr(e)))
            case JoinOnEq(keys, _) =>
              Some(wsOrNL + wl("on", expr(Expression.concatWithEq(keys))))
        val joinSQL: Doc = group(l + joinType + ws + r + c)
        // Append select * from (left) join (right) where ...
        val sql = selectAll(joinSQL, block)
        sql
      case s: SetOperation =>
        val rels: List[Doc] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = query(head, SQLBlock())(using sc)
              val tl = tail.map(x => query(x, SQLBlock())(using sc))
              hd :: tl
        val op  = text(s.toSQLOp)
        val sql = verticalAppend(rels, op)
        selectExpr(sql)
      case p: Pivot => // pivot without explicit aggregations
        selectExpr(
          group(
            text("pivot") + wsOrNL +
              wl(relation(p.child, SQLBlock())(using InFromClause), "on", pivotOnExpr(p))
          )
        )
      case u: Unpivot =>
        // TODO This is a DuckDB unpivot syntax. Support other DBMS syntax
        def unpivotExpr(): Doc =
          nest(wsOrNL + group(wl("on", cl(u.unpivotKey.targetColumns.map(expr))))) +
            nest(
              wsOrNL +
                group(
                  wl(
                    "into",
                    "name",
                    expr(u.unpivotKey.unpivotColumnName),
                    "value",
                    expr(u.unpivotKey.valueColumnName)
                  )
                )
            )

        selectExpr(
          group(wl("unpivot", relation(u.child, SQLBlock())(using InFromClause), unpivotExpr()))
        )
      case d: Debug =>
        // Skip debug expression
        relation(d.inputRelation, block)
      case d: Dedup =>
        val r = relation(d.child, SQLBlock())(using InSubQuery)
        selectAll(r, block.copy(isDistinct = true))
      case s: Sample =>
        val child          = relation(s.child, SQLBlock())(using InFromClause)
        val samplingMethod = s.method.getOrElse(reservoir)
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
                wl(
                  "select",
                  "*",
                  "from",
                  child,
                  "using",
                  "sample",
                  text(samplingMethod.toString.toLowerCase) + paren(size)
                )
              )
            case DBType.Trino =>
              s.size match
                case Rows(n) =>
                  // Supported only in td-trino
                  group(wl("select", cl("*", s"reservoir_sample(${n}) over()"), "from", child))
                case Percentage(percentage) =>
                  group(
                    wl(
                      "select",
                      "*",
                      "from",
                      child,
                      "TABLESAMPLE",
                      text(samplingMethod.toString.toLowerCase) + paren(text(s"${percentage}%"))
                    )
                  )
            case _ =>
              warn(s"Unsupported sampling method: ${samplingMethod} for ${dbType}")
              child
        selectExpr(body)
      case t: TestRelation =>
        // Skip test expression
        code(t) {
          relation(t.inputRelation, block)
        }
      case s: SelectAsAlias =>
        // Just generate the inner bodSQL
        relation(s.child, block)
      case d: Describe =>
        // TODO: Compute schema only from local DataType information without using connectors
        // Trino doesn't support nesting describe statement, so we need to generate raw values as SQL
        val fields = d.child.relationType.fields

        val sql: Doc =
          if fields.isEmpty then
            wl(
              "select",
              cl(
                // empty values
                "'' as column_name",
                "'' as column_type"
              ),
              "limit 0"
            )
          else
            val values = cl(
              fields.map { f =>
                paren(
                  cl(
                    text("'") + f.name.name + text("'"),
                    text("'") + f.dataType.typeName.toString + text("'")
                  )
                )
              }
            )
            wl(
              "select * from",
              paren(wl("values", values)),
              "as table_schema(column_name, column_type)"
            )
        selectExpr(sql)
      case r: RelationInspector =>
        // Skip relation inspector
        // TODO Dump output to the file logger
        relation(r.child, block)
      case s: Show if s.showType == ShowType.tables =>
        val baseSql: Doc = wl("select", "table_name as name", "from", "information_schema.tables")
        val cond         = List.newBuilder[Expression]

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
            baseSql
          else
            wl(baseSql, "where", expr(Expression.concatWithAnd(conds)))

        val sql = wl(body, "order by name")

        selectExpr(sql)
      case s: Show if s.showType == ShowType.schemas =>
        val baseSql: Doc = wl(
          "select",
          cl("catalog_name as \"catalog\"", "schema_name as name"),
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
            baseSql
          else
            wl(baseSql, "where", expr(Expression.concatWithAnd(conds)))

        val sql = wl(body, "order by name")
        selectExpr(sql)
      case s: Show if s.showType == ShowType.catalogs =>
        val sql = lines(
          List(
            group(wl("select distinct", "catalog_name as name")),
            group(wl("from", "information_schema.schemata")),
            group(wl("order by", "name"))
          )
        )
        selectExpr(sql)
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
                val t = m.symbol.tree
                val description: String =
                  if t.comments.isEmpty then
                    "''"
                  else
                    val desc: String =
                      t.comments
                        .map(
                          // Exclude comments
                          _.str
                            .trim
                            .stripPrefix("---")
                            .stripPrefix("--")
                            .stripSuffix("---")
                            .stripSuffix("--")
                        )
                        .mkString("\n")
                        .trim
                    StringLiteral.fromString(desc, NoSpan).sqlExpr

                m.symbol.tree match
                  case md: ModelDef if md.params.nonEmpty =>
                    val args: String = md
                      .params
                      .map(arg => s"${arg.name}:${arg.dataType.typeDescription}")
                      .mkString("'", ", ", "'")
                    e += "args" -> args
                  case _ =>
                    e += "args" -> "cast(null as varchar)"

                e += "description" -> description

                // package_name
                if !m.owner.isNoSymbol && m.owner != ctx.global.defs.RootPackage &&
                  !m.owner.name.isEmpty
                then
                  e += "package_name" -> s"'${m.owner.name}'"
                else
                  e += "package_name" -> "cast(null as varchar)"

                e.result()
              }
          }

        val modelValues = cl(
          models.map { x =>
            group(
              paren(
                cl(
                  x("name"),
                  x.getOrElse("args", ""),
                  x.getOrElse("description", ""),
                  x.getOrElse("package_name", "")
                )
              )
            )
          }
        )

        val sql =
          if modelValues.isEmpty then
            wl(
              "select",
              cl(
                "cast(null as varchar) as name",
                "cast(null as varchar) as args",
                "cast(null as varchar) as description",
                "cast(null as varchar) as package_name"
              ),
              "limit 0"
            )
          else
            wl(
              "select",
              "*",
              "from",
              paren(wl("values", cl(modelValues))),
              "as",
              "__models(name, args, description, package_name)"
            )

        selectExpr(sql)
      case lv: LateralView =>
        // Hive LATERAL VIEW syntax
        val child = relation(lv.child, SQLBlock())(using InFromClause)
        val lateralViewExpr = group(
          wl(
            child,
            "lateral view",
            cl(lv.exprs.map(expr)),
            expr(lv.tableAlias),
            "as",
            cl(lv.columnAliases.map(expr))
          )
        )
        selectAll(lateralViewExpr, block)
      case r: Relation =>
        selectExpr(
          // Start a new nested SQLBlock
          indentedParen(relation(r, SQLBlock())(using InStatement))
        )
//      case other =>
//        unsupportedNode(s"relation ${other.nodeName}", other.span)
    end match

  end relation

  private def tableAliasOf(a: AliasedRelation)(using sc: SyntaxContext): Doc =
    val name = expr(a.alias)
    a.columnNames match
      case Some(columns) =>
        val cols = cl(columns.map(c => text(c.toSQLAttributeName)))
        name + paren(cols)
      case None =>
        name

  private def indented(d: Doc): Doc = nest(maybeNewline + d)

  private def selectAll(fromStmt: Doc, block: SQLBlock)(using sc: SyntaxContext): Doc =
    def renderSelect(input: Doc): Doc =
      val selectItems =
        if block.selectItems.isEmpty then
          text("*")
        else
          cl(block.selectItems.map(x => expr(x)))
      val selectExpr =
        wl(
          text("select"),
          if block.isDistinct then
            Some("distinct")
          else
            None
        ) + nest(wsOrNL + selectItems)

      val s = List.newBuilder[Doc]
      s += group(selectExpr)
      if !input.isEmpty then
        s += wl("from", input)

      if block.whereFilter.nonEmpty then
        val cond = Expression.concatWithAnd(block.whereFilter.map(_.filterExpr))
        s += group(wl("where", indented(expr(cond))))
      if block.groupingKeys.nonEmpty then
        s += group(wl("group by", indented(cl(block.groupingKeys.map(x => expr(x))))))
      if block.having.nonEmpty then
        val cond = Expression.concatWithAnd(block.having.map(_.filterExpr))
        s += group(wl("having", indented(expr(cond))))
      if block.orderBy.nonEmpty then
        s += group(wl("order by", indented(cl(block.orderBy.map(x => expr(x))))))
      if block.limit.nonEmpty then
        s += group(wl("limit", indented(expr(block.limit.get))))
      if block.offset.nonEmpty then
        s += group(wl("offset", indented(expr(block.offset.get))))

      val sql = lines(s.result())
      if sc.isNested then
        indentedParen(sql)
      else
        sql
    end renderSelect

    if block.isEmpty && sc.inFromClause then
      fromStmt
    else if sc.inFromClause then
      indentedParen(renderSelect(fromStmt))
    else
      renderSelect(fromStmt)

  end selectAll

  private def groupBy(g: GroupBy, block: SQLBlock)(using sc: SyntaxContext): Doc =
    // Translate GroupBy node without any projection (select) to Agg node
    def keys: List[Attribute] = g
      .groupingKeys
      .map { k =>
        val keyName =
          if k.name.isEmpty then
            NameExpr.fromString(formatter.render(0, expr(k)), k.span)
          else
            k.name
        SingleColumn(keyName, k.child, NoSpan)
      }
    // Add arbitrary(c1), arbitrary(c2), ... if no aggregation expr is given
    def defaultAggExprs: List[Attribute] = g
      .inputRelationType
      .fields
      .map { f =>
        // Use arbitrary(expr) for efficiency
        val ex: Expression = FunctionApply(
          NameExpr.fromString("arbitrary"),
          args = List(FunctionArg(None, NameExpr.fromString(f.toSQLAttributeName), false, NoSpan)),
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
              NameExpr.fromString(formatter.render(0, exprStr))
        SingleColumn(name, ex, NoSpan)
      }

    def selectItems: List[Attribute] =
      if block.selectItems.isEmpty then
        // Use the default agg exprs
        keys ++ defaultAggExprs
      else
        block.selectItems

    if block.acceptGroupingKeys then
      val newBlock = block.copy(selectItems = selectItems, groupingKeys = g.groupingKeys)
      val d        = relation(g.child, newBlock)
      d
    else
      // Start a new SELECT block
      val d = relation(g.child, SQLBlock(groupingKeys = g.groupingKeys))(using InSubQuery)
      selectAll(d, block)

  end groupBy

  private def values(values: Values)(using sc: SyntaxContext): Doc =
    val rows: List[Doc] = values
      .rows
      .map {
        case a: ArrayConstructor =>
          paren(cl(a.values.map(expr)))
        case other =>
          expr(other)
      }
    text("values") + nest(wsOrNL + cl(rows))

  private def pivotOnExpr(p: Pivot)(using sc: SyntaxContext): Doc = cl(
    p.pivotKeys
      .map { k =>
        val values = cl(k.values.map(v => expr(v)))
        if k.values.isEmpty then
          expr(k.name)
        else
          wl(expr(k.name), "in", paren(values))
      }
  )
  end pivotOnExpr

  def expr(expression: Expression)(using sc: SyntaxContext = InStatement): Doc =
    expression match
      case g: UnresolvedGroupingKey =>
        expr(g.child)
      case f: FunctionApply =>
        val base =
          f.base match
            case d: DoubleQuoteString =>
              // Handle double-quoted strings as identifiers when used as function names
              text(doubleQuoteIfNecessary(d.unquotedValue))
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
            s += wl(text(f.frameType.expr), "between", f.start.expr, "and", f.end.expr)
          }
        wl("over", paren(wl(s.result())))
      case Eq(left, n: NullLiteral, _) =>
        wl(expr(left), "is null")
      case NotEq(left, n: NullLiteral, _) =>
        wl(expr(left), "is not null")
      case IsNull(child, _) =>
        wl(expr(child), "is null")
      case IsNotNull(child, _) =>
        wl(expr(child), "is not null")
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
        expr(c.left) + wsOrNL + text(c.operatorName) + ws + expr(c.right)
      case b: BinaryExpression =>
        wl(expr(b.left), b.operatorName, expr(b.right))
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
        expr(s.sortKey) + s.ordering.map(x => ws + text(x.expr)) +
          s.nullOrdering.map(x => ws + text(x.expr))
      case s: SingleColumn =>
        expr(s.expr) match
          case left if s.nameExpr.isEmpty =>
            // no alias
            left
          case t @ Text(str) if str == s.nameExpr.toSQLAttributeName =>
            // alias is the same with the expression
            t
          case left =>
            wl(left, "as", s.nameExpr.toSQLAttributeName)
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
        text("if") + paren(cl(expr(i.cond), expr(i.onTrue), expr(i.onFalse)))
      case n: Not =>
        wl("not", expr(n.child))
      case l: ListExpr =>
        cl(l.exprs.map(x => expr(x)))
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        expr(qual) + "." + expr(name)
      case in: In =>
        val left = expr(in.a)
        val right =
          in.list match
            case (s: SubQueryExpression) :: Nil =>
              expr(s)
            case _ =>
              paren(cl(in.list.map(x => expr(x))))
        wl(left, "in", right)
      case notIn: NotIn =>
        val left = expr(notIn.a)
        val right =
          notIn.list match
            case (s: SubQueryExpression) :: Nil =>
              expr(s)
            case _ =>
              paren(cl(notIn.list.map(x => expr(x))))
        wl(left, "not in", right)
      case tupleIn: TupleIn =>
        generateTupleInExpression(tupleIn.tuple, tupleIn.list, "in")
      case tupleNotIn: TupleNotIn =>
        generateTupleInExpression(tupleNotIn.tuple, tupleNotIn.list, "not in")
      case a: ArrayConstructor =>
        dbType.arrayConstructorSyntax match
          case SQLDialect.ArraySyntax.ArrayPrefix =>
            text("ARRAY") + bracket(cl(a.values.map(expr(_))))
          case SQLDialect.ArraySyntax.ArrayLiteral =>
            bracket(cl(a.values.map(expr(_))))
      case r: RowConstructor =>
        paren(cl(r.values.map(expr(_))))
      case a: ArrayAccess =>
        expr(a.arrayExpr) + text("[") + expr(a.index) + text("]")
      case c: CaseExpr =>
        wl(
          wl("case", c.target.map(expr)),
          c.whenClauses
            .map { w =>
              nest(newline + wl("when", group(expr(w.condition)), "then", expr(w.result)))
            },
          c.elseClause
            .map { e =>
              nest(newline + wl("else", group(expr(e))))
            },
          maybeNewline + "end"
        )
      case l: LambdaExpr =>
        val args = cl(l.args.map(expr(_)))
        if l.args.size == 1 then
          args + ws + "->" + wsOrNL + expr(l.body)
        else
          paren(args) + ws + "->" + wsOrNL + expr(l.body)
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
            group(text(f.name) + ws + sqlType)
          }
        text("cast") + paren(cl(fields)) + wsOrNL + text("as") + ws + text("row") +
          paren(cl(schema))
      case s: StructValue =>
        val fields = s
          .fields
          .map { f =>
            group(text(f.name) + ":" + ws + expr(f.value))
          }
        brace(cl(fields))
      case m: MapValue =>
        def keyExpr(e: Expression): Expression =
          e match
            case d: DoubleQuotedIdentifier =>
              // Wvlet may produce DoubleQuotedIdentifier, which is not supported in SQL
              val s = d.unquotedValue.replaceAll("'", "''")
              SingleQuoteString(s, NoSpan)
            case _ =>
              e

        dbType.mapConstructorSyntax match
          case SQLDialect.MapSyntax.KeyValue =>
            val entries: List[Doc] = m
              .entries
              .map { e =>
                group(wl(expr(keyExpr(e.key)) + ":", expr(e.value)))
              }
            wl("MAP", brace(cl(entries)))
          case SQLDialect.MapSyntax.ArrayPair =>
            val keys   = ArrayConstructor(m.entries.map(x => keyExpr(x.key)), m.span)
            val values = ArrayConstructor(m.entries.map(_.value), m.span)
            text("MAP") + paren(cl(List(expr(keys), expr(values))))
      case b: Between =>
        wl(expr(b.e), "between", expr(b.a), "and", expr(b.b))
      case b: NotBetween =>
        wl(expr(b.e), "not between", expr(b.a), "and", expr(b.b))
      case c: Cast =>
        group(wl(text("cast") + paren(wl(expr(c.child), "as", text(c.tpe.typeName.name)))))
      case n: NativeExpression =>
        expr(ExpressionEvaluator.eval(n))
      case e: Exists =>
        wl(text("exists"), expr(e.child))
      case c: ColumnDef =>
        wl(
          expr(c.columnName),
          text(c.tpe.sqlExpr)
          // TODO add nullable or other constraints
          // c.nullable.map(x => text(x.expr)),
          // c.comment.map(x => text(x.expr))
        )
      case other =>
        unsupportedNode(s"Expression ${other.nodeName}", other.span)

  private def generateTupleInExpression(
      tuple: Expression,
      list: List[Expression],
      operator: String
  ): Doc =
    val left = expr(tuple)
    val right =
      list match
        case (s: SubQueryExpression) :: Nil =>
          expr(s)
        case _ =>
          paren(cl(list.map(x => expr(x))))
    wl(left, operator, right)

  private def unsupportedNode(nodeType: String, span: Span): Doc =
    val loc = ctx.sourceLocationAt(span)
    val msg = s"Unsupported ${nodeType} (${loc})"
    warn(msg)
    text(s"-- ${msg}")

end SqlGenerator

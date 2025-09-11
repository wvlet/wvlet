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
import wvlet.lang.model.plan.SamplingSize.{Percentage, PercentageExpr, Rows}
import wvlet.log.LogSupport
import SyntaxContext.*
import wvlet.lang.compiler.codegen.CodeFormatter
import wvlet.lang.compiler.codegen.CodeFormatter.*
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.model.DataType.{EmptyRelationType, NamedType, SchemaType}
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
      offset: Option[Expression] = None,
      partitionOptions: List[PartitionWriteOption] = Nil
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
      case c: CreateSchema =>
        group(
          wl(
            "create",
            "schema",
            if c.ifNotExists then
              Some("if not exists")
            else
              None
            ,
            expr(c.schema)
          )
        )
      case d: DropSchema =>
        group(
          wl(
            "drop",
            "schema",
            if d.ifExists then
              Some("if exists")
            else
              None
            ,
            expr(d.schema)
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
      case h: PartitioningHint =>
        // Generate CLUSTER BY, DISTRIBUTE BY, SORT BY clauses only for Hive
        if dbType == DBType.Hive then
          relation(h.child, block.copy(partitionOptions = h.partitionWriteOptions))
        else
          // For other databases, ignore the hints and just process the child
          relation(h.child, block)
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
        v.relationType match
          case s: SchemaType if s.isFullyNamed =>
            // VALUES with alias and schema
            val tableAlias: Doc = tableAliasOf(
              NameExpr.fromString(s.typeName.name),
              Some(s.columnTypes)
            )
            selectAll(group(wl(paren(values(v)), "as", tableAlias)), block)
          case _ =>
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
          // Both Trino and DuckDB support TABLESAMPLE in FROM clause, but with different percentage format
          if sc.inFromClause then
            dbType match
              case DBType.Trino =>
                // Trino: TABLESAMPLE BERNOULLI(5) - integer percentage
                val percentage =
                  s.size match
                    case Rows(n) =>
                      text(n.toString)
                    case Percentage(p) =>
                      text(p.toString.stripSuffix(".0"))
                    case PercentageExpr(e) =>
                      expr(e)
                group(
                  wl(
                    child,
                    "TABLESAMPLE",
                    text(samplingMethod.toString.toUpperCase),
                    paren(percentage)
                  )
                )
              case DBType.DuckDB =>
                // DuckDB: TABLESAMPLE BERNOULLI(5%) - percentage with % sign
                val percentage =
                  s.size match
                    case Rows(n) =>
                      text(s"${n}%") // Convert rows to percentage for DuckDB
                    case Percentage(p) =>
                      text(s"${p.toString.stripSuffix(".0")}%")
                    case PercentageExpr(e) =>
                      expr(e)
                group(
                  wl(
                    child,
                    "TABLESAMPLE",
                    text(samplingMethod.toString.toUpperCase),
                    paren(percentage)
                  )
                )
              case _ =>
                warn(s"Unsupported TABLESAMPLE for ${dbType}, falling back to SELECT wrapper")
                // Fallback to SELECT wrapper for unsupported databases
                val size =
                  s.size match
                    case Rows(n) =>
                      text(s"${n} rows")
                    case Percentage(percentage) =>
                      text(s"${percentage}%")
                    case PercentageExpr(e) =>
                      expr(e)
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
          else
            // Non-FROM contexts: use function-based sampling
            dbType match
              case DBType.Trino =>
                s.size match
                  case Rows(n) =>
                    // Supported only in td-trino
                    group(wl("select", cl("*", s"reservoir_sample(${n}) over()"), "from", child))
                  case Percentage(percentage) =>
                    // Use TABLESAMPLE with consistent formatting for non-FROM Trino contexts
                    group(
                      wl(
                        "select",
                        "*",
                        "from",
                        child,
                        "TABLESAMPLE",
                        text(samplingMethod.toString.toUpperCase),
                        paren(text(percentage.toString.stripSuffix(".0")))
                      )
                    )
                  case PercentageExpr(e) =>
                    // Use TABLESAMPLE with expression for non-FROM Trino contexts
                    group(
                      wl(
                        "select",
                        "*",
                        "from",
                        child,
                        "TABLESAMPLE",
                        text(samplingMethod.toString.toUpperCase),
                        paren(expr(e))
                      )
                    )
              case DBType.DuckDB =>
                // DuckDB uses USING SAMPLE syntax for non-FROM contexts
                val size =
                  s.size match
                    case Rows(n) =>
                      text(s"${n} rows")
                    case Percentage(percentage) =>
                      text(s"${percentage}%")
                    case PercentageExpr(e) =>
                      expr(e)
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
              case _ =>
                warn(s"Unsupported sampling method: ${samplingMethod} for ${dbType}")
                child

        if sc.inFromClause then
          body
        else
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
      case s: Show if s.showType == ShowType.columns =>
        val parts       = s.inExpr.nameParts.reverse
        val tableName   = parts.headOption
        val schemaName  = parts.lift(1)
        val catalogName = parts.lift(2)

        val cond = List.newBuilder[Expression]
        tableName.foreach(t =>
          cond +=
            Eq(
              UnquotedIdentifier("table_name", NoSpan),
              StringLiteral.fromString(t, NoSpan),
              NoSpan
            )
        )
        schemaName.foreach(s =>
          cond +=
            Eq(
              UnquotedIdentifier("table_schema", NoSpan),
              StringLiteral.fromString(s, NoSpan),
              NoSpan
            )
        )
        catalogName.foreach(c =>
          cond +=
            Eq(
              UnquotedIdentifier("table_catalog", NoSpan),
              StringLiteral.fromString(c, NoSpan),
              NoSpan
            )
        )

        val conds = cond.result()
        val wherePart =
          if conds.isEmpty then
            empty
          else
            group(wl("where", expr(Expression.concatWithAnd(conds))))

        val sql = lines(
          List(
            group(wl("select", cl("column_name", "data_type", "is_nullable", "column_default"))),
            group(wl("from", "information_schema.columns")),
            wherePart,
            group(wl("order by", "ordinal_position"))
          )
        )
        selectExpr(sql)
      case s: Show if s.showType == ShowType.models =>
        // TODO: Show models should be handled outside of GenSQL
        // Collect all models from all contexts, then deduplicate by name (keeping the most recent)
        val allModels = ctx
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
          }

        // Deduplicate models by name. Since SymbolLabeler updates existing symbols when
        // models are redefined, all duplicate ModelSymbolInfo instances will reference
        // the latest definition, so picking any one (head) is sufficient.
        val modelsByName = allModels.groupBy(_.name.name)
        val models: Seq[ListMap[String, Any]] = modelsByName
          .values
          .map(_.head)
          .toSeq
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
      case s: Show if s.showType == ShowType.functions =>
        // SHOW FUNCTIONS - database-specific implementation
        dbType match
          case DBType.DuckDB =>
            // DuckDB uses duckdb_functions() function
            val sql = wl(
              "select",
              cl("function_name as name", "description"),
              "from",
              "duckdb_functions()",
              "order by name"
            )
            selectExpr(sql)
          case DBType.Trino =>
            // Trino supports SHOW FUNCTIONS directly
            val sql = wl("show functions")
            selectExpr(sql)
          case _ =>
            // Fallback for other databases - return empty result with proper schema
            val sql = wl(
              "select",
              "cast(null as varchar) as name,",
              "cast(null as varchar) as description",
              "where false"
            )
            selectExpr(sql)
      case s: Show if s.showType == ShowType.createView =>
        // SHOW CREATE VIEW - delegate to database-specific implementation
        val viewName = s.inExpr.nameParts.mkString(".")
        dbType match
          case DBType.Trino =>
            // Trino supports SHOW CREATE VIEW directly
            val sql = wl("show create view", viewName)
            selectExpr(sql)
          case _ =>
            // For other databases, use DESCRIBE which is more widely supported
            val sql = wl("describe", viewName)
            selectExpr(sql)
      case lv: LateralView =>
        // Hive LATERAL VIEW syntax
        val child = relation(lv.child, SQLBlock())(using InFromClause)
        val lateralViewKeywords =
          if lv.isOuter then
            "lateral view outer"
          else
            "lateral view"
        val lateralViewExpr = group(
          wl(
            child,
            lateralViewKeywords,
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

  private def tableAliasOf(alias: NameExpr, columnNames: Option[List[NamedType]])(using
      sc: SyntaxContext
  ): Doc =
    val name = expr(alias)
    columnNames match
      case Some(columns) =>
        val cols = cl(
          columns.map { c =>
            text(c.toSQLAttributeName)
          }
        )
        name + paren(cols)
      case None =>
        name

  private def tableAliasOf(a: AliasedRelation)(using sc: SyntaxContext): Doc = tableAliasOf(
    a.alias,
    a.columnNames
  )

  private def indented(d: Doc): Doc = nest(maybeNewline + d)

  private def selectAll(fromStmt: Doc, block: SQLBlock)(using sc: SyntaxContext): Doc =
    def renderSelect(input: Doc): Doc =
      val selectItems =
        if block.selectItems.isEmpty then
          text("*")
        else
          cl(
            block
              .selectItems
              .map { x =>
                expr(x)
              }
          )
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

      // Add Hive partition clauses
      if block.partitionOptions.nonEmpty then
        for option <- block.partitionOptions do
          option.mode match
            case PartitionWriteMode.HIVE_CLUSTER_BY =>
              s += group(wl("cluster by", indented(cl(option.expressions.map(x => expr(x))))))
            case PartitionWriteMode.HIVE_DISTRIBUTE_BY =>
              s += group(wl("distribute by", indented(cl(option.expressions.map(x => expr(x))))))
            case PartitionWriteMode.HIVE_SORT_BY =>
              s += group(wl("sort by", indented(cl(option.sortItems.map(x => expr(x))))))

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
          args = List(
            FunctionArg(None, NameExpr.fromString(f.toSQLAttributeName), false, Nil, NoSpan)
          ),
          None,
          None,
          None,
          NoSpan
        )
        val name: NameExpr =
          dbType match
            case DBType.DuckDB =>
              // DuckDB generates human-friendly column name
              EmptyName
            case _ =>
              // Use the column name directly to avoid nested quotes in aggregated labels
              val columnName = f.name.name
              NameExpr.fromString(s"arbitrary(${columnName})")
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
      case gs: GroupingSets =>
        val sets = gs
          .groupingSets
          .map { set =>
            if set.isEmpty then
              text("()")
            else
              paren(cl(set.map(k => expr(k))))
          }
        wl(text("grouping sets"), paren(cl(sets)))
      case c: Cube =>
        if c.groupingKeys.isEmpty then
          text("cube ()")
        else
          wl(text("cube"), paren(cl(c.groupingKeys.map(k => expr(k)))))
      case r: Rollup =>
        if r.groupingKeys.isEmpty then
          text("rollup ()")
        else
          wl(text("rollup"), paren(cl(r.groupingKeys.map(k => expr(k)))))
      case f: FunctionApply =>
        // Special handling for specific functions
        f.base match
          case id: UnquotedIdentifier if id.unquotedValue.toLowerCase == "trim" =>
            // Generate special TRIM syntax
            val parts = List.newBuilder[Doc]
            parts += text("trim")
            parts += text("(")

            // Parse the arguments to reconstruct TRIM syntax
            var trimType: Option[String]      = None
            var trimChars: Option[Expression] = None
            var fromExpr: Option[Expression]  = None

            f.args
              .foreach { arg =>
                arg.name match
                  case Some(name) if name.name == "from" =>
                    fromExpr = Some(arg.value)
                  case _ =>
                    arg.value match
                      case id: UnquotedIdentifier if id.unquotedValue.toUpperCase == "LEADING" =>
                        trimType = Some("LEADING")
                      case id: UnquotedIdentifier if id.unquotedValue.toUpperCase == "TRAILING" =>
                        trimType = Some("TRAILING")
                      case id: UnquotedIdentifier if id.unquotedValue.toUpperCase == "BOTH" =>
                        trimType = Some("BOTH")
                      case _ =>
                        if trimChars.isEmpty && fromExpr.isEmpty then
                          // First non-keyword argument is either trimChars or the string to trim
                          if f.args.exists(_.name.exists(_.name == "from")) then
                            trimChars = Some(arg.value)
                          else
                            fromExpr = Some(arg.value)
                        else if trimChars.isEmpty then
                          trimChars = Some(arg.value)
              }

            // Build the TRIM expression
            val trimParts = List.newBuilder[Doc]
            trimType.foreach(t => trimParts += text(t))
            trimChars.foreach(chars => trimParts += expr(chars))
            if trimType.isDefined || trimChars.isDefined then
              trimParts += text("from")

            fromExpr.foreach(e => trimParts += expr(e))

            parts += wl(trimParts.result()*)
            parts += text(")")

            val baseResult = group(concat(parts.result()))
            // Add FILTER clause if present for TRIM
            val result = f
              .filter
              .map(filterExpr => baseResult + wl("filter", paren(wl("where", expr(filterExpr)))))
              .getOrElse(baseResult)
            f.window.map(w => wl(result, expr(w))).getOrElse(result)

          case baseId: Identifier if baseId.unquotedValue.toLowerCase == "map" =>
            // Normalize MAP constructor across dialects and argument styles
            val args = f.args.map(_.value)
            args match
              // Two-array form
              case List(ArrayConstructor(keys, _), ArrayConstructor(values, _)) =>
                dbType.mapConstructorSyntax match
                  case SQLDialect.MapSyntax.KeyValue =>
                    val entries = keys
                      .zip(values)
                      .map { case (k, v) =>
                        group(wl(expr(k) + ":", expr(v)))
                      }
                    wl("MAP", brace(cl(entries)))
                  case SQLDialect.MapSyntax.ArrayPair =>
                    text("MAP") +
                      paren(
                        cl(
                          List(
                            expr(ArrayConstructor(keys, f.span)),
                            expr(ArrayConstructor(values, f.span))
                          )
                        )
                      )
              // Variadic key/value arguments
              case _ =>
                if args.length % 2 != 0 then
                  throw IllegalArgumentException(
                    s"The variadic map function must have an even number of arguments, but got ${args
                        .length}"
                  )
                dbType.mapConstructorSyntax match
                  case SQLDialect.MapSyntax.KeyValue =>
                    val entries: List[Doc] = args
                      .grouped(2)
                      .toList
                      .collect { case List(k, v) =>
                        group(wl(expr(k) + ":", expr(v)))
                      }
                    wl("MAP", brace(cl(entries)))
                  case SQLDialect.MapSyntax.ArrayPair =>
                    val (keys, values) =
                      args
                        .grouped(2)
                        .toList
                        .collect { case List(k, v) =>
                          (k, v)
                        }
                        .unzip
                    val keysArr   = ArrayConstructor(keys, f.span)
                    val valuesArr = ArrayConstructor(values, f.span)
                    text("MAP") + paren(cl(List(expr(keysArr), expr(valuesArr))))
            end match

          case _ =>
            // Regular function handling
            val base =
              f.base match
                case d: DoubleQuoteString =>
                  // Handle double-quoted strings as identifiers when used as function names
                  text(doubleQuoteIfNecessary(d.unquotedValue))
                case id: UnquotedIdentifier =>
                  // Output unquoted identifiers as-is for function names
                  text(id.unquotedValue)
                case other =>
                  expr(other)
            val args = paren(cl(f.args.map(x => expr(x))))
            val stem = base + args
            // Add FILTER clause if present
            val withFilter = f
              .filter
              .map(filterExpr => stem + wl("filter", paren(wl("where", expr(filterExpr)))))
              .getOrElse(stem)
            val w = f.window.map(x => expr(x))
            wl(withFilter, w)
      case w: WindowApply =>
        val base   = expr(w.base)
        val window = expr(w.window)
        w.nullTreatment match
          case Some(nullTreatment) =>
            dbType match
              case DBType.Trino =>
                // Trino style: function(...) IGNORE NULLS OVER (...)
                wl(base, text(nullTreatment.expr), window)
              case DBType.DuckDB =>
                // DuckDB style: function(... IGNORE NULLS) OVER (...)
                // Modify the function to include null treatment inside the parentheses
                val modifiedBase =
                  base match
                    case f: FunctionApply =>
                      val functionName = expr(f.base)
                      val argsWithNullTreatment =
                        if f.args.nonEmpty then
                          // Apply null treatment to the first argument only
                          cl(
                            (expr(f.args.head) + ws + text(nullTreatment.expr)) ::
                              f.args.tail.map(expr).toList
                          )
                        else
                          text(nullTreatment.expr)
                      val funcCall = functionName + paren(argsWithNullTreatment)
                      funcCall
                    case other =>
                      base
                wl(modifiedBase, window)
              case _ =>
                // Default to Trino style for other databases
                wl(base, text(nullTreatment.expr), window)
          case None =>
            wl(base, window)
        end match
      case f: FunctionArg =>
        // TODO handle arg name mapping
        val parts = List.newBuilder[Doc]

        // Add DISTINCT if present
        if f.isDistinct then
          parts += text("distinct")

        // Add the main expression
        parts += expr(f.value)

        // Add ORDER BY if present
        if f.orderBy.nonEmpty then
          parts += text("order by")
          parts += cl(f.orderBy.map(x => expr(x)))

        wl(parts.result()*)
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
      case DistinctFrom(left, right, _) =>
        wl(expr(left), "is distinct from", expr(right))
      case NotDistinctFrom(left, right, _) =>
        wl(expr(left), "is not distinct from", expr(right))
      case a: ArithmeticUnaryExpr =>
        a.sign match
          case Sign.NoSign =>
            expr(a.child)
          case Sign.Positive =>
            text("+") + expr(a.child)
          case Sign.Negative =>
            text("-") + expr(a.child)
      case l: LikeExpression =>
        val escapeClause = l.escape.map(e => ws + text("ESCAPE") + ws + expr(e)).getOrElse(empty)
        expr(l.left) + ws + text(l.operatorName) + ws + expr(l.right) + escapeClause
      case r: RLikeExpression =>
        // Handle RLIKE based on database support
        if dbType.supportRLike then
          expr(r.left) + ws + text(r.operatorName) + ws + expr(r.right)
        else
          // For databases that don't support RLIKE (e.g., DuckDB), use regexp_matches
          r match
            case _: RLike =>
              text("regexp_matches") + paren(cl(expr(r.left), expr(r.right)))
            case _: NotRLike =>
              text("NOT") + ws + text("regexp_matches") + paren(cl(expr(r.left), expr(r.right)))
      case c: LogicalConditionalExpression =>
        // For adding optional newlines for AND/OR
        expr(c.left) + wsOrNL + text(c.operatorName) + ws + expr(c.right)
      case b: BinaryExpression =>
        wl(expr(b.left), b.operatorName, expr(b.right))
      case s: StringPart =>
        text(s.stringValue)
      case j: JsonLiteral =>
        dbType match
          case DBType.Trino =>
            text(s"json '${j.value}'")
          case DBType.DuckDB =>
            text(s"'${j.value}'::JSON")
          case _ =>
            // Default to Trino syntax for other databases
            text(s"json '${j.value}'")
      case i: IntervalLiteral =>
        dbType match
          case DBType.DuckDB =>
            // DuckDB uses INTERVAL 1 DAY (no quotes around the value)
            val signStr  = Option.when(i.sign != Sign.NoSign)(s"${i.sign.symbol} ").getOrElse("")
            val valueStr = i.value.stripPrefix("'").stripSuffix("'") // Remove quotes if present
            val endStr   = i.end.map(f => s" TO ${f}").getOrElse("")
            text(s"INTERVAL ${signStr}${valueStr} ${i.startField}${endStr}")
          case _ =>
            // Trino and others use INTERVAL '1' DAY (with quotes)
            text(i.sqlExpr)
      case l: Literal =>
        text(l.sqlExpr)
      case bq: BackQuotedIdentifier =>
        // SQL needs to use double quotes for back-quoted identifiers, which represents table or column names
        text("\"") + bq.unquotedValue + text("\"")
      case i: Identifier =>
        text(i.toSQLAttributeName)
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
      case j: JsonObjectConstructor =>
        // Special handling for JSON_OBJECT based on database type
        dbType match
          case DBType.DuckDB =>
            // DuckDB always uses: json_object('key1', value1, 'key2', value2)
            val params = j
              .jsonParams
              .map { p =>
                group(cl(expr(p.key), expr(p.value)))
              }
            text("json_object") + paren(cl(params*))
          case _ =>
            val params = j
              .jsonParams
              .map { p =>
                group(
                  wl(group(wl(text("KEY"), expr(p.key))), group(wl(text("VALUE"), expr(p.value))))
                )
              }
            val modifiers = wl(
              j.jsonObjectModifiers
                .map { m =>
                  text(m.expr)
                }
            )
            text("json_object") + paren(wl(params, modifiers))
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
      case e: Extract =>
        wl("extract", paren(wl(text(e.interval.name.toUpperCase), "from", expr(e.expr))))
      case c: Cast =>
        val castKeyword =
          if c.tryCast then
            "try_cast"
          else
            "cast"

        def formatTypeForDB(t: DataType): String =
          val generic = t.sqlExpr
          dbType match
            case DBType.DuckDB =>
              // Convert row(...) to struct(...), and array(T) to T[]
              val rowToStruct = generic.replaceAll("(?i)\\brow\\(", "struct(")
              // Use regex for a more robust array type conversion
              val arrayPattern = "(?i)^array\\((.*)\\)$".r
              rowToStruct match
                case arrayPattern(inner) =>
                  s"${inner}[]"
                case _ =>
                  rowToStruct
            case _ =>
              generic

        group(wl(text(castKeyword) + paren(wl(expr(c.child), "as", text(formatTypeForDB(c.tpe))))))
      case a: AtTimeZone =>
        expr(a.expr) + ws + text("AT") + ws + text("TIME") + ws + text("ZONE") + ws +
          expr(a.timezone)
      case n: NativeExpression =>
        expr(ExpressionEvaluator.eval(n))
      case e: Exists =>
        wl(text("exists"), expr(e.child))
      case c: ColumnDef =>
        val baseColumn = wl(expr(c.columnName), text(c.tpe.sqlExpr))
        val columnAttributes =
          List(
            // Add NOT NULL constraint
            Option.when(c.notNull)(text("not null")),
            // Add COMMENT attribute
            c.comment.map(comment => wl(text("comment"), text(s"'${comment.replace("'", "''")}'"))),
            // Add DEFAULT value
            c.defaultValue.map(default => wl(text("default"), expr(default))),
            // Add WITH properties
            Option.when(c.properties.nonEmpty)(
              wl(
                text("with"),
                paren(
                  cl(
                    c.properties
                      .map { case (key, value) =>
                        wl(expr(key), text("="), expr(value))
                      }
                  )
                )
              )
            )
          ).flatten

        if columnAttributes.nonEmpty then
          wl(baseColumn +: columnAttributes)
        else
          baseColumn
      case l: LikeTableDef =>
        if l.includeProperties then
          wl("like", expr(l.tableName), "including", "properties")
        else
          // Default is EXCLUDING PROPERTIES, so we can omit it for cleaner output
          wl("like", expr(l.tableName))
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

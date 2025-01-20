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

  def toDoc(l: LogicalPlan): Doc =
    def iter(plan: LogicalPlan): Doc =
      plan match
        case r: Relation =>
          query(r, Nil)(using InStatement)
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
      parenBlock(d)
    else
      d

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

  private def addProjectionIfMissing(r: Relation)(using sc: SyntaxContext): Relation =
    if !hasSelection(r) then
      Project(r, List(SingleColumn(EmptyName, Wildcard(NoSpan), NoSpan)), NoSpan)
    else
      r

  /**
    * Print a query matching with SELECT statement in SQL
    * @param r
    * @param remainingParents
    * @param sc
    * @return
    */
  private def query(r: Relation, remainingParents: List[Relation])(using sc: SyntaxContext): Doc =
    val p = addProjectionIfMissing(r)
    relation(p, remainingParents)

  /**
    * Print Relation nodes while tracking the parent nodes (e.g., filter, sort) for merging them
    * later into a single SELECT statement.
    * @param r
    * @param remainingParents
    *   unprocessed parent nodes
    * @param sc
    * @return
    */
  def relation(r: Relation, remainingParents: List[Relation])(using sc: SyntaxContext): Doc =

    r match
      case q: Query =>
        query(q.body, remainingParents)
      case g: GroupBy =>
        // If there has been no Projection or Agg before GroupBy, it reaches here
        groupBy(g, remainingParents)
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = pivotOnExpr(p)
        val aggItems = a.selectItems.map(x => expr(x)).mkString(", ")
        val pivotExpr =
          val child = relation(p.child, remainingParents)(using InSubQuery)
          group(
            text("pivot") + whitespaceOrNewline + child + whitespaceOrNewline +
              ws(text("on"), onExpr) + whitespaceOrNewline + ws(text("using"), aggItems)
          )
        if p.groupingKeys.isEmpty then
          wrapWithParenIfNecessary(pivotExpr)
        else
          val groupByItems = p.groupingKeys.map(x => expr(x)).mkString(", ")
          wrapWithParenIfNecessary(s"${pivotExpr}\n  group by ${groupByItems}")
      case s: Selection =>
        select(s, remainingParents)
      case j: Join =>
        val asof =
          if j.asof then
            if dbType.supportAsOfJoin then
              "asof "
            else
              throw StatusCode.SYNTAX_ERROR.newException(s"AsOf join is not supported in ${dbType}")
          else
            ""

        val l = relation(j.left, remainingParents)(using sc.enterJoin)
        val r = relation(j.right, Nil)(using sc.enterJoin)
        val c =
          j.cond match
            case NoJoinCriteria =>
              ""
            case NaturalJoin(_) =>
              ""
            case u: JoinOnTheSameColumns =>
              s" using(${u.columns.map(_.fullName).mkString(", ")})"
            case JoinOn(expr, _) =>
              s" on ${expr(expr)}"
            case JoinOnEq(keys, _) =>
              s" on ${expr(Expression.concatWithEq(keys))}"
        val joinSQL =
          j.joinType match
            case InnerJoin =>
              s"${l} ${asof}join ${r}${c}"
            case LeftOuterJoin =>
              s"${l} ${asof}left join ${r}${c}"
            case RightOuterJoin =>
              s"${l} ${asof}right join ${r}${c}"
            case FullOuterJoin =>
              s"${l} ${asof}full outer join ${r}${c}"
            case CrossJoin =>
              s"${l} ${asof}cross join ${r}${c}"
            case ImplicitJoin =>
              s"${l}, ${r}${c}"

        joinSQL
      case s: SetOperation =>
        val rels: List[String] =
          s.children.toList match
            case Nil =>
              Nil
            case head :: tail =>
              val hd = query(head, remainingParents)(using sc)
              val tl = tail.map(x => query(x, Nil)(using sc))
              hd :: tl
        val op  = s.toSQLOp
        val sql = rels.mkString(s"\n${op}\n")
        selectWithIndentAndParenIfNecessary(sql)
      case p: Pivot => // pivot without explicit aggregations
        selectWithIndentAndParenIfNecessary(
          s"pivot ${relation(p.child, remainingParents)(using sc.enterFrom)}\n  on ${pivotOnExpr(
              p
            )}"
        )
      case d: Debug =>
        // Skip debug expression
        relation(d.inputRelation, remainingParents)
      case d: Dedup =>
        selectWithIndentAndParenIfNecessary(
          s"""select distinct * from ${relation(d.child, remainingParents)(using sc.enterFrom)}"""
        )
      case s: Sample =>
        val child = relation(s.child, remainingParents)(using sc.enterFrom)
        val body: String =
          dbType match
            case DBType.DuckDB =>
              val size =
                s.size match
                  case Rows(n) =>
                    s"${n} rows"
                  case Percentage(percentage) =>
                    s"${percentage}%"
              s"select * from ${child} using sample ${s.method.toString.toLowerCase}(${size})"
            case DBType.Trino =>
              s.size match
                case Rows(n) =>
                  // Supported only in td-trino
                  s"select *, reservoir_sample(${n}) over() from ${child}"
                case Percentage(percentage) =>
                  s"select * from ${child} TABLESAMPLE ${s
                      .method
                      .toString
                      .toLowerCase()}(${percentage})"
            case _ =>
              warn(s"Unsupported sampling method: ${s.method} for ${dbType}")
              child
        selectWithIndentAndParenIfNecessary(body)
      case a: AliasedRelation =>
        val tableAlias: String =
          val name = expr(a.alias)
          a.columnNames match
            case Some(columns) =>
              s"${name}(${columns.map(x => s"${x.toSQLAttributeName}").mkString(", ")})"
            case None =>
              name

        a.child match
          case t: TableInput =>
            s"${expr(t.sqlExpr)} as ${tableAlias}"
          case v: Values =>
            s"${values(v)} as ${tableAlias}"
//          case v: Values if sc.nestingLevel > 0 && sc.withinJoin =>
//            // For joins, expose table column aliases to the outer scope
//            s"${selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")} as ${a.alias.fullName}"
          case _ =>
            indent(s"${relation(a.child, remainingParents)(using sc.nested)} as ${tableAlias}")
      case p: BracedRelation =>
        def inner = relation(p.child, Nil)
        p.child match
          case v: Values =>
            // No need to wrap values query
            inner
          case AliasedRelation(v: Values, _, _, _) =>
            inner
          case _ =>
            val body = query(p.child, Nil)(using sc.nested)
            selectWithIndentAndParenIfNecessary(body)
      case t: TestRelation =>
        relation(t.inputRelation, remainingParents)(using sc)
      case q: WithQuery =>
        val subQueries = q
          .queryDefs
          .map { w =>
            val subQuery = query(w.child, Nil)(using Indented(0))
            s"${expr(w.alias)} as (\n${subQuery}\n)"
          }
        val body     = query(q.queryBody, remainingParents)(using sc)
        val withStmt = subQueries.mkString("with ", ", ", "")
        s"""${withStmt}
           |${body}
           |""".stripMargin
      case f: FilteringRelation =>
        // Sort, Offset, Limit, Filter, Distinct, etc.
        relation(f.child, f :: remainingParents)
      case t: TableInput =>
        expr(t.sqlExpr)
      case v: Values =>
        values(v)
      case s: SelectAsAlias =>
        relation(s.child, remainingParents)
      case d: Describe =>
        // TODO: Compute schema only from local DataType information without using connectors
        // Trino doesn't support nesting describe statement, so we need to generate raw values as SQL
        val fields = d.child.relationType.fields

        val sql =
          if fields.isEmpty then
            "select '' as column_name, '' as column_type limit 0"
          else
            val values = fields
              .map { f =>
                s"""('${f.name.name}','${f.dataType.typeName}')"""
              }
              .mkString(",")
            s"select * from (values ${values}) as table_schema(column_name, column_type)"

        // TODO Wrap this query with the remaining parents
        selectWithIndentAndParenIfNecessary(sql)
      case r: RelationInspector =>
        // Skip relation inspector
        // TODO Dump output to the file logger
        relation(r.child, remainingParents)
      case s: Show if s.showType == ShowType.tables =>
        val sql  = s"select table_name as name from information_schema.tables"
        val cond = List.newBuilder[Expression]

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
            s"${sql} where ${expr(Expression.concatWithAnd(conds))}"
        selectWithIndentAndParenIfNecessary(s"${body} order by name")
      case s: Show if s.showType == ShowType.schemas =>
        val sql =
          s"""select catalog_name as "catalog", schema_name as name from information_schema.schemata"""
        val cond = List.newBuilder[Expression]

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
            s"${sql} where ${expr(Expression.concatWithAnd(conds))}"
        selectWithIndentAndParenIfNecessary(s"${body} order by name")
      case s: Show if s.showType == ShowType.catalogs =>
        val sql = s"""select distinct catalog_name as "name" from information_schema.schemata"""
        selectWithIndentAndParenIfNecessary(s"${sql} order by 1")
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
        if modelValues.isEmpty then
          selectWithIndentAndParenIfNecessary(
            "select cast(null as varchar) as name, cast(null as varchar) as args, cast(null as varchar) as package_name limit 0"
          )
        else
          selectWithIndentAndParenIfNecessary(
            s"select * from (values ${indent(
                modelValues.mkString(", ")
              )}) as __models(name, args, package_name)"
          )
      case other =>
        warn(s"unsupported relation type: ${other}")
        other.pp
    end match

  end relation

  private def select(r: Relation, parents: List[Relation])(using sc: SyntaxContext): Doc =

    var remainingParents     = parents
    var having: List[Filter] = Nil

    parents.headOption match
      case Some(f: Filter) =>
        r match
          case a: Agg =>
            having = List(f)
            remainingParents = parents.tail
          case g: GroupBy =>
            having = List(f)
            remainingParents = parents.tail
          case _ =>
      case _ =>

    def findParentFilters(lst: List[Relation]): List[Filter] =
      lst match
        case (f: Filter) :: tail =>
          remainingParents = tail
          f :: findParentFilters(tail)
        case tail =>
          Nil
    val parentFilters: List[Filter] = findParentFilters(remainingParents)
    val selectItems: List[Attribute] =
      r match
        case s: Selection =>
          s.selectItems.toList
        case _ =>
          Nil
    val inputRelation =
      r match
        case s: Selection =>
          s.child
        case _ =>
          r

    var orderBy: List[SortItem]   = Nil
    var limit: Option[Expression] = None

    def processSortAndLimit(lst: List[Relation]): List[Relation] =
      lst match
        case (st: Sort) :: (l: Limit) :: tail =>
          orderBy = st.orderBy.toList
          limit = Some(l.limit)
          tail
        case (st: Sort) :: tail =>
          orderBy = st.orderBy.toList
          tail
        case (l: Limit) :: tail =>
          limit = Some(l.limit)
          tail
        case other =>
          other

    remainingParents = processSortAndLimit(remainingParents)

    val sqlSelect = sqlSelect(
      SQLSelect(inputRelation, selectItems, Nil, having, parentFilters, orderBy, limit, r.span),
      inputRelation
    )
    val hasDistinct =
      r match
        case d: Distinct =>
          true
        case _ =>
          false

    // SELECT distinct? ...
    val selectItemsExpr =
      if selectItems.isEmpty then
        "*"
      else
        sqlSelect.selectItems.map(x => expr(x)).mkString(", ")
    val s = Seq.newBuilder[String]
    s +=
      s"select ${
          if hasDistinct then
            "distinct "
          else
            ""
        }${selectItemsExpr}"

    sqlSelect.child match
      case e: EmptyRelation =>
      // Do not add from clause for empty inputs
      case t: TableInput =>
        s += s"from ${expr(t.sqlExpr)}"
      case _ =>
        // Start a new SELECT statement inside FROM
        s += s"from ${relation(sqlSelect.child, Nil)(using sc.enterFrom)}"
    if sqlSelect.filters.nonEmpty then
      val filterExpr = Expression.concatWithAnd(sqlSelect.filters.map(x => x.filterExpr))
      s += s"where ${expr(filterExpr)}"
    if sqlSelect.groupingKeys.nonEmpty then
      s += s"group by ${sqlSelect.groupingKeys.map(x => expr(x)).mkString(", ")}"
    if sqlSelect.having.nonEmpty then
      s += s"having ${sqlSelect.having.map(x => expr(x.filterExpr)).mkString(", ")}"
    if sqlSelect.orderBy.nonEmpty then
      s += s"order by ${sqlSelect.orderBy.map(x => expr(x)).mkString(", ")}"
    if sqlSelect.limit.nonEmpty then
      s += s"limit ${expr(sqlSelect.limit.get)}"

    val body = selectWithIndentAndParenIfNecessary(s"${s.result().mkString("\n")}")
    if remainingParents.isEmpty then
      body
    else
      warn(
        s"Unprocessed parents:\n${remainingParents.mkString(" - ", "\n - ", "")} (${ctx
            .compilationUnit
            .sourceFile
            .fileName})"
      )
      body
  end select

  private def sqlSelect(agg: SQLSelect, r: Relation): SQLSelect =
    def collectFilter(plan: Relation): (List[Filter], Relation) =
      plan match
        case f: Filter =>
          val (filters, child) = collectFilter(f.inputRelation)
          (f :: filters, child)
        case other =>
          (Nil, other)
    end collectFilter

    // pull-up aggregation node
    // Filter(Filter(GroupBy ...))  => GROUP BY ... HAVING ...
    // GroupBy(Filter(Filter(...)) => WHERE ... GROUP BY ...
    r match
      case f: Filter =>
        val (filters, lastNode) = collectFilter(f)
        lastNode match
          case a: GroupBy =>
            agg.copy(child = a.child, groupingKeys = a.groupingKeys, having = filters)
          case other =>
            agg.copy(child = other, filters = agg.filters ++ filters)
      case p: Project =>
        agg
      case a: Agg =>
        agg
      case p: Pivot =>
        agg
      case a: GroupBy =>
        val (filters, lastNode) = collectFilter(a.child)
        agg.copy(child = a.child, groupingKeys = a.groupingKeys, filters = agg.filters ++ filters)
      case _ =>
        agg
  end sqlSelect

  private def groupBy(g: GroupBy, parents: List[Relation])(using sc: SyntaxContext): Doc =
    // Translate GroupBy node without any projection (select) to Agg node
    val keys: List[Attribute] = g
      .groupingKeys
      .map { k =>
        val keyName = expr(k)
        SingleColumn(NameExpr.fromString(keyName), k.name, NoSpan)
      }
    val aggExprs: List[Attribute] =
      g.inputRelationType
        .fields
        .map { f =>
          // Use arbitrary(expr) for efficiency
          val expr: Expression = FunctionApply(
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
                val exprStr = expr(expr)
                NameExpr.fromString(exprStr)
          SingleColumn(name, expr, NoSpan)
        }
        .toList

    val agg = Agg(g, keys, aggExprs, g.span)
    relation(agg, parents)
  end groupBy

  private def values(values: Values)(using sc: SyntaxContext): String =
    val rows = values
      .rows
      .map { row =>
        row match
          case a: ArrayConstructor =>
            val elems = a.values.map(x => expr(x)).mkString(", ")
            s"(${elems})"
          case other =>
            expr(other)
      }
      .mkString(", ")
    s"(values ${rows})"

  private def pivotOnExpr(p: Pivot)(using sc: SyntaxContext): Doc = p
    .pivotKeys
    .map { k =>
      val values = k.values.map(v => expr(v)).mkString(", ")
      if values.isEmpty then
        s"${expr(k.name)}"
      else
        s"${expr(k.name)} in (${values})"
    }
    .mkString(", ")
  end pivotOnExpr

  def expr(expression: Expression)(using sc: SyntaxContext = InStatement): Doc =
    expression match
      case g: UnresolvedGroupingKey =>
        expr(g.child)
      case f: FunctionApply =>
        val base = expr(f.base)
        val args = f.args.map(x => expr(x)).mkString(", ")
        val w    = f.window.map(x => expr(x))
        val stem = s"${base}(${args})"
        if w.isDefined then
          s"${stem} ${w.get}"
        else
          stem
      case w: WindowApply =>
        val base   = expr(w.base)
        val window = expr(w.window)
        Seq(s"${base}", window).mkString(" ")
      case f: FunctionArg =>
        // TODO handle arg name mapping
        if f.isDistinct then
          s"distinct ${expr(f.value)}"
        else
          expr(f.value)
      case w: Window =>
        val s = Seq.newBuilder[String]
        if w.partitionBy.nonEmpty then
          s += "partition by"
          s += w.partitionBy.map(x => expr(x)).mkString(", ")
        if w.orderBy.nonEmpty then
          s += "order by"
          s += w.orderBy.map(x => expr(x)).mkString(", ")
        w.frame
          .foreach { f =>
            s += s"${f.frameType.expr} between ${f.start.expr} and ${f.end.expr}"
          }
        s"over (${s.result().mkString(" ")})"
      case Eq(left, n: NullLiteral, _) =>
        s"${expr(left)} is null"
      case NotEq(left, n: NullLiteral, _) =>
        s"${expr(left)} is not null"
      case a: ArithmeticUnaryExpr =>
        a.sign match
          case Sign.NoSign =>
            expr(a.child)
          case Sign.Positive =>
            s"+${expr(a.child)}"
          case Sign.Negative =>
            s"-${expr(a.child)}"
      case b: BinaryExpression =>
        s"${expr(b.left)} ${b.operatorName} ${expr(b.right)}"
      case s: StringPart =>
        s.stringValue
      case l: Literal =>
        l.sqlExpr
      case bq: BackQuotedIdentifier =>
        // Need to use double quotes for back-quoted identifiers, which represents table or column names
        s"\"${bq.unquotedValue}\""
      case w: Wildcard =>
        w.strExpr
      case i: Identifier =>
        i.strExpr
      case s: SortItem =>
        s"${expr(s.sortKey)}${s.ordering.map(x => s" ${x.expr}").getOrElse("")}"
      case s: SingleColumn =>
        val left = expr(s.expr)
        if s.nameExpr.isEmpty then
          left
        else if left != s.nameExpr.toSQLAttributeName then
          s"${left} as ${s.nameExpr.toSQLAttributeName}"
        else
          left
      case a: Attribute =>
        a.fullName
      case t: TypedExpression =>
        expr(t.child)
      case p: ParenthesizedExpression =>
        s"(${expr(p.child)})"
      case i: InterpolatedString =>
        i.parts
          .map { e =>
            expr(e)
          }
          .mkString
      case s: SubQueryExpression =>
        val sql = query(s.query, Nil)(using sc.enterExpression)
        sql
      case i: IfExpr =>
        s"if(${expr(i.cond)}, ${expr(i.onTrue)}, ${expr(i.onFalse)})"
      case n: Not =>
        s"not ${expr(n.child)}"
      case l: ListExpr =>
        l.exprs.map(x => expr(x)).mkString(", ")
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        s"${expr(qual)}.${expr(name)}"
      case in: In =>
        val left  = expr(in.a)
        val right = in.list.map(x => expr(x)).mkString(", ")
        s"${left} in (${right})"
      case notIn: NotIn =>
        val left  = expr(notIn.a)
        val right = notIn.list.map(x => expr(x)).mkString(", ")
        s"${left} not in (${right})"
//      case n: NativeExpression =>
//        printExpression(ExpressionEvaluator.eval(n, ctx))
      case a: ArrayConstructor =>
        s"ARRAY[${a.values.map(x => expr(x)).mkString(", ")}]"
      case a: ArrayAccess =>
        s"${expr(a.arrayExpr)}[${expr(a.index)}]"
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
            },
          "end"
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
        bracket(cs(fields))
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

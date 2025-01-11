package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType.DuckDB
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.compiler.{Context, DBType, ModelSymbolInfo, SQLDialect}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.plan.JoinType.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

object SqlGenerator:
  sealed trait SqlContext:
    def nestingLevel: Int
    def nested: SqlContext          = Indented(nestingLevel + 1, parent = Some(this))
    def enterFrom: SqlContext       = InFromClause(nestingLevel + 1)
    def enterJoin: SqlContext       = InJoinClause(nestingLevel + 1)
    def enterExpression: SqlContext = InExpression(nestingLevel + 1)

    def isNested: Boolean             = nestingLevel > 0
    def needsToWrapWithParen: Boolean = nestingLevel == 0 || !withinExpression

    def withinJoin: Boolean =
      this match
        case InJoinClause(_) =>
          true
        case Indented(_, Some(parent)) =>
          parent.withinJoin
        case _ =>
          false

    def withinFrom: Boolean =
      this match
        case InFromClause(_) =>
          true
        case InJoinClause(_) =>
          true
        case _ =>
          false

    def withinExpression: Boolean =
      this match
        case InExpression(_) =>
          true
        case _ =>
          false

  end SqlContext

  case class Indented(nestingLevel: Int, parent: Option[SqlContext] = None) extends SqlContext
  case class InFromClause(nestingLevel: Int)                                extends SqlContext
  case class InJoinClause(nestingLevel: Int)                                extends SqlContext
  case class InExpression(nestingLevel: Int)                                extends SqlContext

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
  *     syntax context in SqlContext
  *   - As SQL requires to have a rigit structure of SELECT ... FROM .. WHERE .. GROUP BY ... HAVING
  *     .. ORDER BY ... LIMIT, some we push some plan nodes to remainingParents stack
  *   - When we find Selection node (Project, Agg, column-level operators), consolidate
  *     remainingParents and child filters to form SQLSelect node.
  *
  * @param dbType
  * @param ctx
  */
class SqlGenerator(dbType: DBType)(using ctx: Context = Context.NoContext) extends LogSupport:
  import SqlGenerator.*

  def print(l: LogicalPlan): String =

    def iter(plan: LogicalPlan): String =
      plan match
        case r: Relation =>
          printRelation(r, Nil)(using SqlGenerator.Indented(0))
        case p: PackageDef =>
          p.statements.map(stmt => iter(stmt)).mkString(";\n\n")
        case other =>
          warn(s"unsupported logical plan: ${other}")
          other.toString

    val sql = iter(l)
    trace(l.pp)
    debug(sql)
    sql

  private def selectAllWithIndent(tableExpr: String)(using sqlContext: SqlContext): String =
    if sqlContext.withinFrom then
      s"${tableExpr}"
    else if !sqlContext.needsToWrapWithParen then
      s"select * from ${tableExpr}"
    else
      indent(s"(select * from ${tableExpr})")

  private def selectWithIndentAndParenIfNecessary(
      body: String
  )(using sqlContext: SqlContext): String =
    if !sqlContext.needsToWrapWithParen then
      body
    else
      indent(s"(${body})")

  private def indent(s: String)(using sqlContext: SqlContext): String =
    if sqlContext.nestingLevel == 0 then
      s
    else
      s.split("\n").map(x => s"  ${x}").mkString("\n", "\n", "")

  private def hasSelection(r: Relation): Boolean =
    r match
      case s: Selection =>
        true
      case r: UnaryRelation =>
        hasSelection(r.child)
      case _ =>
        false

  private def addProjectionIfMissing(r: Relation): Relation =
    if !hasSelection(r) then
      Project(r, Seq(SingleColumn(EmptyName, Wildcard(NoSpan), NoSpan)), NoSpan)
    else
      r

  private def printSubQuery(r: Relation, remainingParents: List[Relation])(using
      sqlContext: SqlContext
  ): String =
    val p = addProjectionIfMissing(r)
    printRelation(p, remainingParents)

  /**
    * Print Relation nodes while tracking the parent nodes (e.g., filter, sort) for merging them
    * later into a single SELECT statement.
    * @param r
    * @param remainingParents
    *   unprocessed parent nodes
    * @param sqlContext
    * @return
    */
  def printRelation(r: Relation, remainingParents: List[Relation])(using
      sqlContext: SqlContext
  ): String =

    r match
      case q: Query =>
        printSubQuery(q.body, remainingParents)
      case g: GroupBy =>
        printGroupBy(g, remainingParents)
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = printPivotOnExpr(p)
        val aggItems = a.selectItems.map(x => printExpression(x)).mkString(", ")
        val pivotExpr =
          s"pivot ${printRelation(p.child, remainingParents)(using sqlContext.enterFrom)}\n  on ${onExpr}\n  using ${aggItems}"
        if p.groupingKeys.isEmpty then
          selectWithIndentAndParenIfNecessary(pivotExpr)
        else
          val groupByItems = p.groupingKeys.map(x => printExpression(x)).mkString(", ")
          selectWithIndentAndParenIfNecessary(s"${pivotExpr}\n  group by ${groupByItems}")
      case t: Transform =>
        val transformItems = t.transformItems.map(x => printExpression(x)).mkString(", ")
        selectWithIndentAndParenIfNecessary(
          s"""select ${transformItems}, * from ${printRelation(t.inputRelation, remainingParents)(
              using sqlContext.enterFrom
            )}"""
        )
      case s: Selection =>
        printAsSelect(s, remainingParents)
      case j: Join =>
        val asof =
          if j.asof then
            if dbType.supportAsOfJoin then
              "asof "
            else
              throw StatusCode.SYNTAX_ERROR.newException(s"AsOf join is not supported in ${dbType}")
          else
            ""

        val l = printRelation(j.left, Nil)(using sqlContext.enterJoin)
        val r = printRelation(j.right, Nil)(using sqlContext.enterJoin)
        val c =
          j.cond match
            case NoJoinCriteria =>
              ""
            case NaturalJoin(_) =>
              ""
            case u: JoinOnTheSameColumns =>
              s" using(${u.columns.map(_.fullName).mkString(", ")})"
            case JoinOn(expr, _) =>
              s" on ${printExpression(expr)}"
            case JoinOnEq(keys, _) =>
              s" on ${printExpression(Expression.concatWithEq(keys))}"
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

        if sqlContext.nestingLevel == 0 then
          // Need a top-level select statement for (left) join (right)
          indent(s"select * from ${joinSQL}")
        else
          joinSQL
      case s: SetOperation =>
        val rels = s.children.map(x => printRelation(x, Nil)(using sqlContext))
        val op   = s.toSQLOp
        val sql  = rels.mkString(s"\n${op}\n")
        selectWithIndentAndParenIfNecessary(sql)
      case p: Pivot => // pivot without explicit aggregations
        selectWithIndentAndParenIfNecessary(
          s"pivot ${printRelation(p.child, remainingParents)(using sqlContext.enterFrom)}\n  on ${printPivotOnExpr(p)}"
        )
      case d: Debug =>
        // Skip debug expression
        printRelation(d.inputRelation, remainingParents)
      case d: Dedup =>
        selectWithIndentAndParenIfNecessary(
          s"""select distinct * from ${printRelation(d.child, remainingParents)(using
              sqlContext.enterFrom
            )}"""
        )
      case s: Sample =>
        val child = printRelation(s.child, remainingParents)(using sqlContext.enterFrom)
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
                  s"select * from ${child} TABLESAMPLE ${s.method.toString.toLowerCase()}(${percentage})"
            case _ =>
              warn(s"Unsupported sampling method: ${s.method} for ${dbType}")
              child
        selectWithIndentAndParenIfNecessary(body)
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
          case v: Values if sqlContext.nestingLevel == 0 =>
            selectAllWithIndent(s"${printValues(v)} as ${tableAlias}")
          case v: Values if sqlContext.nestingLevel > 0 && !sqlContext.withinJoin =>
            selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")
          case v: Values if sqlContext.nestingLevel > 0 && sqlContext.withinJoin =>
            // For joins, expose table column aliases to the outer scope
            s"${selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")} as ${a.alias.fullName}"
          case _ =>
            indent(
              s"${printRelation(a.child, remainingParents)(using sqlContext.nested)} as ${tableAlias}"
            )
      case p: BracedRelation =>
        def inner = printRelation(p.child, remainingParents)(using sqlContext.nested)
        p.child match
          case v: Values =>
            // No need to wrap values query
            inner
          case AliasedRelation(v: Values, _, _, _) =>
            inner
          case _ =>
            selectWithIndentAndParenIfNecessary(inner)
      case t: TestRelation =>
        printRelation(t.inputRelation, remainingParents)(using sqlContext)
      case q: WithQuery =>
        val subQueries = q
          .withStatements
          .map { w =>
            val subQuery = printRelation(w.child, Nil)(using sqlContext)
            s"${printExpression(w.alias)} as (\n${subQuery}\n)"
          }
        val body     = printRelation(q.child, remainingParents)(using sqlContext)
        val withStmt = subQueries.mkString("with ", ", ", "")
        s"""${withStmt}
           |${body}
           |""".stripMargin
      case f: FilteringRelation =>
        // Sort, Offset, Limit, Filter, Distinct, etc.
        printRelation(f.child, f :: remainingParents)
      case t: TableInput =>
        printExpression(t.sqlExpr)
      case v: Values =>
        printValues(v)
      case s: SelectAsAlias =>
        printRelation(s.child, remainingParents)
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

        selectWithIndentAndParenIfNecessary(sql)
      case r: RelationInspector =>
        // Skip relation inspector
        // TODO Dump output to the file logger
        printRelation(r.child, remainingParents)
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
          cond += Eq(UnquotedIdentifier("table_catalog", NoSpan), StringLiteral(c, NoSpan), NoSpan)
        }
        schema.foreach { s =>
          cond += Eq(UnquotedIdentifier("table_schema", NoSpan), StringLiteral(s, NoSpan), NoSpan)
        }

        val conds = cond.result()
        val body =
          if conds.size == 0 then
            sql
          else
            s"${sql} where ${printExpression(Expression.concatWithAnd(conds))}"
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
          cond += Eq(UnquotedIdentifier("catalog_name", NoSpan), StringLiteral(c, NoSpan), NoSpan)
        }
        val conds = cond.result()
        val body =
          if conds.size == 0 then
            sql
          else
            s"${sql} where ${printExpression(Expression.concatWithAnd(conds))}"
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
            s"select * from (values ${indent(modelValues.mkString(", "))}) as __models(name, args, package_name)"
          )
      case other =>
        warn(s"unsupported relation type: ${other}")
        other.pp
    end match

  end printRelation

  private def printAsSelect(r: Relation, parents: List[Relation])(using
      sqlContext: SqlContext
  ): String =
    var remainingParents = parents

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

    val sqlSelect = toSQLSelect(
      SQLSelect(inputRelation, selectItems, Nil, Nil, parentFilters, orderBy, limit, r.span),
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
        sqlSelect.selectItems.map(x => printExpression(x)).mkString(", ")
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
      // Do not add from clause for empty imputs
      case t: TableInput =>
        s += s"from ${printExpression(t.sqlExpr)}"
      case _ =>
        // Start a new SELECT statement inside FROM
        s += s"from ${printRelation(sqlSelect.child, Nil)(using sqlContext.enterFrom)}"
    if sqlSelect.filters.nonEmpty then
      val filterExpr = Expression.concatWithAnd(sqlSelect.filters.map(x => x.filterExpr))
      s += s"where ${printExpression(filterExpr)}"
    if sqlSelect.groupingKeys.nonEmpty then
      s += s"group by ${sqlSelect.groupingKeys.map(x => printExpression(x)).mkString(", ")}"
    if sqlSelect.having.nonEmpty then
      s += s"having ${sqlSelect.having.map(x => printExpression(x.filterExpr)).mkString(", ")}"
    if sqlSelect.orderBy.nonEmpty then
      s += s"order by ${sqlSelect.orderBy.map(x => printExpression(x)).mkString(", ")}"
    if sqlSelect.limit.nonEmpty then
      s += s"limit ${printExpression(sqlSelect.limit.get)}"

    val body = selectWithIndentAndParenIfNecessary(s"${s.result().mkString("\n")}")
    if remainingParents.isEmpty then
      body
    else
      warn(
        s"Unprocessed parents:\n${remainingParents.mkString(" - ", "\n - ", "")} (${ctx.compilationUnit.sourceFile.fileName})"
      )
      body
  end printAsSelect

  private def toSQLSelect(agg: SQLSelect, r: Relation): SQLSelect =
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
  end toSQLSelect

  private def printGroupBy(g: GroupBy, parents: List[Relation])(using
      sqlContext: SqlContext
  ): String =
    // Translate GroupBy node without any projection (select) to Agg node
    val selectItems = List.newBuilder[Attribute]
    selectItems ++=
      g.groupingKeys
        .map { k =>
          val keyName = printExpression(k)
          SingleColumn(NameExpr.fromString(keyName), k.name, NoSpan)
        }
    selectItems ++=
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
                val exprStr = printExpression(expr)
                NameExpr.fromString(exprStr)
          SingleColumn(name, expr, NoSpan)
        }

    val agg = Agg(g, selectItems.result(), g.span)
    printRelation(agg, parents)
  end printGroupBy

  private def printValues(values: Values)(using sqlContext: SqlContext): String =
    val rows = values
      .rows
      .map { row =>
        row match
          case a: ArrayConstructor =>
            val elems = a.values.map(x => printExpression(x)).mkString(", ")
            s"(${elems})"
          case other =>
            printExpression(other)
      }
      .mkString(", ")
    s"(values ${rows})"

  private def printPivotOnExpr(p: Pivot)(using sqlContext: SqlContext): String = p
    .pivotKeys
    .map { k =>
      val values = k.values.map(v => printExpression(v)).mkString(", ")
      if values.isEmpty then
        s"${printExpression(k.name)}"
      else
        s"${printExpression(k.name)} in (${values})"
    }
    .mkString(", ")
  end printPivotOnExpr

  def printExpression(expression: Expression)(using sqlContext: SqlContext = Indented(0)): String =
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
        val sql = s"(${printSubQuery(s.query, Nil)(using sqlContext.enterExpression)})"
        sql
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
      case s: StructValue if dbType.supportRowExpr =>
        // For Trino
        val fields = s
          .fields
          .map { f =>
            printExpression(f.value)
          }
        val schema = s
          .fields
          .map { f =>
            val sqlType = DataType.toSQLType(f.value.dataType, dbType)
            s"${f.name} ${sqlType}"
          }
          .mkString(", ")
        s"cast(row(${fields.mkString(", ")}) as row(${schema}))"
      case s: StructValue =>
        val fields = s
          .fields
          .map { f =>
            s"${f.name}: ${printExpression(f.value)}"
          }
        s"{${fields.mkString(", ")}}"
      case m: MapValue =>
        dbType.mapConstructorSyntax match
          case SQLDialect.MapSyntax.KeyValue =>
            val entries = m
              .entries
              .map { e =>
                s"${printExpression(e.key)}: ${printExpression(e.value)}"
              }
            s"MAP{${entries.mkString(", ")}}"
          case SQLDialect.MapSyntax.ArrayPair =>
            val keys   = ArrayConstructor(m.entries.map(_.key), m.span)
            val values = ArrayConstructor(m.entries.map(_.value), m.span)
            s"MAP(${printExpression(keys)}, ${printExpression(values)})"
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

end SqlGenerator

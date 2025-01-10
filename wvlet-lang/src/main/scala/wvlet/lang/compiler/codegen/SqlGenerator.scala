package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType.DuckDB
import wvlet.lang.compiler.{Context, DBType, ModelSymbolInfo, SQLDialect}
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.ArrayConstructor
import wvlet.lang.model.plan.Relation
import wvlet.lang.model.plan.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.JoinType.{
  CrossJoin,
  FullOuterJoin,
  ImplicitJoin,
  InnerJoin,
  LeftOuterJoin,
  RightOuterJoin
}
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

object SqlGenerator:
  sealed trait SqlContext:
    def nestingLevel: Int
    def nested: SqlContext    = Indented(nestingLevel + 1, parent = Some(this))
    def enterFrom: SqlContext = InFromClause(nestingLevel + 1)
    def enterJoin: SqlContext = InJoinClause(nestingLevel + 1)

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

  case class Indented(nestingLevel: Int, parent: Option[SqlContext] = None) extends SqlContext
  case class InFromClause(nestingLevel: Int)                                extends SqlContext
  case class InJoinClause(nestingLevel: Int)                                extends SqlContext

  private val identifierPattern = "^[_a-zA-Z][_a-zA-Z0-9]*$".r
  private def doubleQuoteIfNecessary(s: String): String =
    if identifierPattern.matches(s) then
      s
    else
      s""""${s}""""

end SqlGenerator

class SqlGenerator(dbType: DBType)(using ctx: Context = Context.NoContext) extends LogSupport:
  import SqlGenerator.*

  def print(l: LogicalPlan): String =
    l match
      case r: Relation =>
        printRelation(r, Nil)(using SqlGenerator.Indented(0))
      case p: PackageDef =>
        p.statements.map(stmt => print(stmt)).mkString(";\n\n")
      case other =>
        warn(s"unsupported logical plan: ${other}")
        other.toString

  private def selectAllWithIndent(tableExpr: String)(using sqlContext: SqlContext): String =
    if sqlContext.withinFrom then
      s"${tableExpr}"
    else if sqlContext.nestingLevel == 0 then
      s"select * from ${tableExpr}"
    else
      indent(s"(select * from ${tableExpr})")

  private def selectWithIndentAndParenIfNecessary(
      body: String
  )(using sqlContext: SqlContext): String =
    if sqlContext.nestingLevel == 0 then
      body
    else
      indent(s"(${body})")

  private def indent(s: String)(using sqlContext: SqlContext): String =
    if sqlContext.nestingLevel == 0 then
      s
    else
      s.split("\n").map(x => s"  ${x}").mkString("\n", "\n", "")

  /**
    * Print Relation nodes while tracking the parent nodes (e.g., filter, sort) for merging them
    * later into a single SELECT statement.
    * @param r
    * @param parents
    *   unprocessed parent nodes
    * @param sqlContext
    * @return
    */
  def printRelation(r: Relation, parents: List[Relation])(using sqlContext: SqlContext): String =
    r match
      case q: Query =>
        // Query is just an wrapper of Relation, so no need to push it in the stack
        printRelation(q.body, parents)
      case a: GroupBy =>
        selectWithIndentAndParenIfNecessary(s"${printAggregate(a)}")
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = printPivotOnExpr(p)
        val aggItems = a.selectItems.map(x => printExpression(x)).mkString(", ")
        val pivotExpr =
          s"pivot ${printRelation(p.child, parents)(using sqlContext.enterFrom)}\n  on ${onExpr}\n  using ${aggItems}"
        if p.groupingKeys.isEmpty then
          selectWithIndentAndParenIfNecessary(pivotExpr)
        else
          val groupByItems = p.groupingKeys.map(x => printExpression(x)).mkString(", ")
          selectWithIndentAndParenIfNecessary(s"${pivotExpr}\n  group by ${groupByItems}")
      case t: Transform =>
        val transformItems = t.transformItems.map(x => printExpression(x)).mkString(", ")
        selectWithIndentAndParenIfNecessary(
          s"""select ${transformItems}, * from ${printRelation(t.inputRelation, parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case s: Selection =>
        printSelection(s, parents)
      case j: Join =>
        val asof =
          if j.asof then
            if dbType.supportAsOfJoin then
              "asof "
            else
              throw StatusCode.SYNTAX_ERROR.newException(s"AsOf join is not supported in ${dbType}")
          else
            ""

        val l = printRelation(j.left, parents)(using sqlContext.enterJoin)
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
          s"pivot ${printRelation(p.child, parents)(using sqlContext.enterFrom)}\n  on ${printPivotOnExpr(p)}"
        )
      case d: Debug =>
        // Skip debug expression
        printRelation(d.inputRelation, d :: parents)
      case d: Dedup =>
        selectWithIndentAndParenIfNecessary(
          s"""select distinct * from ${printRelation(d.child, d :: parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case s: Sample =>
        val child = printRelation(s.child, s :: parents)(using sqlContext.enterFrom)
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

        val sql =
          a.child match
            case t: TableScan =>
              s"${t.name.fullName} as ${tableAlias}"
            case t: TableRef =>
              s"${t.name.fullName} as ${tableAlias}"
            case t: TableFunctionCall =>
              s"${printRelation(t, t :: parents)(using sqlContext)} as ${tableAlias}"
            case v: Values if sqlContext.nestingLevel == 0 =>
              selectAllWithIndent(s"${printValues(v)} as ${tableAlias}")
            case v: Values if sqlContext.nestingLevel > 0 && !sqlContext.withinJoin =>
              selectWithIndentAndParenIfNecessary(
                s"select * from ${printValues(v)} as ${tableAlias}"
              )
            case v: Values if sqlContext.nestingLevel > 0 && sqlContext.withinJoin =>
              s"${selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")} as ${a.alias.fullName}"
            case _ =>
              indent(
                s"${printRelation(a.inputRelation, parents)(using sqlContext.nested)} as ${tableAlias}"
              )
        sql
      case p: BracedRelation =>
        def inner = printRelation(p.child, p :: parents)(using sqlContext.nested)
        p.child match
          case v: Values =>
            // No need to wrap values query
            inner
          case AliasedRelation(v: Values, _, _, _) =>
            inner
          case _ =>
            selectWithIndentAndParenIfNecessary(inner)
      case t: TestRelation =>
        printRelation(t.inputRelation, t :: parents)(using sqlContext)
      case q: WithQuery =>
        val subQueries = q
          .withStatements
          .map { w =>
            val subQuery = printRelation(w.child, Nil)(using sqlContext)
            s"${printExpression(w.alias)} as (\n${subQuery}\n)"
          }
        val body     = printRelation(q.child, parents)(using sqlContext)
        val withStmt = subQueries.mkString("with ", ", ", "")
        s"""${withStmt}
           |${body}
           |""".stripMargin
      case a: AddColumnsToRelation =>
        val newColumns = a
          .newColumns
          .map { c =>
            printExpression(c)
          }
        selectWithIndentAndParenIfNecessary(
          s"""select *, ${newColumns
              .mkString(", ")} from ${printRelation(a.inputRelation, a :: parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case d: ExcludeColumnsFromRelation =>
        selectWithIndentAndParenIfNecessary(
          s"""select ${d
              .relationType
              .fields
              .map(_.toSQLAttributeName)
              .mkString(", ")} from ${printRelation(d.inputRelation, d :: parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case r: RenameColumnsFromRelation =>
        val newColumns = r
          .child
          .relationType
          .fields
          .map { f =>
            r.columnMapping.get(f.name) match
              case Some(alias) =>
                s"${f.toSQLAttributeName} as ${alias.toSQLAttributeName}"
              case None =>
                s"${f.toSQLAttributeName}"
          }
        selectWithIndentAndParenIfNecessary(
          s"""select ${newColumns
              .mkString(", ")} from ${printRelation(r.inputRelation, r :: parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case s: ShiftColumns =>
        selectWithIndentAndParenIfNecessary(
          s"""select ${s
              .relationType
              .fields
              .map(_.toSQLAttributeName)
              .mkString(", ")} from ${printRelation(s.inputRelation, s :: parents)(using
              sqlContext.enterFrom
            )}"""
        )
      case f: FilteringRelation =>
        // Sort, Offset, Limit, Filter, Distinct, etc.
        printRelation(f.child, f :: parents)
      case t: TableRef =>
        selectAllWithIndent(s"${t.name.fullName}")
      case t: TableFunctionCall =>
        val args = t.args.map(x => printExpression(x)).mkString(", ")
        selectAllWithIndent(s"${t.name.strExpr}(${args})")
      case r: RawSQL =>
        selectWithIndentAndParenIfNecessary(printExpression(r.sql))
      case t: TableScan =>
        selectAllWithIndent(s"${t.name.fullName}")
      case j: JSONFileScan =>
        selectAllWithIndent(s"'${j.path}'")
      case t: ParquetFileScan =>
        selectAllWithIndent(s"'${t.path}'")
      case v: Values =>
        printValues(v)
      case s: SelectAsAlias =>
        printRelation(s.child, s :: parents)
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
        printRelation(r.child, r :: parents)
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

  private def printSelection(l: Selection, parents: List[Relation])(using
      sqlContext: SqlContext
  ): String =
    // pull-up filter nodes to build where clause
    // pull-up an Aggregate node to build group by clause
    val agg = toSQLSelect(SQLSelect(l.child, l.selectItems.toList, Nil, Nil, Nil, l.span), l.child)
    val hasDistinct =
      l match
        case d: Distinct =>
          true
        case _ =>
          false

    val selectItems = agg.selectItems.map(x => printExpression(x)).mkString(", ")
    val s           = Seq.newBuilder[String]
    s +=
      s"select ${
          if hasDistinct then
            "distinct "
          else
            ""
        }${selectItems}"
    agg.child match
      case e: EmptyRelation =>
      // Do not add from clause for empty imputs
      case _ =>
        // Start a new SELECT statement inside FROM
        s += s"from ${printRelation(agg.child, Nil)(using sqlContext.enterFrom)}"
    if agg.filters.nonEmpty then
      val filterExpr = Expression.concatWithAnd(agg.filters.map(x => x.filterExpr))
      s += s"where ${printExpression(filterExpr)}"
    if agg.groupingKeys.nonEmpty then
      s += s"group by ${agg.groupingKeys.map(x => printExpression(x)).mkString(", ")}"
    if agg.having.nonEmpty then
      s += s"having ${agg.having.map(x => printExpression(x.filterExpr)).mkString(", ")}"

    def processSortAndLimit(lst: List[Relation]): List[Relation] =
      lst match
        case (st: Sort) :: (l: Limit) :: tail =>
          s += s"order by ${st.orderBy.map(e => printExpression(e)).mkString(", ")}"
          s += s"limit ${printExpression(l.limit)}"
          tail
        case (st: Sort) :: tail =>
          s += s"order by ${st.orderBy.map(e => printExpression(e)).mkString(", ")}"
          tail
        case (l: Limit) :: tail =>
          s += s"limit ${printExpression(l.limit)}"
          tail
        case other =>
          other

    val remainingParents = processSortAndLimit(parents)
    val body             = selectWithIndentAndParenIfNecessary(s"${s.result().mkString("\n")}")

    if remainingParents.isEmpty then
      body
    else
      warn(s"Unprocessed parents: ${remainingParents}")
      body

  end printSelection

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
            agg.copy(child = other, filters = filters)
      case p: Project =>
        agg
      case a: Agg =>
        agg
      case p: Pivot =>
        agg
      case a: GroupBy =>
        val (filters, lastNode) = collectFilter(a.child)
        agg.copy(child = a.child, groupingKeys = a.groupingKeys, filters = filters)
      case _ =>
        agg
  end toSQLSelect

  private def printAggregate(a: GroupBy)(using sqlContext: SqlContext): String =
    // Aggregation without any projection (select)
    val agg = toSQLSelect(SQLSelect(a.child, Nil, a.groupingKeys, Nil, Nil, a.span), a.child)
    val s   = Seq.newBuilder[String]
    val selectItems = Seq.newBuilder[String]
    selectItems ++=
      agg
        .groupingKeys
        .map { x =>
          val key = printExpression(x)
          s"""${key} as "${key}""""
        }
    selectItems ++=
      a.inputRelationType
        .fields
        .map { f =>
          // TODO: This should generate a nested relation, but use arbitrary(expr) for efficiency
          val expr = s"arbitrary(${f.toSQLAttributeName})"
          dbType match
            case DBType.DuckDB =>
              // DuckDB generates human-friendly column name
              expr
            case _ =>
              s"""${expr} as "${expr}""""
        }
    s += s"select ${selectItems.result().mkString(", ")}"
    s += s"from ${printRelation(agg.child, Nil)(using sqlContext.enterFrom)}"
    if agg.filters.nonEmpty then
      val cond = printExpression(Expression.concatWithAnd(agg.filters.map(_.filterExpr)))
      s += s"where ${cond}"
    s += s"group by ${agg.groupingKeys.map(x => printExpression(x)).mkString(", ")}"
    s.result().mkString("\n")
  end printAggregate

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

  def printExpression(expression: Expression)(using sqlContext: SqlContext): String =
    expression match
      case g: UnresolvedGroupingKey =>
        printExpression(g.child)
      case f: FunctionApply =>
        val base = printExpression(f.base)
        val args = f.args.map(x => printExpression(x)).mkString(", ")
        val w    = f.window.map(x => printExpression(x))
        val stem = s"${base}(${args})"
        if w.isDefined then
          s"${stem} ${w}"
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
        s"(${printRelation(s.query, Nil)(using sqlContext.nested)})"
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
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end SqlGenerator

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler.codegen

import wvlet.lang.BuildInfo
import wvlet.lang.compiler.{
  BoundedSymbolInfo,
  CompilationUnit,
  Context,
  DBType,
  ModelSymbolInfo,
  Name,
  Phase,
  Symbol,
  TermName
}
import wvlet.lang.compiler.DBType.{DuckDB, Trino}
import wvlet.lang.compiler.analyzer.TypeResolver
import wvlet.lang.compiler.transform.{ExpressionEvaluator, PreprocessLocalExpr}
import wvlet.lang.ext.NativeFunction
import wvlet.lang.api.{NodeLocation, StatusCode}
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.JoinType.*
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport

import java.io.File
import scala.collection.immutable.ListMap

case class GeneratedSQL(sql: String, plan: Relation)

object GenSQL extends Phase("generate-sql"):

  sealed trait SQLGenContext:
    def nestingLevel: Int
    def nested: SQLGenContext    = Indented(nestingLevel + 1)
    def enterFrom: SQLGenContext = InFromClause(nestingLevel + 1)

    def withinFrom: Boolean =
      this match
        case InFromClause(_) =>
          true
        case _ =>
          false

  case class Indented(nestingLevel: Int)     extends SQLGenContext
  case class InFromClause(nestingLevel: Int) extends SQLGenContext

  private def doubleQuoteIfNecessary(s: String): String =
    if s.matches("^[_a-zA-Z][_a-zA-Z0-9]*$") then
      s
    else
      s""""${s}""""

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Generate SQL from the resolved plan
    // generateSQL(unit.resolvedPlan)
    // Attach the generated SQL to the CompilationUnit
    unit

  def generateSQL(unit: CompilationUnit, ctx: Context): String =
    val statements = List.newBuilder[String]

    def loop(p: ExecutionPlan): Unit =
      p match
        case ExecuteTasks(tasks) =>
          tasks.foreach(loop)
        case ExecuteQuery(plan) =>
          plan match
            case r: Relation =>
              val gen = GenSQL.generateSQL(r, ctx)
              statements += gen.sql
            case other =>
              warn(s"Unsupported query type: ${other.pp}")
        case ExecuteSave(save, queryPlan) =>
          statements ++= generateSaveSQL(save, ctx)
        case other =>
          warn(s"Unsupported execution plan: ${other.pp}")
    end loop

    val executionPlan = ExecutionPlanner.plan(unit, ctx)
    loop(executionPlan)
    val sql = statements.result().mkString(";\n")
    sql

  private def withHeader(sql: String, ctx: Context): String =
    def headerComment(ctx: Context): String =
      val header = Seq.newBuilder[String]
      header += s"version:${BuildInfo.version}"
      val src =
        if !ctx.compilationUnit.sourceFile.isEmpty then
          header += s"src:${ctx.compilationUnit.sourceFile.fileName}"
      s"""--- wvlet ${header.result().mkString(", ")}"""

    s"${headerComment(ctx)}\n${sql}"

  def generateSQL(q: Relation, ctx: Context, addHeader: Boolean = true): GeneratedSQL =
    val expanded = expand(q, ctx)
    // val sql      = SQLGenerator.toSQL(expanded)
    val gen = GenSQL(ctx)
    val sql = gen.printRelation(expanded)(using Indented(0))

    val query: String =
      if addHeader then
        withHeader(sql, ctx)
      else
        sql
    trace(s"[plan]\n${expanded.pp}\n[SQL]\n${query}")
    GeneratedSQL(query, expanded)

  def generateDeleteSQL(ops: DeleteOps, context: Context): List[String] =
    given Context = context
    val gen       = GenSQL(context)
    ops match
      case d: Delete =>
        def filterExpr(x: Relation): Option[String] =
          x match
            case q: Query =>
              filterExpr(q.child)
            case f: Filter =>
              Some(gen.printExpression(f.filterExpr)(using Indented(0)))
            case l: LeafPlan =>
              None
            case other =>
              throw StatusCode
                .SYNTAX_ERROR
                .newException(s"Unsupported delete input: ${other.modelName}", other.sourceLocation)

        val filterSQL = filterExpr(d.inputRelation)
        var sql       = withHeader(s"delete from ${d.targetTable.fullName}", context)
        filterSQL.foreach { expr =>
          sql += s"\nwhere ${expr}"
        }
        List(sql)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"${other.modelName} is not implemented yet", other.sourceLocation)

  def generateSaveSQL(save: Save, context: Context): List[String] =
    val statements = List.newBuilder[String]
    save match
      case s: SaveAs =>
        val baseSQL           = generateSQL(save.inputRelation, context, addHeader = false)
        var needsTableCleanup = false
        val ctasCmd =
          if context.dbType.supportCreateOrReplace then
            s"create or replace table"
          else
            needsTableCleanup = true
            "create table"
        val ctasSQL = withHeader(s"${ctasCmd} ${s.target.fullName} as\n${baseSQL.sql}", context)

        if needsTableCleanup then
          val dropSQL = s"drop table if exists ${s.target.fullName}"
          // TODO: May need to wrap drop-ctas in a transaction
          statements += withHeader(dropSQL, context)

        statements += ctasSQL
      case s: SaveAsFile if context.dbType == DBType.DuckDB =>
        val baseSQL    = GenSQL.generateSQL(save.inputRelation, context, addHeader = false)
        val targetPath = context.dataFilePath(s.path)
        val sql        = s"copy (${baseSQL.sql}) to '${targetPath}'"
        statements += withHeader(sql, context)
      case a: AppendTo =>
        val baseSQL       = GenSQL.generateSQL(save.inputRelation, context, addHeader = false)
        val tbl           = TableName.parse(a.targetName)
        val schema        = tbl.schema.getOrElse(context.defaultSchema)
        val fullTableName = s"${schema}.${tbl.name}"
        val insertSQL =
          context.catalog.getTable(TableName.parse(fullTableName)) match
            case Some(t) =>
              s"insert into ${fullTableName}\n${baseSQL.sql}"
            case None =>
              s"create table ${fullTableName} as\n${baseSQL.sql}"
        statements += withHeader(insertSQL, context)
      case a: AppendToFile if context.dbType == DBType.DuckDB =>
        val baseSQL    = GenSQL.generateSQL(save.inputRelation, context, addHeader = false)
        val targetPath = context.dataFilePath(a.path)
        if new File(targetPath).exists then
          val sql =
            s"""copy (
               |  (select * from '${targetPath}')
               |  union all
               |  ${baseSQL.sql}
               |)
               |to '${targetPath}' (USE_TMP_FILE true)""".stripMargin
          statements += withHeader(sql, context)
        else
          val sql = s"create (${baseSQL.sql}) to '${targetPath}'"
          statements += withHeader(sql, context)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"${other.modelName} is not implemented yet for ${context.dbType}",
            other.sourceLocation(using context)
          )
    end match
    statements.result()

  end generateSaveSQL

  /**
    * Expand referenced model queries by populating model arguments
    * @param relation
    * @param ctx
    * @return
    */
  def expand(relation: Relation, ctx: Context): Relation =
    // expand referenced models

    def transformExpr(r: Relation, ctx: Context): Relation = r
      .transformUpExpressions {
        case b: BackquoteInterpolatedString =>
          PreprocessLocalExpr.EvalBackquoteInterpolation.transformExpression(b, ctx)
        case i: Identifier =>
          val nme = Name.termName(i.leafName)
          ctx.scope.lookupSymbol(nme) match
            case Some(sym) =>
              sym.symbolInfo match
                case b: BoundedSymbolInfo =>
                  // Replace to the bounded expression
                  b.expr
                case _ =>
                  i
            case None =>
              i
      }
      .asInstanceOf[Relation]

    def transformModelScan(m: ModelScan, sym: Symbol): Relation =
      sym.tree match
        case md: ModelDef =>
          val newCtx = ctx.newContext(sym)
          // TODO add model args to the context sco
          m.modelArgs
            .zipWithIndex
            .foreach { (arg, index) =>
              val argName: TermName = arg.name.getOrElse(md.params(index).name)
              val argValue          = arg.value

              // Register function arguments to the current scope
              val argSym = Symbol(ctx.global.newSymbolId)

              given Context = ctx

              argSym.symbolInfo = BoundedSymbolInfo(
                symbol = argSym,
                name = argName,
                tpe = argValue.dataType,
                // TODO: This expr can be outdated after tree rewrite.
                expr = argValue
              )
              newCtx.scope.add(argName, argSym)
              argSym
            }

          // Replace function argument references in the model body with the actual expressions
          val modelBody = transformExpr(md.child, newCtx)
          expand(modelBody, newCtx)
        case other =>
          warn(s"Unknown model tree for ${m.name}: ${other}")
          m

    // TODO expand expressions and inline macros as well
    relation
      .transformUp {
        case m: ModelScan =>
          lookupType(Name.termName(m.name.name), ctx) match
            case Some(sym) =>
              val rel = transformModelScan(m, sym)
              // Finally resolve types again
              TypeResolver.resolve(rel, ctx)
            case None =>
              warn(s"unknown model: ${m.name}")
              m
        case q: Query =>
          // unwrap
          q.child
      }
      .transformOnce { case r: Relation =>
        // Evaluate identifiers
        transformExpr(r, ctx)
      }
      .asInstanceOf[Relation]

  end expand

  private def lookupType(name: Name, ctx: Context): Option[Symbol] = ctx
    .scope
    .lookupSymbol(name)
    .orElse {
      var result: Option[Symbol] = None
      for
        c <- ctx.global.getAllContexts
        if result.isEmpty
      do
        result = c.compilationUnit.knownSymbols.find(_.name == name)
      result
    }

end GenSQL

class GenSQL(ctx: Context) extends LogSupport:
  import GenSQL.*

  def printRelation(r: Relation)(using sqlContext: SQLGenContext): String =
    def indent(s: String): String =
      if sqlContext.nestingLevel == 0 then
        s
      else
        val str = s.split("\n").map(x => s"  ${x}").mkString("\n", "\n", "")
        str

    def selectWithIndentAndParenIfNecessary(body: String): String =
      if sqlContext.nestingLevel == 0 then
        body
      else
        indent(s"(${body})")

    def selectAllWithIndent(tableExpr: String): String =
      if sqlContext.withinFrom then
        s"${tableExpr}"
      else if sqlContext.nestingLevel == 0 then
        s"select * from ${tableExpr}"
      else
        indent(s"(select * from ${tableExpr})")

    def toSQLSelect(agg: SQLSelect, r: Relation): SQLSelect =
      def collectFilter(plan: Relation): (List[Filter], Relation) =
        plan match
          case f: Filter =>
            val (filters, child) = collectFilter(f.inputRelation)
            (f :: filters, child)
          case other =>
            (Nil, other)
      end collectFilter

      // pull-up aggregation node
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
          agg.copy(child = a.child, groupingKeys = a.groupingKeys)
        case _ =>
          agg

    end toSQLSelect

    def printAggregate(a: GroupBy): String =
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
            ctx.dbType match
              case DBType.DuckDB =>
                // DuckDB generates human-friendly column name
                expr
              case _ =>
                s"""${expr} as "${expr}""""
          }
      s += s"select ${selectItems.result().mkString(", ")}"
      s += s"from ${printRelation(agg.child)(using sqlContext.enterFrom)}"
      if agg.filters.nonEmpty then
        val cond = printExpression(Expression.concatWithAnd(agg.filters.map(_.filterExpr)))
        s += s"where ${cond}"
      s += s"group by ${agg.groupingKeys.map(x => printExpression(x)).mkString(", ")}"
      s.result().mkString("\n")
    end printAggregate

    def pivotOnExpr(p: Pivot): String = p
      .pivotKeys
      .map { k =>
        val values = k.values.map(v => printExpression(v)).mkString(", ")
        if values.isEmpty then
          s"${printExpression(k.name)}"
        else
          s"${printExpression(k.name)} in (${values})"
      }
      .mkString(", ")
    end pivotOnExpr

    def printValues(values: Values): String =
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

    r match
      case a: Agg if a.child.isPivot =>
        // pivot + agg combination
        val p: Pivot = a.child.asInstanceOf[Pivot]
        val onExpr   = pivotOnExpr(p)
        val aggItems = a.selectItems.map(x => printExpression(x)).mkString(", ")
        val pivotExpr =
          s"pivot ${printRelation(p.child)(using sqlContext.enterFrom)}\n  on ${onExpr}\n  using ${aggItems}"
        if p.groupingKeys.isEmpty then
          selectWithIndentAndParenIfNecessary(pivotExpr)
        else
          val groupByItems = p.groupingKeys.map(x => printExpression(x)).mkString(", ")
          selectWithIndentAndParenIfNecessary(s"${pivotExpr}\n  group by ${groupByItems}")
      case p: Pivot => // pivot without explicit aggregations
        selectWithIndentAndParenIfNecessary(
          s"pivot ${printRelation(p.child)(using sqlContext.enterFrom)}\n  on ${pivotOnExpr(p)}"
        )
      case p: AggSelect =>
        // pull-up filter nodes to build where clause
        // pull-up an Aggregate node to build group by clause
        val agg = toSQLSelect(
          SQLSelect(p.child, p.selectItems.toList, Nil, Nil, Nil, p.span),
          p.child
        )
        val hasDistinct =
          p match
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
            s += s"from ${printRelation(agg.child)(using sqlContext.enterFrom)}"
        if agg.filters.nonEmpty then
          val filterExpr = Expression.concatWithAnd(agg.filters.map(x => x.filterExpr))
          s += s"where ${printExpression(filterExpr)}"
        if agg.groupingKeys.nonEmpty then
          s += s"group by ${agg.groupingKeys.map(x => printExpression(x)).mkString(", ")}"
        if agg.having.nonEmpty then
          s += s"having ${agg.having.map(x => printExpression(x.filterExpr)).mkString(", ")}"

        selectWithIndentAndParenIfNecessary(s"${s.result().mkString("\n")}")
      case a: GroupBy =>
        selectWithIndentAndParenIfNecessary(s"${printAggregate(a)}")
      case j: Join =>
        val l = printRelation(j.left)(using sqlContext.nested)
        // join (right) is similar to enter from ...
        val r = printRelation(j.right)(using sqlContext.enterFrom)
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
              s"${l} join ${r}${c}"
            case LeftOuterJoin =>
              s"${l} left join ${r}${c}"
            case RightOuterJoin =>
              s"${l} right join ${r}${c}"
            case FullOuterJoin =>
              s"${l} full outer join ${r}${c}"
            case CrossJoin =>
              s"${l} cross join p${r}${c}"
            case ImplicitJoin =>
              s"${l}, ${r}${c}"

        if sqlContext.nestingLevel == 0 then
          // Need a top-level select statement for (left) join (right)
          indent(s"select * from ${joinSQL}")
        else
          joinSQL
      case f: Filter =>
        f.child match
          case a: GroupBy =>
            val body = List(s"${printAggregate(a)}", s"having ${printExpression(f.filterExpr)}")
              .mkString("\n")
            selectWithIndentAndParenIfNecessary(s"${body}")
          case _ =>
            selectWithIndentAndParenIfNecessary(
              s"""select * from ${printRelation(f.child)(using
                  sqlContext.enterFrom
                )}\nwhere ${printExpression(f.filterExpr)}"""
            )
      case d: Dedup =>
        selectWithIndentAndParenIfNecessary(
          s"""select distinct * from ${printRelation(d.child)(using sqlContext.enterFrom)}"""
        )
      case s: SetOperation =>
        val rels = s.children.map(x => printRelation(x)(using sqlContext.nested))
        val op   = s.toSQLOp
        val sql  = rels.mkString(s"\n${op}\n")
        selectWithIndentAndParenIfNecessary(sql)
      case a: AliasedRelation =>
        val tableAlias: String =
          val name = printExpression(a.alias)
          a.columnNames match
            case Some(columns) =>
              s"${name}(${columns.map(x => s"${x.toSQLAttributeName}").mkString(", ")})"
            case None =>
              name

        a.child match
          case t: TableScan =>
            s"${t.name.fullName} as ${tableAlias}"
          case v: Values if sqlContext.nestingLevel == 0 =>
            selectAllWithIndent(s"${printValues(v)} as ${tableAlias}")
          case v: Values if sqlContext.nestingLevel > 0 =>
            s"${selectWithIndentAndParenIfNecessary(s"select * from ${printValues(v)} as ${tableAlias}")} as ${a.alias.fullName}"
          case _ =>
            indent(s"${printRelation(a.inputRelation)(using sqlContext.nested)} as ${tableAlias}")
      case p: ParenthesizedRelation =>
        val inner = printRelation(p.child)(using sqlContext.nested)
        p.child match
          case v: Values =>
            // No need to wrap values query
            inner
          case AliasedRelation(v: Values, _, _, _) =>
            inner
          case _ =>
            selectWithIndentAndParenIfNecessary(inner)
      case t: TestRelation =>
        printRelation(t.inputRelation)(using sqlContext)
      case q: Query =>
        printRelation(q.body)(using sqlContext)
      case l: Limit =>
        l.inputRelation match
          case s: Sort =>
            // order by needs to be compresed with limit as subexpression query ordering will be ignored in Trino
            val input = printRelation(s.inputRelation)(using sqlContext.enterFrom)
            val body =
              s"""select * from ${input}
                 |order by ${s.orderBy.map(e => printExpression(e)).mkString(", ")}
                 |limit ${l.limit.stringValue}""".stripMargin
            selectWithIndentAndParenIfNecessary(body)
          case _ =>
            val input = printRelation(l.inputRelation)(using sqlContext.enterFrom)
            selectWithIndentAndParenIfNecessary(
              s"""select * from ${input}\nlimit ${l.limit.stringValue}"""
            )
      case s: Sort =>
        val input = printRelation(s.inputRelation)(using sqlContext.enterFrom)
        val body =
          s"""select * from ${input}\norder by ${s
              .orderBy
              .map(e => printExpression(e))
              .mkString(", ")}"""
        selectWithIndentAndParenIfNecessary(body)
      case t: Transform =>
        val transformItems = t.transformItems.map(x => printExpression(x)).mkString(", ")
        selectWithIndentAndParenIfNecessary(
          s"""select ${transformItems}, * from ${printRelation(t.inputRelation)(using
              sqlContext.enterFrom
            )}"""
        )
      case a: AddColumnsToRelation =>
        val newColumns = a
          .newColumns
          .map { c =>
            printExpression(c)
          }
        selectWithIndentAndParenIfNecessary(
          s"""select *, ${newColumns
              .mkString(", ")} from ${printRelation(a.inputRelation)(using sqlContext.enterFrom)}"""
        )
      case d: ExcludeColumnsFromRelation =>
        selectWithIndentAndParenIfNecessary(
          s"""select ${d
              .relationType
              .fields
              .map(_.toSQLAttributeName)
              .mkString(", ")} from ${printRelation(d.inputRelation)(using sqlContext.enterFrom)}"""
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
              .mkString(", ")} from ${printRelation(r.inputRelation)(using sqlContext.enterFrom)}"""
        )
      case s: ShiftColumns =>
        selectWithIndentAndParenIfNecessary(
          s"""select ${s
              .relationType
              .fields
              .map(_.toSQLAttributeName)
              .mkString(", ")} from ${printRelation(s.inputRelation)(using sqlContext.enterFrom)}"""
        )
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
        printRelation(s.child)
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
        printRelation(r.child)
      case s: Show if s.showType == ShowType.tables =>
        val sql  = s"select table_name from information_schema.tables"
        val cond = List.newBuilder[Expression]
        val opts = ctx.global.compilerOptions
        opts
          .catalog
          .map { catalog =>
            cond +=
              Eq(
                UnquotedIdentifier("table_catalog", NoSpan),
                StringLiteral(catalog, NoSpan),
                NoSpan
              )
          }
        opts
          .schema
          .map { schema =>
            cond +=
              Eq(UnquotedIdentifier("table_schema", NoSpan), StringLiteral(schema, NoSpan), NoSpan)
          }

        val conds = cond.result()
        val body =
          if conds.size == 0 then
            sql
          else
            s"${sql} where ${printExpression(Expression.concatWithAnd(conds))}"
        selectWithIndentAndParenIfNecessary(s"${body} order by table_name")
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
            s"select * from values ${indent(modelValues.mkString(", "))} as __models(name, args, package_name)"
          )
      case s: Sample =>
        val child = printRelation(s.child)(using sqlContext.enterFrom)
        val body: String =
          ctx.dbType match
            case DuckDB =>
              val size =
                s.size match
                  case Rows(n) =>
                    s"${n} rows"
                  case Percentage(percentage) =>
                    s"${percentage}%"
              s"select * from ${child} using sample ${s.method.toString.toLowerCase}(${size})"
            case Trino =>
              s.size match
                case Rows(n) =>
                  // Supported only in td-trino
                  s"select *, reservoir_sample(${n}) over() from ${child}"
                case Percentage(percentage) =>
                  s"select * from ${child} TABLESAMPLE ${s.method.toString.toLowerCase()}(${percentage})"
            case _ =>
              warn(s"Unsupported sampling method: ${s.method} for ${ctx.dbType}")
              child
        selectWithIndentAndParenIfNecessary(body)
      case d: Debug =>
        // Skip debug expression
        printRelation(d.inputRelation)
      case other =>
        warn(s"unknown relation type: ${other}")
        other.toString

    end match

  end printRelation

  def printExpression(expression: Expression)(using sqlContext: SQLGenContext): String =
    expression match
      case g: UnresolvedGroupingKey =>
        printExpression(g.child)
      case f: FunctionApply =>
        val base = printExpression(f.base)
        val args = f.args.map(x => printExpression(x)).mkString(", ")
        val w    = f.window.map(x => printExpression(x)).getOrElse("")
        Seq(s"${base}(${args})", w).mkString(" ")
      case f: FunctionArg =>
        // TODO handle arg name mapping
        printExpression(f.value)
      case w: Window =>
        val s = Seq.newBuilder[String]
        if w.partitionBy.nonEmpty then
          s += "partition by"
          s += w.partitionBy.map(x => printExpression(x)).mkString(", ")
        if w.orderBy.nonEmpty then
          s += "order by"
          s += w.orderBy.map(x => printExpression(x)).mkString(", ")
        s"over (${s.result().mkString(" ")})"
      case Eq(left, n: NullLiteral, _) =>
        s"${printExpression(left)} is null"
      case NotEq(left, n: NullLiteral, _) =>
        s"${printExpression(left)} is not null"
      case b: BinaryExpression =>
        s"${printExpression(b.left)} ${b.operatorName} ${printExpression(b.right)}"
      case s: StringPart =>
        s.stringValue
      case s: StringLiteral =>
        // Escape single quotes
        val v = s.stringValue.replaceAll("'", "''")
        s"'${v}'"
      case l: Literal =>
        l.stringValue
      case bq: BackQuotedIdentifier =>
        // Need to use double quotes for back-quoted identifiers, which represents table or column names
        s"\"${bq.unquotedValue}\""
      case i: Identifier =>
        i.strExpr
      case s: SortItem =>
        s"${printExpression(s.sortKey)}${s.ordering.map(x => s" ${x.expr}").getOrElse("")}"
      case s: SingleColumn =>
        if s.nameExpr.isEmpty then
          printExpression(s.expr)
        else
          s"${printExpression(s.expr)} as ${s.nameExpr.toSQLAttributeName}"
      case a: Attribute =>
        a.fullName
      case p: ParenthesizedExpression =>
        s"(${printExpression(p.child)})"
      case i: InterpolatedString =>
        i.parts
          .map { e =>
            printExpression(e)
          }
          .mkString
      case s: SubQueryExpression =>
        s"(${printRelation(s.query)(using sqlContext.nested)})"
      case i: IfExpr =>
        s"if(${printExpression(i.cond)}, ${printExpression(i.onTrue)}, ${printExpression(i.onFalse)})"
      case i: Wildcard =>
        i.strExpr
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
      case n: NativeExpression =>
        printExpression(ExpressionEvaluator.eval(n, ctx))
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end GenSQL

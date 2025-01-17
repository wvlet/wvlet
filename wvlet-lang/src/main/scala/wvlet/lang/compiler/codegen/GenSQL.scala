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
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.{SourceLocation, StatusCode}
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.DBType.{DuckDB, Trino}
import wvlet.lang.compiler.analyzer.TypeResolver
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.compiler.transform.{ExpressionEvaluator, PreprocessLocalExpr}
import wvlet.lang.compiler.{
  ValSymbolInfo,
  CompilationUnit,
  Context,
  DBType,
  ModelSymbolInfo,
  Name,
  Phase,
  SQLDialect,
  SourceIO,
  Symbol,
  TermName
}
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.plan.JoinType.*
import wvlet.lang.model.plan.SamplingSize.{Percentage, Rows}
import wvlet.log.LogSupport

import java.io.File
import scala.collection.immutable.ListMap

case class GeneratedSQL(sql: String, plan: Relation)

object GenSQL extends Phase("generate-sql"):

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

  def generateSQL(
      unit: CompilationUnit,
      ctx: Context,
      targetPlan: Option[LogicalPlan] = None
  ): String =
    val statements = List.newBuilder[String]

    def loop(p: ExecutionPlan): Unit =
      p match
        case ExecuteTasks(tasks) =>
          tasks.foreach(loop)
        case ExecuteQuery(plan) =>
          plan match
            case r: Relation =>
              val gen = GenSQL.generateSQLFromRelation(r, ctx)
              statements += gen.sql
            case other =>
              warn(s"Unsupported query type: ${other.pp}")
        case ExecuteSave(save, queryPlan) =>
          statements ++= generateSaveSQL(save, ctx)
        case ExecuteValDef(v) =>
          // TODO Refactor this with QueryExecutor
          val expr = ExpressionEvaluator.eval(v.expr)(using ctx)
          v.symbol.symbolInfo = ValSymbolInfo(ctx.owner, v.symbol, v.name, expr.dataType, expr)
          ctx.enter(v.symbol)
        case cmd: ExecuteCommand =>
          cmd.execute match
            case ExecuteExpr(e, _) =>
              val sql = generateExecute(e, ctx)
              statements += sql
            case _ =>
              warn(s"Unsupported command: ${cmd}")
        case ExecuteNothing =>
        // ok
        case other =>
          warn(s"Unsupported execution plan: ${other.pp}")
    end loop

    val executionPlan =
      if targetPlan.isEmpty then
        if unit.executionPlan.isEmpty then
          ExecutionPlanner.plan(unit, ctx)
        else
          unit.executionPlan
      else
        // Create an execution plan for sub queries
        ExecutionPlanner.plan(unit, targetPlan.get, ctx)
    loop(executionPlan)
    val queries = statements.result()
    val sql     = queries.mkString("\n;\n")
    if queries.size > 1 then
      // Add a last semicolon for multiple statements
      s"${sql}\n;"
    else
      sql

  end generateSQL

  private def withHeader(sql: String, sourceLocation: SourceLocation)(using ctx: Context): String =
    def headerComment: String =
      val header = Seq.newBuilder[String]
      header += s"version=${BuildInfo.version}"
      val src =
        if !ctx.compilationUnit.sourceFile.isEmpty then
          header += s"src=${sourceLocation.lineLocationString}"
      s"""-- wvlet ${header.result().mkString(", ")}"""

    s"${headerComment}\n${sql}"

  def generateSQLFromRelation(q: Relation, ctx: Context, addHeader: Boolean = true): GeneratedSQL =
    given Context = ctx
    val expanded  = expand(q, ctx)
    val gen       = SqlGenerator(ctx.dbType)(using ctx)
    val sql       = gen.print(expanded)

    val query: String =
      if addHeader then
        withHeader(sql, q.sourceLocation)
      else
        sql
    trace(s"[plan]\n${expanded.pp}\n[SQL]\n${query}")
    GeneratedSQL(query, expanded)

  def generateSaveSQL(save: Save, context: Context): List[String] =
    given Context  = context
    val statements = List.newBuilder[String]
    save match
      case s: SaveTo if s.isForTable =>
        val baseSQL = generateSQLFromRelation(save.inputRelation, context, addHeader = false)
        var needsTableCleanup = false
        val ctasCmd =
          if context.dbType.supportCreateOrReplace then
            s"create or replace table"
          else
            needsTableCleanup = true
            "create table"
        val options =
          if s.saveOptions.nonEmpty && context.dbType.supportCreateTableWithOption then
            val gen = SqlGenerator(context.dbType)(using context)
            s.saveOptions
              .map { opt =>
                s"${opt.key.fullName}=${gen.printExpression(opt.value)}"
              }
              .mkString(" with (", ", ", ")")
          else
            ""
        val ctasSQL = withHeader(
          s"${ctasCmd} ${s.targetName}${options} as\n${baseSQL.sql}",
          s.sourceLocation
        )

        if needsTableCleanup then
          val dropSQL = s"drop table if exists ${s.targetName}"
          // TODO: May need to wrap drop-ctas in a transaction
          statements += withHeader(dropSQL, s.sourceLocation)

        statements += ctasSQL
      case s: SaveTo if s.isForFile && context.dbType.supportSaveAsFile =>
        val baseSQL = GenSQL.generateSQLFromRelation(save.inputRelation, context, addHeader = false)
        val targetPath = context.dataFilePath(s.targetName)
        val copySQL    = s"copy (${baseSQL.sql}) to '${targetPath}'"
        val sql =
          if s.saveOptions.isEmpty then
            copySQL
          else
            val g = SqlGenerator(context.dbType)(using context)
            val opts = s
              .saveOptions
              .map { opt =>
                s"${opt.key.fullName} ${g.printExpression(opt.value)}"
              }
              .mkString("(", ", ", ")")
            s"${copySQL} ${opts}"
        statements += withHeader(sql, s.sourceLocation)
      case a: AppendTo if a.isForTable =>
        val baseSQL = GenSQL.generateSQLFromRelation(save.inputRelation, context, addHeader = false)
        val tbl     = TableName.parse(a.targetName)
        val schema  = tbl.schema.getOrElse(context.defaultSchema)
        val fullTableName = s"${schema}.${tbl.name}"
        val insertSQL =
          context.catalog.getTable(TableName.parse(fullTableName)) match
            case Some(t) =>
              s"insert into ${fullTableName}\n${baseSQL.sql}"
            case None =>
              s"create table ${fullTableName} as\n${baseSQL.sql}"
        statements += withHeader(insertSQL, save.sourceLocation)
      case a: AppendTo if a.isForFile && context.dbType == DBType.DuckDB =>
        val baseSQL = GenSQL.generateSQLFromRelation(save.inputRelation, context, addHeader = false)
        val targetPath = context.dataFilePath(a.targetName)
        if SourceIO.existsFile(targetPath) then
          val sql =
            s"""copy (
               |  (select * from '${targetPath}')
               |  union all
               |  ${baseSQL.sql}
               |)
               |to '${targetPath}' (USE_TMP_FILE true)""".stripMargin
          statements += withHeader(sql, a.sourceLocation)
        else
          val sql = s"create (${baseSQL.sql}) to '${targetPath}'"
          statements += withHeader(sql, a.sourceLocation)
      case d: Delete =>
        val gen = SqlGenerator(context.dbType)(using context)
        def filterExpr(x: Relation): Option[String] =
          x match
            case q: Query =>
              filterExpr(q.child)
            case f: Filter =>
              Some(gen.printExpression(f.filterExpr))
            case l: LeafPlan =>
              None
            case other =>
              throw StatusCode
                .SYNTAX_ERROR
                .newException(s"Unsupported delete input: ${other.nodeName}", other.sourceLocation)

        val filterSQL = filterExpr(d.inputRelation)
        var sql       = withHeader(s"delete from ${d.targetName}", d.sourceLocation)
        filterSQL.foreach { expr =>
          sql += s"\nwhere ${expr}"
        }
        List(sql)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"${other.nodeName} is not implemented yet for ${context.dbType}",
            other.sourceLocation(using context)
          )
    end match
    statements.result()

  end generateSaveSQL

  def generateExecute(expr: Expression, context: Context): String =
    given Context = context
    val gen       = SqlGenerator(context.dbType)(using context)
    val sql       = gen.printExpression(expr)
    withHeader(sql, expr.sourceLocation)

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
        case b: BackquoteInterpolatedIdentifier =>
          PreprocessLocalExpr.EvalBackquoteInterpolation.transformExpression(b, ctx)
        case i: Identifier =>
          val nme = Name.termName(i.leafName)
          ctx.scope.lookupSymbol(nme) match
            case Some(sym) =>
              sym.symbolInfo match
                case b: ValSymbolInfo =>
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
              val argSym = Symbol(ctx.global.newSymbolId, arg.span)

              given Context = ctx

              argSym.symbolInfo = ValSymbolInfo(
                ctx.owner,
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

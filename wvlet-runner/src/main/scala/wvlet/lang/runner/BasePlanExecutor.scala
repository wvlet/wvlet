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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.compiler.parser.SqlParser
import wvlet.lang.compiler.transform.ExpressionEvaluator
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

/**
  * The engine-agnostic `ExecutionPlan` interpreter shared by every platform: walks the plan, tracks
  * the last query result, evaluates `test` statements via [[TestEvaluator]], `val` definitions, and
  * the engine-independent commands. How SQL actually reaches an engine is left to subclasses
  * through [[executeQuery]] / [[runStatements]]; JVM-only plan nodes (save-to files, flows) default
  * to a clear not-supported error and are overridden by the JVM `QueryExecutor`. (The walk and
  * command handling moved verbatim from `QueryExecutor`.)
  */
abstract class BasePlanExecutor(val workEnv: WorkEnv) extends LogSupport with AutoCloseable:

  /** Run the compiled SQL of a query plan on the active engine and materialize the result. */
  protected def executeQuery(plan: LogicalPlan)(using Context): QueryResult

  /** Run side-effecting SQL statements (DDL, execute commands) on the active engine. */
  protected def runStatements(sqls: List[String])(using Context): Unit

  /** Handle `use <connector>[.<catalog>].<schema>`. */
  protected def executeUseConnector(u: UseConnector)(using Context): QueryResult

  /** Handle `use [schema] <name>` (a leading connector name switches the connector). */
  protected def executeUseSchema(u: UseSchema)(using Context): QueryResult

  protected def executeSave(save: Save)(using Context): QueryResult =
    throw notSupported("save statements")

  protected def executeFlow(flow: FlowDef)(using Context): QueryResult =
    throw notSupported("flow execution")

  protected def notSupported(what: String): Exception = StatusCode
    .NOT_IMPLEMENTED
    .newException(s"${what} are not supported by this runner; use the JVM wvlet CLI instead")

  def execute(executionPlan: ExecutionPlan, context: Context): QueryResult =
    var lastResult: QueryResult = QueryResult.empty
    val results                 = List.newBuilder[QueryResult]

    // TODO: Use an external reporting object to collect the results
    def report(r: QueryResult): QueryResult =
      if !r.isEmpty then
        results += r
        // Update the last result only when there is no error
        if r.isSuccessfulQueryResult then
          // TODO Add a unique name to the last result
          trace(s"last result is updated:\n${r}")
          lastResult = r
        // log results
        def isMultiline(s: String): Boolean = s.split("\n").size > 1

        r match
          case t: TestSuccess =>
            if isMultiline(t.msg) then
              workEnv.debug(s"Test passed: (${t.loc.locationString})\n${t.msg}")
            else
              workEnv.debug(s"Test passed: ${t.msg} (${t.loc.locationString})")
          case t: TestFailure =>
            if isMultiline(t.msg) then
              workEnv.error(s"Test failed: (${t.loc.locationString})\n${t.msg}")
            else
              workEnv.error(s"Test failed: ${t.msg} (${t.loc.locationString})")
          case w: WarningResult =>
            warn(s"${w.msg} (${w.loc.locationString})")
            workEnv.warn(s"Warning: ${w.msg}")
          case _ =>

      r

    def process(e: ExecutionPlan)(using ctx: Context): QueryResult =
      e match
        case ExecuteQuery(plan) =>
          report(executeQuery(plan))
        case ExecuteSave(save, queryPlan) =>
          // Evaluate test/debug if exists
          report(process(queryPlan))
          report(executeSave(save))
        case d @ ExecuteDebug(debugPlan, debugExecutionPlan) =>
          val debugInput = lastResult
          executeDebug(d)
          debugInput
        case ExecuteTest(test) =>
          trace(s"run test: ${test.testExpr}")
          report(TestEvaluator.evaluate(test, lastResult, workEnv))
        case ExecuteTasks(tasks) =>
          val results = tasks.map { task =>
            process(task)
          }
          QueryResult.fromList(results)
        case ExecuteCommand(e) =>
          // Command produces no QueryResult other than errors
          report(executeCommand(e))
        case ExecuteFlow(flow) =>
          report(executeFlow(flow))
        case ExecuteValDef(v) =>
          val expr = ExpressionEvaluator.eval(v.expr)(using ctx)
          v.symbol.symbolInfo = ValSymbolInfo(
            ctx.owner,
            v.symbol,
            v.name,
            expr.dataType,
            expr,
            ctx.compilationUnit
          )
          ctx.enter(v.symbol)
          QueryResult.empty
        case ExecuteNothing =>
          report(QueryResult.empty)

    process(executionPlan)(using context)
    // Prefer the last successful result when no results were accumulated.
    // This guards against execution paths that update `lastResult` but do not
    // add to the `results` builder (e.g., nested task flows).
    val aggregated = QueryResult.fromList(results.result())
    if aggregated.isEmpty && !lastResult.isEmpty then
      lastResult
    else
      aggregated

  end execute

  private def executeDebug(debugPlan: ExecuteDebug)(using context: Context): QueryResult =
    val result = execute(debugPlan.debugExecutionPlan, context)
    // TODO: Output to REPL
    workEnv.info(result)
    QueryResult.empty

  protected def executeCommand(cmd: Command)(using context: Context): QueryResult =
    cmd match
      case e: ExecuteExpr =>
        val cmd = GenSQL.generateExecute(e.expr)
        runStatements(List(cmd))
        QueryResult.empty
      case e: ExplainPlan =>
        // Expand RawSQL to a logical plan
        val plan = e
          .child
          .transformUp { case r: RawSQL =>
            val sql     = SqlGenerator(CodeFormatterConfig(sqlDBType = context.dbType)).print(r.sql)
            val unit    = CompilationUnit.fromSqlString(sql)
            val sqlPlan = SqlParser(unit).parse()
            var query: Option[Query] = None
            sqlPlan.traverseOnce { case q: Query =>
              query = Some(q)
            }
            query.getOrElse {
              throw StatusCode.SYNTAX_ERROR.newException(s"Failed to find query within SQL: ${sql}")
            }
          }
        val logicalPlanString = plan.pp
        println(s"\n${logicalPlanString}")
        QueryResult.empty
      case s: ShowQuery =>
        context.findTermSymbolByName(s.name.fullName) match
          case Some(sym) =>
            sym.tree match
              case md: ModelDef =>
                sym.symbolInfo match
                  case m: ModelSymbolInfo =>
                    val query = m
                      .compilationUnit
                      .text(md.child.span)
                      // Remove indentation
                      .split("\n")
                      .map(_.trim)
                      .mkString("\n")

                    // TODO Report query in the provided output
                    println(query)
                  case _ =>
              // TODO Support SelectAsAlias, already resolved models, etc.
              case _ =>
            QueryResult.empty
          case None =>
            WarningResult(s"${s.name} is not found", s.sourceLocation(using context))
      case u: UseConnector =>
        executeUseConnector(u)
      case u: UseSchema =>
        executeUseSchema(u)
      case d: DescribeInput =>
        // For now, just return empty result since DESCRIBE INPUT is mainly for parsing validation
        // In a full implementation, this would query the prepared statement input metadata
        workEnv.info(s"DESCRIBE INPUT ${d.name.fullName}")
        QueryResult.empty
      case d: DescribeOutput =>
        // For now, just return empty result since DESCRIBE OUTPUT is mainly for parsing validation
        // In a full implementation, this would query the prepared statement output metadata
        workEnv.info(s"DESCRIBE OUTPUT ${d.name.fullName}")
        QueryResult.empty
    end match

  end executeCommand

end BasePlanExecutor

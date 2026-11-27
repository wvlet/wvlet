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

import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.compiler.query.QueryProgressMonitor

/**
  * The last statement the REPL ran and its rendered output, kept for the `clip` commands
  */
case class LastOutput(
    line: String,
    output: String,
    result: QueryResult,
    error: Option[Throwable] = None
):
  def hasError: Boolean = error.isDefined

/**
  * Runs REPL statements one at a time over one session, so `use` and saved tables persist across
  * statements: the JVM's [[WvletScriptRunner]] (JDBC connectors, flows) and the cross-platform
  * [[PlanScriptRunner]] (npm and Native).
  */
trait ScriptRunner extends AutoCloseable:

  /** Compile and run a statement, or the part of it the request selects */
  def runStatement(request: QueryRequest)(using
      queryProgressMonitor: QueryProgressMonitor
  ): QueryResult

  /**
    * Print the query result to stdout, paging results wider than the terminal in interactive mode
    *
    * @param terminalWidth
    *   the number of terminal columns
    */
  def displayOutput(query: String, queryResult: QueryResult, terminalWidth: Int): LastOutput

  /**
    * Render the query result as a box cut to `maxWidth` columns without printing it, e.g., to show
    * it above the input being edited
    */
  def renderOutput(query: String, queryResult: QueryResult, maxWidth: Int): LastOutput

  /** The maximum number of result rows to show */
  def getResultRowLimit: Int

  /** Set the maximum number of result rows to show */
  def setResultRowLimit(limit: Int): Unit

  /** Set the maximum column width to show */
  def setMaxColWidth(size: Int): Unit

  /** The catalog that unqualified table names resolve in */
  def getCurrentCatalog: String

  /** The schema that unqualified table names resolve in */
  def getCurrentSchema: String

end ScriptRunner

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

import wvlet.lang.compiler.{CompilationUnit, WorkEnv}
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.airspec.AirSpec

class ExecutionPlanTest extends AirSpec:

  test("create an execution plan") {
    val workEnv = WorkEnv()
    val duckdb  = new DuckDBConnector(workEnv)
    val trino   =
      new TrinoConnector(
        TrinoConfig(catalog = "memory", schema = "main", hostAndPort = "localhost:8080"),
        workEnv
      )

    inline def query(q: String)(body: String => Unit)(using ctx: DBConnector): Unit =
      // var expr: SqlExpr = null
      test(s"query: ${q}") {
        val unit: CompilationUnit = CompilationUnit.fromWvletString(q)

        val parser = WvletParser(unit)
        val plan   = parser.parse()
        debug(plan)

//        val planner = ExecutionPlanner(using unit, ctx)
//        val expr    = planner.plan(plan).toSQL
//        debug(expr)
//        body(expr)
      }

    // Extend Standard SQL, which doesn't support string concatenation with '+'
    given dbx: DBConnector = duckdb
    query("select 'hello' + ' wvlet-ql!'"): sql =>
      sql shouldBe "select 'hello' || ' wvlet-ql!'"

  }

end ExecutionPlanTest

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

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

/**
  * Run-time checks of for-loops that spec/basic cannot express: tables named by interpolation are
  * only known at run time, so reading them back in a spec would count as untyped plans in
  * TyperCoverageCheck
  */
class ForLoopRunnerTest extends UniTest:
  private val workEnv             = WorkEnv(path = ".", logLevel = logger.getLogLevel)
  private val profile             = Profile.defaultDuckDBProfile
  private val dbConnectorProvider = ConnectorProvider(workEnv)
  private val queryExecutor       = QueryExecutor(dbConnectorProvider, profile, workEnv)

  override def afterAll: Unit =
    queryExecutor.close()
    dbConnectorProvider.close()

  private def run(query: String): QueryResult =
    val compiler = Compiler(CompilerOptions(workEnv = workEnv))
    compiler.setDefaultCatalog(queryExecutor.getDBConnector(profile).getCatalog("memory", "main"))
    val unit          = CompilationUnit.fromWvletString(query)
    val compileResult = compiler.compileSingleUnit(unit)
    val result        = queryExecutor.executeSingle(unit, compileResult.context)
    result.getError.foreach(e => throw e)
    result

  private def lastRows(result: QueryResult): List[List[String]] =
    def tables(r: QueryResult): List[TableRows] =
      r match
        case l: QueryResultList =>
          l.list.toList.flatMap(tables)
        case t: TableRows =>
          List(t)
        case _ =>
          Nil
    tables(result).last.rows.map(_.values.map(v => String.valueOf(v)).toList).toList

  test("interpolate fields of a bound row into save targets") {
    val result = run("""for p in (from [[1, 'a'], [2, 'b']] as t(id, label) select id, label) {
        |  select p.id * 10 as id, p.label as label
        |  save to s`for_loop_runner_${p.label}`
        |}
        |from for_loop_runner_b""".stripMargin)
    lastRows(result) shouldBe List(List("20", "b"))
  }

end ForLoopRunnerTest

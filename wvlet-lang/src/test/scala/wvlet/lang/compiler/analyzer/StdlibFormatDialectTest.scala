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
package wvlet.lang.compiler.analyzer

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL

/**
  * `s.format(args)` returns NULL for a NULL argument on every engine, so the variants for engines
  * that print NULL as 'null' check the arguments first
  */
class StdlibFormatDialectTest extends UniTest:
  private def generateSQL(wv: String, dbType: DBType): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private val query = "from [['a', 'b']] as t(x, y)\nselect '%s:%s'.format(x, y) as k\n"

  test("use the engine's printf where a NULL argument already gives NULL") {
    generateSQL(query, DBType.DuckDB) shouldContain "printf('%s:%s',x,y) as k"
    generateSQL(query, DBType.BigQuery) shouldContain "format('%s:%s',x,y) as k"
  }

  test("check for NULL arguments where the engine prints them as 'null'") {
    generateSQL(query, DBType.Trino) shouldContain
      "case when (x) is null or (y) is null then null else format('%s:%s',x,y) end as k"
    generateSQL(query, DBType.Hive) shouldContain
      "case when (x) is null or (y) is null then null else printf('%s:%s',x,y) end as k"
  }

end StdlibFormatDialectTest

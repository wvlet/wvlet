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

/** Generated SQL of stdlib aggregates whose variants differ by engine */
class StdlibAggregateDialectTest extends UniTest:
  private def generateSQL(wv: String, dbType: DBType): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private val mapAgg = "from [['a', 1]] as kv(k, v)\ngroup by 'all'\nagg k.map_agg(v) as m\n"

  test("build map_agg from key and value lists without NULL keys on DuckDB") {
    generateSQL(mapAgg, DBType.DuckDB) shouldContain
      "map(list(k) filter (where k is not null), list(v) filter (where k is not null)) as m"
  }

  test("use Trino's map_agg") {
    generateSQL(mapAgg, DBType.Trino) shouldContain "map_agg(k, v) as m"
  }

end StdlibAggregateDialectTest

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
  * `to_int` / `to_long` on fractional values truncate toward zero on every engine, so the variants
  * for engines whose casts round wrap the cast in their truncation function
  */
class StdlibCastDialectTest extends UniTest:
  private def generateSQL(wv: String, dbType: DBType): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private val query =
    """from [[2.5]] as t(x)
      |select x.to_int as a, x.to_double.to_long as b, x.to_double.to_boolean as c,
      |  x.to_float.to_long as f, x.to_long as l, x.to_boolean as xb
      |""".stripMargin

  test("truncate before casting on engines that round") {
    val duckdb = generateSQL(query, DBType.DuckDB)
    duckdb shouldContain "cast(trunc(x) as bigint) as a"
    duckdb shouldContain "cast(trunc(cast(x as double)) as bigint) as b"
    duckdb shouldContain "cast(cast(x as double) as boolean) as c"

    val trino = generateSQL(query, DBType.Trino)
    trino shouldContain "cast(truncate(x) as bigint) as a"
    trino shouldContain "cast(truncate(cast(x as double)) as bigint) as b"
    trino shouldContain "cast(truncate(cast(x as double)) as bigint) as f"

    val snowflake = generateSQL(query, DBType.Snowflake)
    snowflake shouldContain "cast(trunc(x) as bigint) as a"
    snowflake shouldContain "cast(trunc(x) as bigint) as l"

    val bigquery = generateSQL(query, DBType.BigQuery)
    bigquery shouldContain "cast(trunc(x) as int64) as a"
    bigquery shouldContain "(cast(x as float64) != 0) as c"
    bigquery shouldContain "(x != 0) as xb"
  }

  test("keep Hive's cast, which already truncates") {
    val hive = generateSQL(query, DBType.Hive)
    hive shouldContain "cast(x as bigint) as a"
    hive shouldContain "cast(cast(x as double) as bigint) as b"
  }

end StdlibCastDialectTest

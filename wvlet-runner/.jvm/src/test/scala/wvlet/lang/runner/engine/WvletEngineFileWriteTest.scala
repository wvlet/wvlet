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
package wvlet.lang.runner.engine

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.runner.PlanExecutor
import wvlet.lang.runner.QueryResult
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

/** `save to` / `append to` a data file on the wvlet engine, read back by the engine */
class WvletEngineFileWriteTest extends UniTest:
  private val workDir =
    Files.createTempDirectory(Files.createDirectories(Path.of("target")), "engine-write").toString

  private def run(query: String): QueryResult =
    val profile  = Profile("test", List(ConnectorConfig(name = "wvlet", `type` = "wvlet")))
    val workEnv  = WorkEnv(workDir, logLevel = LogLevel.WARN)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(workDir), workEnv = workEnv, dbType = DBType.Wvlet)
    )
    val unit   = CompilationUnit.fromWvletString(query)
    val result = compiler.compileSingleUnit(unit)
    result.reportAllErrors
    val ctx = result
      .context
      .withCompilationUnit(unit)
      .withDebugRun(true)
      .newContext(Symbol.NoSymbol)
    val provider = SqlConnectorProvider(profile)
    try PlanExecutor(provider, profile, workEnv).execute(ExecutionPlanner.plan(unit, ctx), ctx)
    finally provider.close()

  private val rows =
    """from [
      |  [1, 'a,"b"', 1.5, true, '2024-01-02'::date, 12.34],
      |  [2, null, null, false, null, null]
      |] as t(id, name, score, flag, d, amt)""".stripMargin

  private val expected =
    """[[1, 'a,"b"', 1.5, true, '2024-01-02', '12.34'], [2, null, null, false, null, null]]"""

  for ext <- Seq("parquet", "csv", "json") do
    test(s"save to a .${ext} file and read it back") {
      run(s"""${rows}
             |save to 'out/t.${ext}'""".stripMargin).getError.foreach(e => throw e)
      // A separate run: the compiler reads JSON files for their schema when compiling `from`
      val result = run(s"""from 'out/t.${ext}'
                          |order by id
                          |test _.rows should be ${expected}""".stripMargin)
      result.getError.foreach(e => throw e)
    }

  test("append to a Parquet file") {
    val result = run(s"""from [[1, 'a']] as t(id, name)
                        |save to 'out/append.parquet'
                        |
                        |from [[2, 'b']] as t(id, name)
                        |append to 'out/append.parquet'
                        |
                        |from [[3, 'c']] as t(id, name)
                        |append to 'out/new.parquet'
                        |
                        |from 'out/append.parquet'
                        |test _.rows should be [[1, 'a'], [2, 'b']]
                        |
                        |from 'out/new.parquet'
                        |test _.rows should be [[3, 'c']]""".stripMargin)
    result.getError.foreach(e => throw e)
  }

  test("append rows to a CSV file without rewriting the existing text") {
    Files.createDirectories(Path.of(workDir, "out"))
    Files.writeString(Path.of(workDir, "out/codes.csv"), "code,name\n007,bond\n")
    run("""from [['008', 'trevelyan']] as t(code, name)
          |append to 'out/codes.csv'""".stripMargin).getError.foreach(e => throw e)
    Files.readString(Path.of(workDir, "out/codes.csv")) shouldBe
      "code,name\n007,bond\n008,trevelyan\n"
  }

  test("keep empty strings distinct from NULL in CSV") {
    run("""from [[1, ''], [2, null]] as t(id, s)
          |save to 'out/empty.csv'""".stripMargin).getError.foreach(e => throw e)
    Files.readString(Path.of(workDir, "out/empty.csv")) shouldBe "id,s\n1,\"\"\n2,\n"
  }

  test("append rows to a JSON lines file under the file's column names") {
    run("""from [[1, 'a']] as t(id, name)
          |save to 'out/append.jsonl'
          |
          |from [[2, 'b']] as t(x, y)
          |append to 'out/append.jsonl'""".stripMargin).getError.foreach(e => throw e)
    Files.readString(Path.of(workDir, "out/append.jsonl")) shouldBe
      "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n"
  }

  test("write timestamps to Parquet without a UTC adjustment") {
    run("""select '2024-01-02 03:04:05'::timestamp as ts
          |save to 'out/ts.parquet'""".stripMargin).getError.foreach(e => throw e)
    val field = wvlet.uni.parquet.Parquet.open(s"${workDir}/out/ts.parquet").schema.columns.head
    field.logicalType.toString shouldContain "false"
    val result = run("""from 'out/ts.parquet'
                       |test _.rows should be [['2024-01-02 03:04:05']]""".stripMargin)
    result.getError.foreach(e => throw e)
  }

  test("reject decimals wider than the column precision, keeping the file") {
    run("""from [[1.5]] as t(v)
          |save to 'out/dec.parquet'""".stripMargin).getError.foreach(e => throw e)
    val e = intercept[wvlet.lang.api.WvletLangException] {
      run("""from [[123.5]] as t(v)
            |append to 'out/dec.parquet'""".stripMargin)
    }
    e.getMessage shouldContain "doesn't fit"
    val result = run("""from 'out/dec.parquet'
                       |test _.rows should be [['1.5']]""".stripMargin)
    result.getError.foreach(e => throw e)
  }

  test("compress Parquet files with the compression option") {
    val result = run("""from [[1, 'a']] as t(id, name)
                       |save to 'out/zstd.parquet' with compression: 'zstd'
                       |
                       |from 'out/zstd.parquet'
                       |test _.rows should be [[1, 'a']]""".stripMargin)
    result.getError.foreach(e => throw e)
    val meta = wvlet.uni.parquet.Parquet.open(s"${workDir}/out/zstd.parquet").metadata
    meta.toString shouldContain "ZSTD"
  }

end WvletEngineFileWriteTest

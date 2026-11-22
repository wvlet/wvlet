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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
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
import scala.jdk.CollectionConverters.*

/** Iceberg tables on the wvlet engine: create and append through uni, and read pyiceberg tables */
class WvletEngineIcebergTest extends UniTest:
  private val workDir =
    Files.createTempDirectory(Files.createDirectories(Path.of("target")), "engine-iceberg").toString

  private val fixtures = Path.of("wvlet-runner/.jvm/src/test/resources/iceberg").toAbsolutePath

  private def fixture(name: String): String = fixtures.resolve(name).toString

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

  private def check(query: String): Unit = run(query).getError.foreach(e => throw e)

  private def failure(query: String): WvletLangException =
    val e =
      try
        run(query).getError
      catch
        case e: WvletLangException =>
          Some(e)
    e match
      case Some(w: WvletLangException) =>
        w
      case Some(other) =>
        throw other
      case None =>
        fail(s"Expected an error: ${query}")

  private def filesUnder(dir: String): Seq[String] =
    val root = Path.of(workDir, dir)
    Files
      .walk(root)
      .iterator()
      .asScala
      .filter(Files.isRegularFile(_))
      .map(p => root.relativize(p).toString)
      .toSeq
      .sorted

  test("create an Iceberg table with save to and read it back") {
    check("""from [[1, 'a', 1.5], [2, null, null]] as t(id, name, score)
            |save to 'out/t1' with format: 'iceberg'""".stripMargin)
    val files = filesUnder("out/t1")
    files shouldContain "metadata/v1.metadata.json"
    files shouldContain "metadata/version-hint.text"
    files.exists(f => f.startsWith("data/") && f.endsWith(".parquet")) shouldBe true
    check("""from 'out/t1'
            |order by id
            |test _.rows should be [[1, 'a', 1.5], [2, null, null]]""".stripMargin)
  }

  test("append to adds a snapshot, casting rows to the table columns by position") {
    check("""from [[1, 'a']] as t(id, name)
            |save to 'out/t2' with format: 'iceberg'
            |
            |from [[2, 'b']] as t(x, y)
            |append to 'out/t2'
            |
            |from [['3', 'c']] as t(x, y)
            |append to 'out/t2'""".stripMargin)
    filesUnder("out/t2") shouldContain "metadata/v3.metadata.json"
    check("""from 'out/t2'
            |order by id
            |test _.rows should be [[1, 'a'], [2, 'b'], [3, 'c']]""".stripMargin)
    // A metadata file path reads the snapshot it records
    check("""from 'out/t2/metadata/v2.metadata.json'
            |order by id
            |test _.rows should be [[1, 'a'], [2, 'b']]""".stripMargin)
  }

  test("partition an Iceberg table with partition_by") {
    check("""from [[1, 'JP', 10], [2, 'US', 20], [3, 'JP', 30]] as t(id, country, v)
            |save to 'out/t3' with format: 'iceberg', partition_by: 'country'
            |
            |from [[4, 'US', 40]] as t(id, country, v)
            |append to 'out/t3'""".stripMargin)
    val files = filesUnder("out/t3")
    files.exists(_.startsWith("data/country=JP/")) shouldBe true
    files.count(_.startsWith("data/country=US/")) shouldBe 2
    check("""from 'out/t3'
            |where country = 'US'
            |order by id
            |test _.rows should be [[2, 'US', 20], [4, 'US', 40]]""".stripMargin)
  }

  test("refuse save to an existing Iceberg table, keeping its files") {
    check("""from [[1]] as t(id)
            |save to 'out/t4' with format: 'iceberg'""".stripMargin)
    val before = filesUnder("out/t4")
    val e      = failure("""from [[2]] as t(id)
                       |save to 'out/t4' with format: 'iceberg'""".stripMargin)
    e.getMessage shouldContain "already an Iceberg table"
    e.getMessage shouldContain "append to"
    filesUnder("out/t4") shouldBe before
  }

  test("refuse an Iceberg table in a folder that holds other files") {
    Files.createDirectories(Path.of(workDir, "out/plain"))
    Files.writeString(Path.of(workDir, "out/plain/notes.txt"), "keep me")
    failure("""from [[1]] as t(id)
              |save to 'out/plain' with format: 'iceberg'""".stripMargin).getMessage shouldContain
      "not an empty folder"
    filesUnder("out/plain") shouldBe Seq("notes.txt")
  }

  test("append to a folder that is not an Iceberg table suggests format: 'iceberg'") {
    val e = failure("""from [[1]] as t(id)
                      |append to 'out/missing'""".stripMargin)
    e.getMessage shouldContain "format: 'iceberg'"
    Files.exists(Path.of(workDir, "out/missing")) shouldBe false
  }

  test("reject unknown Iceberg write options and missing partition columns") {
    failure("""from [[1]] as t(id)
              |save to 'out/t5' with format: 'iceberg', header: true""".stripMargin)
      .getMessage shouldContain "Unsupported Iceberg write option"
    failure("""from [[1]] as t(id)
              |save to 'out/t6' with format: 'iceberg', partition_by: 'country'""".stripMargin)
      .getMessage shouldContain "Partition column country"
  }

  test("read the current snapshot of a partitioned pyiceberg table") {
    check(s"""from '${fixture("events")}'
             |order by id
             |select id, name, country
             |test _.rows should be [[1, 'n1', 'JP'], [3, 'n3', 'JP'], [4, 'n4', 'US'], [5, 'n5', 'US'], [6, 'n6', 'JP']]
             |""".stripMargin)
    check(s"""from '${fixture("events")}'
             |where dt = '2026-11-20'::date
             |order by id
             |select id
             |test _.rows should be [[4], [5]]""".stripMargin)
  }

  test("read an evolved schema: promoted, added, and dropped columns") {
    check(s"""from '${fixture("evolved")}'
             |order by id
             |test _.columns should be ['id', 'n', 'added']
             |test _.rows should be [[1, 10, null], [2, 20, 'y']]""".stripMargin)
  }

  test("refuse a table with delete files with a clear error") {
    val e = failure(s"""from '${fixture("deletes")}'""")
    e.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
    e.getMessage shouldContain "can't read the Iceberg table"
    e.getMessage shouldContain "delete files"
  }

  test("refuse append to a table that a catalog owns, keeping its files") {
    val src = fixtures.resolve("events")
    val dst = Path.of(workDir, "out/catalog_events")
    Files
      .walk(src)
      .iterator()
      .asScala
      .foreach { p =>
        val target = dst.resolve(src.relativize(p).toString)
        if Files.isDirectory(p) then
          Files.createDirectories(target)
        else
          Files.copy(p, target)
      }
    val before = filesUnder("out/catalog_events")
    val e      = failure("""from [[7, 'n7', '2026-11-21'::date, 'JP']] as t(id, name, dt, country)
                      |append to 'out/catalog_events'""".stripMargin)
    e.getMessage shouldContain "catalog owns"
    filesUnder("out/catalog_events") shouldBe before
  }

end WvletEngineIcebergTest

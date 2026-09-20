/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.external

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.ext.ExternalFunction
import wvlet.lang.ext.FunctionProvider
import wvlet.lang.ext.ScalarFunction
import wvlet.lang.ext.TableFunction
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.runner.FlowExecutor
import wvlet.lang.runner.FlowRunStore
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.json.JSON.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

/**
  * JVM functions discovered through META-INF/services/wvlet.lang.ext.FunctionProvider of the test
  * resources, i.e. the same ServiceLoader path a plugin jar uses
  */
class TestFunctionProvider extends FunctionProvider:
  override def functions: Seq[ExternalFunction] = Seq(
    new ScalarFunction:
      override def name: String                = "test_shout"
      override def eval(args: Seq[Any]): Any = s"${args.head.toString.toUpperCase}!"
    ,
    new TableFunction:
      override def name: String                                                      = "test_repeat"
      override def apply(args: JSONObject, input: Iterator[JSONObject]): JSONObject =
        val times =
          args.get("times") match
            case Some(JSONLong(n)) =>
              n.toInt
            case _ =>
              1
        val rows = input.flatMap(r => Seq.fill(times)(r)).toIndexedSeq
        JSONObject(Seq("rows" -> JSONArray(rows), "input_rows" -> JSONLong(rows.size / times)))
  )

class ExternalFunctionTest extends UniTest:
  private val tempDirs = List.newBuilder[Path]

  override def afterAll: Unit = tempDirs
    .result()
    .foreach { dir =>
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p => Files.deleteIfExists(p))
    }

  private def newWorkDir(plugins: (String, String)*): WorkEnv =
    val dir = Files.createTempDirectory(Path.of("target"), "external-function-test")
    tempDirs += dir
    if plugins.nonEmpty then
      val pluginDir = Files.createDirectory(dir.resolve("plugins"))
      plugins.foreach((name, code) => Files.writeString(pluginDir.resolve(name), code))
    WorkEnv(dir.toString)

  /** Compile and run the queries; embedded `test` statements assert on the results */
  private def run(wv: String, workEnv: WorkEnv = WorkEnv(".")): Unit =
    val provider = ConnectorProvider(workEnv)
    try
      val compiler = Compiler(CompilerOptions(workEnv = workEnv))
      val unit     = CompilationUnit.fromWvletString(wv)
      val result   = compiler.compileSingleUnit(unit)
      val executor = QueryExecutor(provider, Profile.defaultDuckDBProfile, workEnv)
      executor.executeSingle(unit, result.context).getError.foreach(e => throw e)
    finally
      provider.close()

  private val people =
    """from [[1, 'ann', 'tokyo'], [2, 'bob', 'paris']] as people(id, name, city)"""

  // Node 22.6+ runs TypeScript modules by stripping types; older versions only run JavaScript
  private lazy val nodeVersion: Option[(Int, Int)] =
    try
      val p   = ProcessBuilder("node", "--version").redirectErrorStream(true).start()
      val out = String(p.getInputStream.readAllBytes()).trim
      p.waitFor()
      out.stripPrefix("v").split('.').toList match
        case major :: minor :: _ =>
          Some((major.toInt, minor.toInt))
        case _ =>
          None
    catch
      case _: Exception =>
        None

  private def requireNode(typescript: Boolean): Unit =
    nodeVersion match
      case None =>
        skip("node is not available")
      case Some(v) if typescript && Ordering[(Int, Int)].lt(v, (22, 6)) =>
        skip(s"TypeScript modules need Node 22.6+, found ${v}")
      case _ =>

  test("apply a JVM table function from a function provider to a relation") {
    run(s"""type person = {
           |  id: int
           |  name: string
           |  city: string
           |}
           |def test_repeat(times: int): person = native
           |
           |${people}
           || test_repeat(2)
           |test _.size should be 4
           |test _.columns should be ['id', 'name', 'city']
           |
           |${people}
           || where id = 1
           || test_repeat(times = 3)
           || where city = 'tokyo'
           |test _.size should be 3
           |""".stripMargin)
  }

  test("call a JVM scalar function in select and where") {
    run(s"""def test_shout(s: string): string = native
           |
           |${people}
           |select id, test_shout(name) as loud
           |test _.rows should be [[1, 'ANN!'], [2, 'BOB!']]
           |
           |${people}
           |where test_shout(city) = 'PARIS!'
           |test _.columns should be ['id', 'name', 'city']
           |test _.rows should be [[2, 'bob', 'paris']]
           |
           |${people}
           |add test_shout(concat(name, '@', city)) as tag
           |select id, tag
           |test _.rows should be [[1, 'ANN@TOKYO!'], [2, 'BOB@PARIS!']]
           |""".stripMargin)
  }

  test("evaluate nested scalar calls inside-out") {
    run(s"""def test_shout(s: string): string = native
           |
           |${people}
           |select id, test_shout(test_shout(name)) as louder
           |test _.rows should be [[1, 'ANN!!'], [2, 'BOB!!']]
           |""".stripMargin)
  }

  test("run table and scalar functions exported by a JavaScript module") {
    requireNode(typescript = false)
    val workEnv = newWorkDir(
      "udf.mjs" ->
        """export async function js_top(rows, args) {
          |  const out = []
          |  for await (const r of rows) if (r.score >= args.threshold) out.push(r)
          |  return { rows: out, model: 'v3' }
          |}
          |export function js_len(s) { return s.length }
          |""".stripMargin
    )
    run(
      """type scored = {
        |  id: int
        |  score: double
        |}
        |def js_top(threshold: double): scored = native
        |def js_len(s: string): long = native
        |
        |from [[1, 0.5], [2, 0.9]] as t(id, score)
        || js_top(0.8)
        |test _.rows should be [[2, 0.9]]
        |
        |from [[1, 'abc'], [2, 'de']] as t(id, name)
        |select id, js_len(name) as len
        |test _.rows should be [[1, 3], [2, 2]]
        |""".stripMargin,
      workEnv
    )
  }

  test("run a function exported by a TypeScript module") {
    requireNode(typescript = true)
    val workEnv = newWorkDir(
      "geo.ts" ->
        """type Row = { id: number; city: string }
          |export async function ts_cities(rows: AsyncIterable<Row>, args: { suffix: string }) {
          |  const out: Row[] = []
          |  for await (const r of rows) out.push({ id: r.id, city: r.city + args.suffix })
          |  return { rows: out }
          |}
          |""".stripMargin
    )
    run(
      """type place = {
        |  id: int
        |  city: string
        |}
        |def ts_cities(suffix: string): place = native
        |
        |from [[1, 'tokyo']] as t(id, city)
        || ts_cities('-jp')
        |test _.rows should be [[1, 'tokyo-jp']]
        |""".stripMargin,
      workEnv
    )
  }

  test("report a native table function without an implementation") {
    val e = intercept[WvletLangException] {
      run("""type r = {
            |  id: int
            |}
            |def no_such_impl: r = native
            |from no_such_impl()
            |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.FUNCTION_NOT_FOUND
  }

  test("fail with the stderr of a failing shell function") {
    val e = intercept[WvletLangException] {
      run("""type r = {
            |  id: int
            |}
            |def broken: r = sh"echo 'boom: bad input' >&2; exit 3"
            |from broken()
            |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.EXTERNAL_FUNCTION_FAILED
    e.getMessage shouldContain "exit code 3"
    e.getMessage shouldContain "boom: bad input"
  }

  test("reject a shell function called in an expression") {
    val e = intercept[WvletLangException] {
      run("""type r = {
            |  id: int
            |}
            |def as_table: r = sh"cat"
            |from [[1]] as t(id)
            |select as_table() as x
            |""".stripMargin)
    }
    e.getMessage shouldContain "table function"
  }

  test("run an external function in a flow stage and record its metadata") {
    val workEnv = newWorkDir()
    // Shell functions run in the working folder
    Files.writeString(
      Path.of(workEnv.path, "rescore-result.json"),
      """{"rows":[{"id":1,"score":0.9},{"id":2,"score":0.1}],"model":"v7"}"""
    )
    val provider = ConnectorProvider(workEnv)
    val store    = FlowRunStore.ofType("sqlite", workEnv)
    try
      val compiler = Compiler(CompilerOptions(workEnv = workEnv))
      val unit     = CompilationUnit.fromWvletString(
        """type scored = {
          |  id: int
          |  score: double
          |}
          |def rescore: scored = sh"cat rescore-result.json"
          |
          |flow Scoring = {
          |  stage orders = from [[1], [2]] as t(id)
          |  stage scored = from orders | rescore
          |  stage top = from scored | where score > 0.5
          |}
          |""".stripMargin
      )
      val ctx = compiler
        .compileSingleUnit(unit)
        .context
        .withCompilationUnit(unit)
        .newContext(Symbol.NoSymbol)
      var flow: Option[FlowDef] = None
      unit.resolvedPlan.traverse { case f: FlowDef =>
        flow = Some(f)
      }
      val connector = provider.getConnector(Profile.defaultDuckDBProfile)
      val result    = FlowExecutor(connector, workEnv, registry = Some(store)).execute(flow.get)(using
        ctx
      )
      result.isSuccess shouldBe true
      val top = result.stageResult("top").flatMap(_.table).get
      connector.runQuery(s"""select count(*) from "${top}"""") { rs =>
        rs.next()
        rs.getLong(1)
      } shouldBe 1L

      val record = store.get(result.runId).get
      val meta   = record.stages.find(_.name == "scored").flatMap(_.metadata).get
      meta shouldContain "\"model\":\"v7\""
      record.stages.find(_.name == "top").flatMap(_.metadata) shouldBe None
    finally
      store.close()
      provider.close()
  }

end ExternalFunctionTest

package wvlet.lang.runner

import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.expr.DoubleQuotedIdentifier
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.model.plan.TableRef
import wvlet.lang.runner.activity.ActivityExecutor
import wvlet.lang.runner.activity.ActivityRequest
import wvlet.lang.runner.activity.ActivityResult
import wvlet.lang.runner.activity.CancelAck
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.engine.WvletEngineConnector
import wvlet.uni.test.UniTest

import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

/**
  * Flows and `wvlet build` on the in-process wvlet engine: stages run as Wvlet plans, with no SQL
  * connector
  */
class EngineFlowExecutorTest extends UniTest:
  private val workEnv  = WorkEnv(".")
  private val provider = ConnectorProvider(workEnv)
  private val engine   = provider.getConnector(Profile.defaultWvletProfile.defaultEngine)

  override def afterAll: Unit = provider.close()

  private def compileFlow(wv: String): (FlowDef, Context) =
    val compiler              = Compiler(CompilerOptions(workEnv = workEnv))
    val unit                  = CompilationUnit.fromWvletString(wv)
    val result                = compiler.compileSingleUnit(unit)
    val ctx                   = result.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    var flow: Option[FlowDef] = None
    unit
      .resolvedPlan
      .traverse { case f: FlowDef =>
        if flow.isEmpty then
          flow = Some(f)
      }
    (flow.getOrElse(fail("No FlowDef found in the compiled plan")), ctx)

  private def rowsOf(connector: Any, table: String)(using Context): Seq[Map[String, Any]] =
    connector
      .asInstanceOf[WvletEngineConnector]
      .session
      .execute(TableRef(DoubleQuotedIdentifier(table, Span.NoSpan), Span.NoSpan), Int.MaxValue)
      .rows
      .map(_.toMap)

  private def stageRows(result: FlowExecutionResult, stage: String)(using
      Context
  ): Seq[Map[String, Any]] = rowsOf(
    engine,
    result.stageResult(stage).flatMap(_.table).getOrElse(fail(s"Stage ${stage} has no table"))
  )

  test("run query stages, routes, and merges on the engine") {
    val (flow, ctx) = compileFlow("""flow EngineFlow = {
        |  stage src = from [[1, 25], [2, 15], [3, 40]] as t(id, age)
        |  stage gate = from src | route {
        |    case _.age >= 18 -> adult
        |    else -> minor
        |  }
        |  stage adult = from gate | select id
        |  stage minor = from gate | select id
        |  stage all = merge adult, minor
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv).execute(flow)
    result.isSuccess shouldBe true
    stageRows(result, "adult").map(_("id")).toSet shouldBe Set(1L, 3L)
    stageRows(result, "minor").map(_("id")) shouldBe Seq(2L)
    stageRows(result, "all").size shouldBe 3
  }

  test("pass a sensor whose condition holds on the engine") {
    val (flow, ctx) = compileFlow("""flow EngineSensorFlow = {
        |  stage src = from [[10], [0]] as t(x)
        |  stage gate with { poll_interval: 20ms } = from src | wait until _.x > 5
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv).execute(flow)
    result.isSuccess shouldBe true
    stageRows(result, "gate").size shouldBe 2
  }

  test("resume a failed run on the engine") {
    val registry    = FlowRunRegistry(Files.createTempDirectory("wv-engine-flow-resume"))
    var fail        = true
    val (flow, ctx) = compileFlow("""flow EngineResumeFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage out = from src | select id
        |}""".stripMargin)
    given Context = ctx
    val failing   =
      new FlowStageRunner:
        override def run(stage: wvlet.lang.model.plan.StageDef, targetTable: String)(using
            Context
        ): Unit =
          if fail then
            throw StatusCode.UNEXPECTED_STATE.newException("injected failure")
    val first = FlowExecutor(
      engine,
      workEnv,
      stageRunner = Some(failing),
      registry = Some(registry)
    ).execute(flow)
    first.hasError shouldBe true
    fail = false
    val resumed = FlowExecutor(engine, workEnv, registry = Some(registry)).execute(
      flow,
      resumeFrom = registry.get(first.runId)
    )
    resumed.isSuccess shouldBe true
    stageRows(resumed, "out").size shouldBe 2
  }

  test("record one row per item of an activity stage on the engine") {
    val echo =
      new ActivityExecutor:
        override def name: String = "test.echo"
        override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
          ActivityResult.Completed(Map("echo" -> request.input("item")))
        override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged
    val (flow, ctx) = compileFlow("""flow EngineActivityFlow = {
        |  stage src = from [['a'], ['b'], ['a']] as t(name)
        |  stage calls = from src | for each _.name activity 'test.echo'
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv, activityExecutors = List(echo)).execute(flow)
    result.isSuccess shouldBe true
    val rows = stageRows(result, "calls")
    rows.map(_("item_key")).toSet shouldBe Set("a", "b")
    rows.map(_("state")).toSet shouldBe Set("succeeded")
    rows.map(_("attempts")).toSet shouldBe Set(1)
  }

  /** A sink recording the request and the JSON rows of the delivered table */
  private class RecordingSink(override val name: String) extends ActivationSink:
    val delivered = ListBuffer.empty[(ActivationRequest, List[String])]
    override def activate(request: ActivationRequest): Unit =
      delivered += request -> request.jsonRows(10)

  test("deliver activated stage outputs to a sink on the engine") {
    val sink        = RecordingSink("collector")
    val (flow, ctx) = compileFlow("""flow EngineActivateFlow = {
        |  stage src = from [[1, 'a'], [2, 'b']] as t(id, name)
        |  stage send = from src | activate('collector', channel: 'sales')
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv, activationSinks = List(sink)).execute(flow)
    result.isSuccess shouldBe true
    sink.delivered.size shouldBe 1
    val (request, rows) = sink.delivered.head
    request.params shouldBe Map("channel" -> "sales")
    request.sqlConnector.isEmpty shouldBe true
    rows.toSet shouldBe Set("""{"id":1,"name":"a"}""", """{"id":2,"name":"b"}""")
  }

  test("export activated stage outputs to files on the engine") {
    val dir         = Files.createTempDirectory("wv-engine-activate")
    val csv         = dir.resolve("out.csv")
    val json        = dir.resolve("out.json")
    val (flow, ctx) = compileFlow(s"""flow EngineFileFlow = {
        |  stage src = from [[1, 'a'], [2, 'b']] as t(id, name)
        |  stage to_csv = from src | activate('file', path: '${csv}')
        |  stage to_json = from src | activate('file', path: '${json}')
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(
      engine,
      workEnv,
      activationSinks = FlowExecutor.defaultActivationSinks
    ).execute(flow)
    result.isSuccess shouldBe true
    Files.readAllLines(csv).asScala.toList shouldBe List("id,name", "1,a", "2,b")
    Files.readAllLines(json).asScala.toList shouldBe
      List("""{"id":1,"name":"a"}""", """{"id":2,"name":"b"}""")
  }

  test("deliver run notifications on the engine") {
    val sink        = RecordingSink("test_notify")
    val (flow, ctx) = compileFlow(
      """flow EngineNotifyFlow with { on_finish: activate('test_notify') } = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin
    )
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv, activationSinks = List(sink)).execute(flow)
    result.isSuccess shouldBe true
    sink.delivered.size shouldBe 1
    val rows = sink.delivered.head._2
    rows.size shouldBe 1
    rows.head shouldContain "\"stage\":\"src\""
    rows.head shouldContain "\"state\":\"success\""
  }

  test("drop run tables on the engine") {
    val (flow, ctx) = compileFlow("""flow EngineDropFlow = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin)
    given Context = ctx
    val result    = FlowExecutor(engine, workEnv).execute(flow)
    val table     = result.stageResult("src").flatMap(_.table).get
    val session   = engine.asInstanceOf[WvletEngineConnector].session
    session.hasTable(table) shouldBe true
    FlowExecutor.dropRunTables(engine, result.runId, List("src"))
    session.hasTable(table) shouldBe false
  }

  // --- wvlet build ---

  private def project(files: (String, String)*): String =
    val dir = Files.createTempDirectory("wvlet-engine-build-test")
    files.foreach { (name, content) =>
      Files.writeString(dir.resolve(name), content)
    }
    dir.toAbsolutePath.toString

  private def warehouseProfile(warehouse: String): Profile = Profile(
    name = "local",
    connectors = Seq(
      ConnectorConfig(name = "wvlet", `type` = "wvlet", properties = Map("warehouse" -> warehouse))
    )
  )

  private def buildOn(profile: Profile, dir: String, selectors: List[String] = Nil) =
    val plan  = ModelBuildLauncher.plan(dir, workEnv, selectors, DBType.Wvlet)
    val store = FlowRunRegistry(Files.createTempDirectory("wv-engine-build-runs"))
    (plan, ModelBuildLauncher.execute(plan, profile, workEnv, store))

  private val shop = Seq("models.wv" -> """model raw_orders = {
        |  from [[1, 'alice', 100], [2, 'bob', 50], [3, 'alice', 30]] as t(id, customer, amount)
        |}
        |
        |model stg_orders with { materialized: 'table', alias: 'eb_stg_orders' } = {
        |  from raw_orders
        |  where amount > 40
        |}
        |
        |model customer_totals with { materialized: 'table', alias: 'eb_customer_totals' } = {
        |  from stg_orders
        |  group by customer
        |  agg amount.sum as total
        |  order by customer
        |}
        |""".stripMargin)

  test("build table models on the engine and persist them in the warehouse") {
    val dir            = project(shop*)
    val warehouse      = Files.createTempDirectory("wv-engine-warehouse").toString
    val profile        = warehouseProfile(warehouse)
    val (plan, result) = buildOn(profile, dir)
    result.hasError shouldBe false
    result.stageResult("customer_totals").map(_.state) shouldBe Some(StageState.Success)

    // A new session reads the built tables back from the warehouse
    val reader = ConnectorProvider(workEnv)
    try
      given Context = plan.context
      val rows      = rowsOf(reader.getConnector(profile.defaultEngine), "eb_customer_totals")
      rows.map(r => (r("customer"), r("total"))).toSet shouldBe Set(("alice", 100L), ("bob", 50L))

      // The selection's upstream is already built, so building only the mart passes the check
      val (_, again) = buildOn(profile, dir, List("customer_totals"))
      again.hasError shouldBe false
    finally
      reader.close()
  }

  test("fail a build whose unselected upstream is not built on the engine") {
    val dir       = project(shop*)
    val warehouse = Files.createTempDirectory("wv-engine-warehouse-empty").toString
    val e         = intercept[WvletLangException] {
      buildOn(warehouseProfile(warehouse), dir, List("customer_totals"))
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
    e.getMessage shouldContain "stg_orders"
  }

  test("report view models as not supported on the engine") {
    val dir = project("models.wv" -> """model v with { materialized: 'view', alias: 'eb_view' } = {
          |  from [[1]] as t(id)
          |}
          |""".stripMargin)
    val (_, result) = buildOn(Profile.defaultWvletProfile, dir)
    result.hasError shouldBe true
    val message = result.stageResult("v").get.error.get.getMessage
    message shouldContain "not supported by the wvlet engine"
    message shouldContain "-t duckdb"
  }

end EngineFlowExecutorTest

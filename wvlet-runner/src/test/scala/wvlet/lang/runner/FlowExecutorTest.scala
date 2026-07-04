package wvlet.lang.runner

import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.planner.ExecutionPlanner
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.uni.test.UniTest

import scala.collection.mutable.ListBuffer

/**
  * Tests for the stage execution model of FlowExecutor using DuckDB
  */
class FlowExecutorTest extends UniTest:
  private val workEnv             = WorkEnv(".")
  private val profile             = Profile.defaultDuckDBProfile
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val connector           = dbConnectorProvider.getConnector(profile)

  override def afterAll: Unit = dbConnectorProvider.close()

  private def compileFlowUnit(wv: String): (CompilationUnit, FlowDef, Context) =
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
    (unit, flow.getOrElse(fail("No FlowDef found in the compiled plan")), ctx)

  private def compileFlow(wv: String): (FlowDef, Context) =
    val (_, flow, ctx) = compileFlowUnit(wv)
    (flow, ctx)

  private def runFlow(wv: String, sleeper: Long => Unit = _ => ()): FlowExecutionResult =
    val (flow, ctx) = compileFlow(wv)
    FlowExecutor(connector, workEnv, sleeper).execute(flow)(using ctx)

  private def countRows(table: String): Long =
    connector.runQuery(s"select count(*) cnt from ${table}") { rs =>
      rs.next()
      rs.getLong(1)
    }

  test("run all stages of a successful flow in order") {
    val result = runFlow("""flow SimpleFlow = {
        |  stage src = from [[1, 'a'], [2, 'b'], [3, 'a']] as t(id, name)
        |  stage filtered = from src | where name = 'a'
        |  stage output = from filtered | select id
        |}""".stripMargin)
    result.isSuccess shouldBe true
    result.stageResults.map(_.state) shouldBe
      List(StageState.Success, StageState.Success, StageState.Success)
    result.stageResults.map(_.attempts) shouldBe List(1, 1, 1)
    // Each stage is materialized as a queryable temp table
    countRows("filtered") shouldBe 2L
    countRows("output") shouldBe 2L
  }

  test("skip downstream stages when an upstream stage fails") {
    val result = runFlow("""flow FailingFlow = {
        |  stage primary = from nonexistent_table_xyz
        |  stage transform = from primary | select *
        |}""".stripMargin)
    result.isSuccess shouldBe false
    result.stageResult("primary").get.state shouldBe StageState.Failed
    result.stageResult("primary").get.error.isDefined shouldBe true
    result.stageResult("transform").get.state shouldBe StageState.Skipped
    result.stageResult("transform").get.attempts shouldBe 0
  }

  test("run a fallback stage when the primary stage fails") {
    val result = runFlow("""flow ResilientFlow = {
        |  stage primary = from nonexistent_table_xyz
        |  stage fallback if primary.failed = from [[1]] as t(id)
        |  stage cleanup if primary.done = from [[1], [2]] as t(id)
        |}""".stripMargin)
    result.stageResult("primary").get.state shouldBe StageState.Failed
    result.stageResult("fallback").get.state shouldBe StageState.Success
    result.stageResult("cleanup").get.state shouldBe StageState.Success
  }

  test("skip a trigger stage when its condition does not hold") {
    val result = runFlow("""flow HealthyFlow = {
        |  stage src = from [[1]] as t(id)
        |  stage alert if src.failed = from [[1]] as t(id)
        |  stage notify if src.done = from [[1]] as t(id)
        |}""".stripMargin)
    result.stageResult("src").get.state shouldBe StageState.Success
    result.stageResult("alert").get.state shouldBe StageState.Skipped
    result.stageResult("notify").get.state shouldBe StageState.Success
  }

  test("evaluate and/or trigger conditions") {
    val result = runFlow("""flow TriggerFlow = {
        |  stage a = from [[1]] as t(id)
        |  stage b = from nonexistent_table_xyz
        |  stage alert if a.failed or b.failed = from [[1]] as t(id)
        |  stage summary if a.done and b.done = from [[1]] as t(id)
        |  stage never if a.failed and b.failed = from [[1]] as t(id)
        |}""".stripMargin)
    result.stageResult("alert").get.state shouldBe StageState.Success
    result.stageResult("summary").get.state shouldBe StageState.Success
    result.stageResult("never").get.state shouldBe StageState.Skipped
  }

  test("retry a failing stage with the configured backoff") {
    val delays = ListBuffer.empty[Long]
    val result = runFlow(
      """flow RetryFlow = {
        |  stage flaky with {
        |    retries: 2
        |    retry_delay: 10ms
        |  } = from nonexistent_table_xyz
        |}""".stripMargin,
      sleeper = delays += _
    )
    val flaky = result.stageResult("flaky").get
    flaky.state shouldBe StageState.Failed
    flaky.attempts shouldBe 3
    // Default exponential backoff: 10ms, then 20ms
    delays.toList shouldBe List(10L, 20L)
  }

  test("materialize stages whose names are reserved SQL keywords") {
    val result = runFlow("""flow ReservedNameFlow = {
        |  stage primary = from [[1], [2]] as t(id)
        |  stage transform = from primary | select id
        |}""".stripMargin)
    result.isSuccess shouldBe true
    result.stageResults.map(_.state) shouldBe List(StageState.Success, StageState.Success)
  }

  test("merge stages with union-all semantics") {
    val result = runFlow("""flow MergeFlow = {
        |  stage source_a = from [[1], [2]] as t(id)
        |  stage source_b = from [[3]] as t(id)
        |  stage merged = merge source_a, source_b
        |  stage output = from merged | select id
        |}""".stripMargin)
    result.isSuccess shouldBe true
    countRows("merged") shouldBe 3L
    countRows("output") shouldBe 3L
  }

  test("skip a merge stage when one of its sources failed") {
    val result = runFlow("""flow BrokenMergeFlow = {
        |  stage source_a = from [[1]] as t(id)
        |  stage source_b = from nonexistent_table_xyz
        |  stage merged = merge source_a, source_b
        |}""".stripMargin)
    result.stageResult("merged").get.state shouldBe StageState.Skipped
  }

  test("fail a stage using unsupported flow operators without retrying") {
    val result = runFlow("""flow JourneyFlow = {
        |  stage entry = from [[1]] as t(id)
        |  stage delayed with { retries: 3 } = from entry | wait('7 days')
        |}""".stripMargin)
    val delayed = result.stageResult("delayed").get
    delayed.state shouldBe StageState.Failed
    // NOT_IMPLEMENTED errors are not retryable
    delayed.attempts shouldBe 1
  }

  test("cancel remaining stages when cancellation is requested") {
    val (flow, ctx) = compileFlow("""flow CancelFlow = {
        |  stage a = from [[1]] as t(id)
        |  stage b = from a | select *
        |}""".stripMargin)
    val executor = FlowExecutor(connector, workEnv)
    executor.cancel()
    val result = executor.execute(flow)(using ctx)
    result.stageResults.map(_.state) shouldBe List(StageState.Cancelled, StageState.Cancelled)
  }

  test("parse stage execution config from with block") {
    val (flow, _) = compileFlow("""flow ConfigFlow = {
        |  stage s with {
        |    retries: 3
        |    timeout: 5m
        |    retry_delay: 1s
        |    backoff: 'linear'
        |    max_retry_delay: 30s
        |  } = from [[1]] as t(id)
        |}""".stripMargin)
    val config = StageExecutionConfig.fromConfigItems(flow.stages.head.config)
    config.retries shouldBe 3
    config.timeoutMillis shouldBe Some(5 * 60 * 1000L)
    config.retryDelayMillis shouldBe 1000L
    config.backoff shouldBe "linear"
    config.maxRetryDelayMillis shouldBe Some(30000L)
  }

  test("execute a flow via the ExecuteFlow execution plan") {
    val (unit, flow, ctx) = compileFlowUnit("""flow WiredFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage doubled = from src | select id * 2 as id2
        |}""".stripMargin)
    val executionPlan = ExecutionPlanner.plan(unit, flow)(using ctx)
    val queryExecutor = QueryExecutor(dbConnectorProvider, profile, workEnv)
    val result        = queryExecutor.execute(executionPlan, ctx)
    result match
      case f: FlowExecutionResult =>
        f.flowName shouldBe "WiredFlow"
        f.stageResults.map(_.state) shouldBe List(StageState.Success, StageState.Success)
      case other =>
        fail(s"Expected FlowExecutionResult, but got: ${other}")
  }

  test("not execute flow definitions on whole-file execution") {
    val (unit, _, ctx) = compileFlowUnit("""flow DefinedOnlyFlow = {
        |  stage boom = from nonexistent_table_xyz
        |}""".stripMargin)
    val executionPlan = ExecutionPlanner.plan(unit, ctx)
    val queryExecutor = QueryExecutor(dbConnectorProvider, profile, workEnv)
    // The flow contains a failing stage, but merely defining it must not run it
    val result = queryExecutor.execute(executionPlan, ctx)
    result.hasError shouldBe false
  }

  test("compute retry delays for backoff strategies") {
    val base = StageExecutionConfig(retryDelayMillis = 100L)
    base.withBackoff("constant").retryDelayFor(1) shouldBe 100L
    base.withBackoff("constant").retryDelayFor(3) shouldBe 100L
    base.withBackoff("linear").retryDelayFor(1) shouldBe 100L
    base.withBackoff("linear").retryDelayFor(3) shouldBe 300L
    base.retryDelayFor(1) shouldBe 100L
    base.retryDelayFor(2) shouldBe 200L
    base.retryDelayFor(4) shouldBe 800L
    base.withMaxRetryDelayMillis(150L).retryDelayFor(4) shouldBe 150L
  }

end FlowExecutorTest

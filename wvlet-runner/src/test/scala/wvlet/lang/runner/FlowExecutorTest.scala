package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
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
import wvlet.lang.model.plan.StageDef
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.uni.test.UniTest

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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

  private def runFlow(
      wv: String,
      config: FlowExecutorConfig = FlowExecutorConfig(),
      stageRunner: Option[FlowStageRunner] = None,
      retryScheduler: Option[(Long, () => Unit) => Unit] = None,
      registry: Option[FlowRunStore] = None
  ): FlowExecutionResult =
    val (flow, ctx) = compileFlow(wv)
    FlowExecutor(connector, workEnv, config, stageRunner, retryScheduler, registry).execute(flow)(
      using ctx
    )

  /** Compile a unit possibly containing multiple flows and return them by name */
  private def compileFlows(wv: String): (Map[String, FlowDef], Context) =
    val compiler = Compiler(CompilerOptions(workEnv = workEnv))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    val ctx      = result.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    val flows    = Map.newBuilder[String, FlowDef]
    unit
      .resolvedPlan
      .traverse { case f: FlowDef =>
        flows += f.name.name -> f
      }
    (flows.result(), ctx)

  private def countRows(table: String): Long =
    connector.runQuery(s"""select count(*) cnt from "${table}"""") { rs =>
      rs.next()
      rs.getLong(1)
    }

  /** Count rows in the run-scoped materialization of the given stage */
  private def countStageRows(result: FlowExecutionResult, stage: String): Long = countRows(
    result
      .stageResult(stage)
      .flatMap(_.table)
      .getOrElse(fail(s"Stage ${stage} has no materialized table"))
  )

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
    // Each stage is materialized as a queryable run-scoped temp table
    result.stageResults.forall(_.table.exists(_.contains(result.runId.toLowerCase))) shouldBe true
    countStageRows(result, "filtered") shouldBe 2L
    countStageRows(result, "output") shouldBe 2L
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
      retryScheduler = Some { (delay, action) =>
        delays += delay
        action()
      }
    )
    val flaky = result.stageResult("flaky").get
    flaky.state shouldBe StageState.Failed
    flaky.attempts shouldBe 3
    // Default exponential backoff: 10ms, then 20ms
    delays.toList shouldBe List(10L, 20L)
  }

  test("run independent stages in parallel") {
    // Each stage blocks until both stages have started; this only completes if the
    // scheduler launches independent stages concurrently
    val bothStarted = CountDownLatch(2)
    val runner      =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit =
          bothStarted.countDown()
          if !bothStarted.await(10, TimeUnit.SECONDS) then
            throw IllegalStateException(s"Stage ${stage.name.name} did not run in parallel")
    val result = runFlow(
      """flow ParallelFlow = {
        |  stage a = from [[1]] as t(id)
        |  stage b = from [[2]] as t(id)
        |}""".stripMargin,
      stageRunner = Some(runner)
    )
    result.stageResults.map(_.state) shouldBe List(StageState.Success, StageState.Success)
  }

  test("run a diamond-shaped DAG respecting data dependencies") {
    val result = runFlow("""flow DiamondFlow = {
        |  stage root = from [[1], [2], [3]] as t(id)
        |  stage left = from root | where id <= 2
        |  stage right = from root | where id >= 2
        |  stage merged = merge left, right
        |}""".stripMargin)
    result.isSuccess shouldBe true
    result.stageResults.map(_.state).distinct shouldBe List(StageState.Success)
    // left (2 rows) union all right (2 rows)
    countStageRows(result, "merged") shouldBe 4L
  }

  test("cancel a flow while a stage is running") {
    val stageStarted = CountDownLatch(1)
    val runner       =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit =
          stageStarted.countDown()
          Thread.sleep(10000)
    val (flow, ctx) = compileFlow("""flow MidCancelFlow = {
        |  stage slow = from [[1]] as t(id)
        |  stage after = from slow | select *
        |}""".stripMargin)
    val executor  = FlowExecutor(connector, workEnv, stageRunner = Some(runner))
    val canceller = Thread { () =>
      stageStarted.await(10, TimeUnit.SECONDS)
      executor.cancel()
    }
    canceller.start()
    val result = executor.execute(flow)(using ctx)
    canceller.join()
    // The running stage and the pending downstream stage both end in cancelled
    result.stageResults.map(_.state) shouldBe List(StageState.Cancelled, StageState.Cancelled)
  }

  test("time out a slow stage attempt") {
    val runner =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit = Thread.sleep(
          10000
        )
    val result = runFlow(
      """flow SlowFlow = {
        |  stage slow with { timeout: 50ms } = from [[1]] as t(id)
        |}""".stripMargin,
      stageRunner = Some(runner)
    )
    val slow = result.stageResult("slow").get
    slow.state shouldBe StageState.Failed
    slow.error.get.getMessage shouldContain "timed out"
  }

  test("free the worker slot when a timed-out attempt is interrupted") {
    // The first stage blocks its worker indefinitely; with a single worker slot, the second
    // stage can only run if the timed-out attempt is interrupted to free the slot
    val blocked = CountDownLatch(1)
    val runner  =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit =
          if stage.name.name == "slow" then
            blocked.await()
    val result = runFlow(
      """flow TimeoutSlotFlow = {
        |  stage slow with { timeout: 100ms } = from [[1]] as t(id)
        |  stage fast = from [[2]] as t(id)
        |}""".stripMargin,
      config = FlowExecutorConfig(maxParallelism = 1),
      stageRunner = Some(runner)
    )
    val slow = result.stageResult("slow").get
    slow.state shouldBe StageState.Failed
    slow.error.get.getMessage shouldContain "timed out"
    result.stageResult("fast").get.state shouldBe StageState.Success
  }

  test("cancel a timed-out SQL statement server-side to free the worker slot") {
    // A cross join large enough to run for minutes if not cancelled server-side. With a
    // single worker slot, the second stage can only complete once the timed-out statement
    // is actually stopped in the database
    connector.execute(
      "create or replace table __wv_slow_cross_src as select range as id from range(1000000)"
    )
    try
      val result = runFlow(
        """flow SqlCancelFlow = {
          |  stage slow with { timeout: 500ms } = from __wv_slow_cross_src as a cross join __wv_slow_cross_src as b | select max(a.id * b.id) as m
          |  stage fast = from [[1]] as t(id)
          |}""".stripMargin,
        config = FlowExecutorConfig(maxParallelism = 1)
      )
      val slow = result.stageResult("slow").get
      slow.state shouldBe StageState.Failed
      slow.error.get.getMessage shouldContain "timed out"
      result.stageResult("fast").get.state shouldBe StageState.Success
    finally
      connector.execute("drop table if exists __wv_slow_cross_src")
  }

  test("retry timed-out attempts until retries are exhausted") {
    val delays = ListBuffer.empty[Long]
    val runner =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit = Thread.sleep(
          10000
        )
    val result = runFlow(
      """flow SlowRetryFlow = {
        |  stage slow with {
        |    timeout: 20ms
        |    retries: 1
        |    retry_delay: 1ms
        |  } = from [[1]] as t(id)
        |}""".stripMargin,
      stageRunner = Some(runner),
      retryScheduler = Some { (delay, action) =>
        delays += delay
        action()
      }
    )
    val slow = result.stageResult("slow").get
    slow.state shouldBe StageState.Failed
    slow.attempts shouldBe 2
    delays.toList shouldBe List(1L)
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
    countStageRows(result, "merged") shouldBe 3L
    countStageRows(result, "output") shouldBe 3L
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
    val result = runFlow("""flow JumpFlow = {
        |  stage entry = from [[1]] as t(id)
        |  stage jump with { retries: 3 } = from entry | -> OtherFlow
        |}""".stripMargin)
    val jump = result.stageResult("jump").get
    jump.state shouldBe StageState.Failed
    // NOT_IMPLEMENTED errors are not retryable
    jump.attempts shouldBe 1
  }

  test("route rows to target stages with conditional predicates") {
    val result = runFlow("""flow RouteFlow = {
        |  stage src = from [[1, 25], [2, 15], [3, 40]] as t(id, age)
        |  stage gate = from src | route {
        |    case _.age >= 18 -> adult
        |    else -> minor
        |  }
        |  stage adult = from gate | select id
        |  stage minor = from gate | select id
        |}""".stripMargin)
    result.isSuccess shouldBe true
    // The route stage materializes its full input; targets receive the routed subsets
    countStageRows(result, "gate") shouldBe 3L
    countStageRows(result, "adult") shouldBe 2L
    countStageRows(result, "minor") shouldBe 1L
  }

  test("route rows deterministically with percentage buckets") {
    val values = (1 to 100).map(i => s"[${i}]").mkString(", ")
    val flowWv =
      s"""flow ABTestFlow = {
         |  stage src = from [${values}] as t(id)
         |  stage split = from src | route by hash(id) {
         |    case 50 -> variant_a
         |    case 50 -> variant_b
         |  }
         |  stage variant_a = from split | select id
         |  stage variant_b = from split | select id
         |}""".stripMargin
    val result1 = runFlow(flowWv)
    result1.isSuccess shouldBe true
    val a1 = countStageRows(result1, "variant_a")
    val b1 = countStageRows(result1, "variant_b")
    // Buckets partition the input: every row lands in exactly one variant
    a1 + b1 shouldBe 100L
    // Deterministic partitioning: a re-run produces the identical split
    val result2 = runFlow(flowWv)
    countStageRows(result2, "variant_a") shouldBe a1
    countStageRows(result2, "variant_b") shouldBe b1
  }

  test("flatten fork stages and run them against the shared input") {
    val result = runFlow("""flow ForkFlow = {
        |  stage entry = from [[1], [2]] as t(id)
        |  stage parallel = from entry | fork {
        |    stage email = from entry | select id
        |    stage sms = from entry | select id
        |  }
        |}""".stripMargin)
    result.isSuccess shouldBe true
    result.stageResults.map(_.name) shouldBe List("entry", "parallel", "email", "sms")
    countStageRows(result, "email") shouldBe 2L
    countStageRows(result, "sms") shouldBe 2L
  }

  test("delay a stage with the wait operator") {
    val start  = System.nanoTime()
    val result = runFlow("""flow WaitFlow = {
        |  stage src = from [[1]] as t(id)
        |  stage delayed = from src | wait('100 ms')
        |}""".stripMargin)
    val elapsedMillis = (System.nanoTime() - start) / 1000000L
    result.isSuccess shouldBe true
    elapsedMillis >= 100L shouldBe true
    countStageRows(result, "delayed") shouldBe 1L
  }

  test("activate materializes its input and succeeds as a local stub") {
    val result = runFlow("""flow ActivateFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage send = from src | activate('email')
        |  stage done = from send | end()
        |}""".stripMargin)
    result.isSuccess shouldBe true
    countStageRows(result, "send") shouldBe 2L
    countStageRows(result, "done") shouldBe 2L
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

  test("execute a flow with the run flow statement and query its summary") {
    val (unit, _, ctx) = compileFlowUnit("""flow StmtFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage doubled = from src | select id * 2 as id2
        |}
        |
        |run flow StmtFlow
        |""".stripMargin)
    val executionPlan = ExecutionPlanner.plan(unit, ctx)
    val queryExecutor = QueryExecutor(dbConnectorProvider, profile, workEnv)
    val result        = queryExecutor.execute(executionPlan, ctx)
    result match
      case t: TableRows =>
        t.rows.size shouldBe 2
        t.rows.map(_("stage")) shouldBe Seq("src", "doubled")
        t.rows.map(_("state")) shouldBe Seq("success", "success")
      case other =>
        fail(s"Expected TableRows summary, but got: ${other}")
  }

  test("report an error when running an undefined flow") {
    val e = intercept[WvletLangException] {
      compileFlowUnit("""flow DefinedFlow = {
          |  stage src = from [[1]] as t(id)
          |}
          |
          |run flow NoSuchFlow
          |""".stripMargin)
    }
    e.statusCode shouldBe StatusCode.FLOW_NOT_FOUND
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

  test("record flow runs in the registry") {
    val registry = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    val result   = runFlow(
      """flow RecordedFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage out = from src | select id
        |}""".stripMargin,
      registry = Some(registry)
    )
    val record = registry.get(result.runId).get
    record.flowName shouldBe "RecordedFlow"
    record.state shouldBe FlowRunRecord.STATE_SUCCESS
    record.finishedAtMillis.isDefined shouldBe true
    record.stages.map(_.name) shouldBe List("src", "out")
    record.stages.map(_.state).distinct shouldBe List("success")
    registry.latestRunOf("RecordedFlow").get.runId shouldBe result.runId
  }

  test("record failed flow runs with stage errors") {
    val registry = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    val result   = runFlow(
      """flow BrokenRecordedFlow = {
        |  stage broken = from nonexistent_table_xyz
        |}""".stripMargin,
      registry = Some(registry)
    )
    val record = registry.get(result.runId).get
    record.state shouldBe FlowRunRecord.STATE_FAILED
    record.stages.head.state shouldBe "failed"
    record.stages.head.error.isDefined shouldBe true
  }

  test("evaluate cross-flow dependencies against the run registry") {
    val registry     = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    val (flows, ctx) = compileFlows("""flow Upstream = {
        |  stage src = from [[1]] as t(id)
        |}
        |
        |flow Downstream depends on Upstream = {
        |  stage report = from [[1]] as t(id)
        |}
        |
        |flow Recovery if Upstream.failed = {
        |  stage alert = from [[1]] as t(id)
        |}
        |
        |flow Cleanup if Upstream.done = {
        |  stage archive = from [[1]] as t(id)
        |}
        |
        |flow Orphan depends on NeverRunFlow = {
        |  stage x = from [[1]] as t(id)
        |}
        |""".stripMargin)

    def run(name: String): FlowExecutionResult =
      FlowExecutor(connector, workEnv, registry = Some(registry)).execute(flows(name))(using ctx)

    // Before any upstream run, dependent flows are skipped entirely
    run("Downstream").stageResults.map(_.state) shouldBe List(StageState.Skipped)

    run("Upstream").isSuccess shouldBe true

    // depends on: runs only after the upstream flow succeeded
    run("Downstream").stageResults.map(_.state) shouldBe List(StageState.Success)
    // if Upstream.failed: upstream succeeded, so the recovery flow is skipped
    run("Recovery").stageResults.map(_.state) shouldBe List(StageState.Skipped)
    // if Upstream.done: upstream reached a terminal state, so cleanup runs
    run("Cleanup").stageResults.map(_.state) shouldBe List(StageState.Success)
    // depends on a flow that never ran
    run("Orphan").stageResults.map(_.state) shouldBe List(StageState.Skipped)

    // Dependency-skipped runs are recorded with the skipped state
    registry.latestRunOf("Orphan").get.state shouldBe FlowRunRecord.STATE_SKIPPED
  }

  test("resume a failed run reusing successful stage materializations") {
    val registry = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    val wv       =
      """flow ResumableFlow = {
        |  stage src = from [[1], [2]] as t(id)
        |  stage out = from src cross join __wv_resume_dep as d | select id
        |}""".stripMargin
    connector.execute("drop table if exists __wv_resume_dep")
    val first = runFlow(wv, registry = Some(registry))
    first.isSuccess shouldBe false
    first.stageResult("src").get.state shouldBe StageState.Success
    first.stageResult("out").get.state shouldBe StageState.Failed

    try
      // Create the dependency that was missing in the first run, and tag the recorded src
      // materialization so that re-execution of src would be detectable
      connector.execute("create or replace table __wv_resume_dep as select 10 as x")
      val srcTable = first.stageResult("src").get.table.get
      connector.execute(s"""insert into "${srcTable}" values (99)""")

      val record = registry.get(first.runId).get
      record.state shouldBe FlowRunRecord.STATE_FAILED
      val (flow, ctx) = compileFlow(wv)
      val resumed     =
        FlowExecutor(connector, workEnv, registry = Some(registry)).execute(
          flow,
          resumeFrom = Some(record)
        )(using ctx)
      resumed.runId shouldBe first.runId
      resumed.isSuccess shouldBe true
      resumed.stageResult("out").get.state shouldBe StageState.Success
      // out read the src table recorded in the first run (2 rows + 1 tagged row), proving
      // that the successful stage was reused instead of re-executed
      countStageRows(resumed, "out") shouldBe 3L
      registry.get(first.runId).get.state shouldBe FlowRunRecord.STATE_SUCCESS
    finally
      connector.execute("drop table if exists __wv_resume_dep")
  }

  test("cancel a run across processes via the registry cancel marker") {
    val registry     = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    val stageStarted = CountDownLatch(1)
    val runner       =
      new FlowStageRunner:
        override def run(stage: StageDef, targetTable: String)(using Context): Unit =
          stageStarted.countDown()
          Thread.sleep(30000)
    val (flow, ctx) = compileFlow("""flow MarkerCancelFlow = {
        |  stage slow = from [[1]] as t(id)
        |}""".stripMargin)
    val executor = FlowExecutor(
      connector,
      workEnv,
      config = FlowExecutorConfig(cancelPollIntervalMillis = 10L),
      stageRunner = Some(runner),
      registry = Some(registry)
    )
    // Simulate another process: locate the running record via the registry and place the
    // cancellation marker
    val canceller = Thread { () =>
      stageStarted.await(10, TimeUnit.SECONDS)
      var runId: Option[String] = None
      while runId.isEmpty do
        runId = registry.list().headOption.map(_.runId)
        if runId.isEmpty then
          Thread.sleep(10)
      registry.requestCancel(runId.get)
    }
    canceller.start()
    val result = executor.execute(flow)(using ctx)
    canceller.join()
    result.stageResults.map(_.state) shouldBe List(StageState.Cancelled)
    registry.get(result.runId).get.state shouldBe FlowRunRecord.STATE_CANCELLED
    // The marker is cleared once the run reaches a terminal state
    registry.cancelRequested(result.runId) shouldBe false
  }

  test("record and cancel flow runs with the SQLite-backed store") {
    val store = SQLiteFlowRunStore(
      java.nio.file.Files.createTempDirectory("wv-flow-sqlite").resolve("registry.db")
    )
    try
      val result = runFlow(
        """flow SqliteRecordedFlow = {
          |  stage src = from [[1], [2]] as t(id)
          |  stage out = from src | select id
          |}""".stripMargin,
        registry = Some(store)
      )
      result.isSuccess shouldBe true
      val record = store.get(result.runId).get
      record.flowName shouldBe "SqliteRecordedFlow"
      record.state shouldBe FlowRunRecord.STATE_SUCCESS
      record.stages.map(_.state).distinct shouldBe List("success")
      store.latestRunOf("SqliteRecordedFlow").get.runId shouldBe result.runId
    finally
      store.close()
  }

  test("manage cancellation markers and record deletion in the registry") {
    val registry = FlowRunRegistry(java.nio.file.Files.createTempDirectory("wv-flow-reg"))
    registry.save(FlowRunRecord("run1", "F", FlowRunRecord.STATE_RUNNING, 100L))
    registry.cancelRequested("run1") shouldBe false
    registry.requestCancel("run1")
    registry.cancelRequested("run1") shouldBe true
    registry.clearCancelRequest("run1")
    registry.cancelRequested("run1") shouldBe false
    registry.requestCancel("run1")
    registry.delete("run1")
    registry.get("run1") shouldBe None
    registry.cancelRequested("run1") shouldBe false
    registry.list() shouldBe Nil
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

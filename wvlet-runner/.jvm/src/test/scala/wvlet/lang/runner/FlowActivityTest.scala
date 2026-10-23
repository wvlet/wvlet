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
import wvlet.lang.runner.activity.ActivityExecutor
import wvlet.lang.runner.activity.ActivityJson
import wvlet.lang.runner.activity.ActivityRequest
import wvlet.lang.runner.activity.ActivityResult
import wvlet.lang.runner.activity.CancelAck
import wvlet.lang.runner.activity.InMemoryItemStore
import wvlet.lang.runner.activity.SQLiteItemStore
import wvlet.lang.runner.activity.TaskQueue
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/**
  * Tests for `activity` and `for each ... activity` flow stages executed through FlowExecutor
  */
class FlowActivityTest extends UniTest:
  private val workEnv             = WorkEnv(".")
  private val dbConnectorProvider = ConnectorProvider(workEnv)
  private val connector           = dbConnectorProvider.getConnector(Profile.defaultDuckDBProfile)

  override def afterAll: Unit = dbConnectorProvider.close()

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

  private def runFlow(
      wv: String,
      activityExecutors: List[ActivityExecutor] = Nil,
      taskQueue: Option[TaskQueue] = None
  ): FlowExecutionResult =
    val (flow, ctx) = compileFlow(wv)
    FlowExecutor(connector, workEnv, activityExecutors = activityExecutors, taskQueue = taskQueue)
      .execute(flow, resumeFrom = None)(using ctx)

  /** Rows of a stage table as column -> value maps */
  private def stageRows(result: FlowExecutionResult, stage: String): List[Map[String, String]] =
    val table = result
      .stageResult(stage)
      .flatMap(_.table)
      .getOrElse(fail(s"Stage ${stage} has no table"))
    connector
      .queryJsonRows(s"""select * from "${table}" order by 1""")
      .map(row => ActivityJson.decode(row))

  /** Echoes the item back, failing for the configured keys */
  class EchoExecutor(failKeys: Set[String] = Set.empty) extends ActivityExecutor:
    val requests              = ConcurrentHashMap.newKeySet[String]()
    override def name: String = "test.echo"
    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val item = request.input("item").toString
      requests.add(item)
      if failKeys.contains(item) then
        ActivityResult.Failed(s"cannot process ${item}")
      else
        ActivityResult.Completed(Map("echo" -> item, "stage" -> request.input("stage")))

    override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

  test("invoke an activity per distinct for-each key and materialize item results") {
    val executor = EchoExecutor()
    val result   = runFlow(
      """flow Investigate = {
        |  stage customers = from [[1, 'a'], [2, 'b'], [2, 'b'], [3, 'c']] as t(id, name)
        |  stage investigate = from customers | for each _.name activity 'test.echo'
        |  stage done = from investigate | where state = 'succeeded'
        |}""".stripMargin,
      activityExecutors = List(executor)
    )
    result.isSuccess shouldBe true
    executor.requests.asScala.toSet shouldBe Set("a", "b", "c")

    val rows = stageRows(result, "investigate")
    rows.map(_("item_key")) shouldBe List("a", "b", "c")
    rows.map(_("state")).distinct shouldBe List("succeeded")
    rows.map(_("attempts")).distinct shouldBe List("1")
    val output = ActivityJson.decode(rows.head("output"))
    output("echo") shouldBe "a"
    output("stage") shouldBe "investigate"

    // Downstream stages read the item results like any stage output
    stageRows(result, "done").size shouldBe 3
  }

  test("fail the stage when an item fails, keeping the per-item results") {
    val result = runFlow(
      """flow PartialFailure = {
        |  stage targets = from [[1], [2], [3]] as t(id)
        |  stage work = from targets | for each _.id activity 'test.echo'
        |  stage after = from work
        |}""".stripMargin,
      activityExecutors = List(EchoExecutor(failKeys = Set("2")))
    )
    result.isSuccess shouldBe false
    result.stageResult("work").get.state shouldBe StageState.Failed
    result.stageResult("work").get.error.map(_.getMessage).getOrElse("") shouldContain "1 failed"
    result.stageResult("after").get.state shouldBe StageState.Skipped
  }

  test("run a bare activity once for the stage") {
    val executor = EchoExecutor()
    val result   = runFlow(
      """flow Single = {
        |  stage data = select 1 as x
        |  stage process = from data | activity 'test.echo'
        |}""".stripMargin,
      activityExecutors = List(executor)
    )
    result.isSuccess shouldBe true
    executor.requests.asScala.toList shouldBe List("process")
    stageRows(result, "process").map(_("state")) shouldBe List("succeeded")
  }

  test("fail an activity stage whose type has no executor") {
    val result = runFlow("""flow Unknown = {
        |  stage data = select 1 as x
        |  stage process = from data | for each _.x activity 'no.such.type'
        |}""".stripMargin)
    result.isSuccess shouldBe false
    result.stageResult("process").get.error.map(_.getMessage).getOrElse("") shouldContain
      "No executor"
  }

  /** Fails the first attempt of each item, then completes it */
  class FlakyExecutor extends ActivityExecutor:
    val attempts              = ConcurrentHashMap[String, AtomicInteger]()
    val calls                 = AtomicInteger(0)
    override def name: String = "test.flaky"

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val item = request.input("item").toString
      calls.incrementAndGet()
      if attempts.computeIfAbsent(item, _ => AtomicInteger(0)).incrementAndGet() == 1 then
        ActivityResult.Failed(s"first try of ${item} failed")
      else
        ActivityResult.Completed(Map("echo" -> item))

    override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

  /**
    * Cancels the items in `cancelKeys` on its own (without a run cancel), fails the first
    * `failures(item)` attempts of an item, and completes the rest
    */
  class SelfCancellingExecutor(
      cancelKeys: Set[String] = Set.empty,
      failures: Map[String, Int] = Map.empty
  ) extends ActivityExecutor:
    val calls                 = ConcurrentHashMap[String, AtomicInteger]()
    override def name: String = "test.self_cancel"

    def callsOf(item: String): Int = Option(calls.get(item)).map(_.get()).getOrElse(0)

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val item = request.input("item").toString
      val n    = calls.computeIfAbsent(item, _ => AtomicInteger(0)).incrementAndGet()
      if cancelKeys.contains(item) then
        ActivityResult.Cancelled
      else if n <= failures.getOrElse(item, 0) then
        ActivityResult.Failed(s"first try of ${item} failed")
      else
        ActivityResult.Completed(Map("echo" -> item))

    override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

  /** Holds its item until the run cancels it, and confirms the cancellation */
  class ConfirmingGateExecutor extends ActivityExecutor:
    val started               = CountDownLatch(1)
    val gate                  = CountDownLatch(1)
    override def name: String = "test.self_cancel"

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      started.countDown()
      gate.await(10, TimeUnit.SECONDS)
      ActivityResult.Cancelled

    override def cancel(attemptId: String): CancelAck =
      gate.countDown()
      CancelAck.Acknowledged

  /** Holds its item until the test releases it, and does not confirm the cancellation */
  class GateExecutor extends ActivityExecutor:
    val started               = CountDownLatch(1)
    val cancelled             = CountDownLatch(1)
    val gate                  = CountDownLatch(1)
    override def name: String = "test.gate"

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      started.countDown()
      gate.await(10, TimeUnit.SECONDS)
      ActivityResult.Completed(Map("echo" -> request.input("item").toString))

    override def cancel(attemptId: String): CancelAck =
      cancelled.countDown()
      CancelAck.Unknown

  test("re-run failed items of an activity stage when retry_failed is set") {
    val executor = FlakyExecutor()
    val result   = runFlow(
      """flow RetryFailed = {
        |  stage targets = from [[1], [2]] as t(id)
        |  stage work with { retries: 1, retry_delay: 1ms, retry_failed: true } = from targets | for each _.id activity 'test.flaky'
        |  stage after = from work
        |}""".stripMargin,
      activityExecutors = List(executor)
    )
    result.isSuccess shouldBe true
    executor.calls.get() shouldBe 4
    result.stageResult("work").get.attempts shouldBe 2
  }

  test("keep failed items of an activity stage when retry_failed is not set") {
    val executor = FlakyExecutor()
    val result   = runFlow(
      """flow KeepFailed = {
        |  stage targets = from [[1], [2]] as t(id)
        |  stage work with { retries: 1, retry_delay: 1ms } = from targets | for each _.id activity 'test.flaky'
        |}""".stripMargin,
      activityExecutors = List(executor)
    )
    result.isSuccess shouldBe false
    executor.calls.get() shouldBe 2
    result.stageResult("work").get.attempts shouldBe 2
  }

  test("keep executor-cancelled items of an activity stage on a stage retry") {
    val executor = SelfCancellingExecutor(cancelKeys = Set("1"), failures = Map("2" -> 1))
    val result   = runFlow(
      """flow KeepCancelled = {
        |  stage targets = from [[1], [2]] as t(id)
        |  stage work with { retries: 1, retry_delay: 1ms, retry_failed: true } = from targets | for each _.id activity 'test.self_cancel'
        |}""".stripMargin,
      activityExecutors = List(executor)
    )
    result.stageResult("work").get.attempts shouldBe 2
    // The retry re-ran the failed item only; the cancelled item needs a resume to run again
    executor.callsOf("2") shouldBe 2
    executor.callsOf("1") shouldBe 1
  }

  test("end a cancelled activity stage once its batch drains its items") {
    val executor    = GateExecutor()
    val (flow, ctx) = compileFlow("""flow CancelDrain = {
        |  stage targets = from [[1]] as t(id)
        |  stage work = from targets | for each _.id activity 'test.gate'
        |}""".stripMargin)
    val flowExecutor = FlowExecutor(connector, workEnv, activityExecutors = List(executor))
    val pool         = Executors.newSingleThreadExecutor()
    try
      val future = pool.submit(
        new Callable[FlowExecutionResult]:
          override def call(): FlowExecutionResult =
            flowExecutor.execute(flow, resumeFrom = None)(using ctx)
      )
      executor.started.await(5, TimeUnit.SECONDS) shouldBe true

      flowExecutor.cancel()
      executor.cancelled.await(5, TimeUnit.SECONDS) shouldBe true
      executor.gate.countDown()

      val result = future.get(10, TimeUnit.SECONDS)
      result.stageResult("work").get.state shouldBe StageState.Cancelled
    finally
      executor.gate.countDown()
      pool.shutdownNow()
  }

  /**
    * Runs `body` while a worker polls the queue for `activityType` like a remote process would
    * through TaskApi, completing each task. Returns the item keys the worker received
    */
  private def withQueueWorker[A](queue: TaskQueue, activityType: String)(
      body: => A
  ): (A, Set[String]) =
    val stop    = AtomicBoolean(false)
    val workers = ConcurrentHashMap.newKeySet[String]()
    val worker  = Thread { () =>
      while !stop.get() do
        queue
          .poll(List(activityType), "worker-1", 100)
          .foreach { task =>
            workers.add(task.input("item").toString)
            queue.heartbeat(task.attemptId)
            queue.reportResult(
              task.attemptId,
              "completed",
              Some(ActivityJson.encode(Map("session" -> s"session-${task.input("item")}"))),
              None
            )
          }
    }
    worker.setDaemon(true)
    worker.start()
    try
      val result = body
      (result, workers.asScala.toSet)
    finally
      stop.set(true)
      worker.join(5000)

  test("dispatch activity items to remote workers through the task queue") {
    val queue             = TaskQueue(InMemoryItemStore())
    val (result, workers) =
      withQueueWorker(queue, "agent.investigate") {
        runFlow(
          """flow RemoteBatch = {
          |  stage customers = from [[10], [20], [30]] as t(customer_id)
          |  stage investigate with { concurrency: 2 } =
          |    from customers | for each _.customer_id activity 'agent.investigate'
          |}""".stripMargin,
          taskQueue = Some(queue)
        )
      }
    result.isSuccess shouldBe true
    workers shouldBe Set("10", "20", "30")
    val rows = stageRows(result, "investigate")
    rows.map(_("state")).distinct shouldBe List("succeeded")
    rows.map(r => ActivityJson.decode(r("output"))("session")) shouldBe
      List("session-10", "session-20", "session-30")
    // The queue's store records the claiming worker for inspection
    queue.store.listItems(result.runId, "investigate").flatMap(_.workerId).distinct shouldBe
      List("worker-1")
  }

  test("dispatch activity items of a run flow statement through the query executor's queue") {
    val queue = TaskQueue(InMemoryItemStore())
    val unit  = CompilationUnit.fromWvletString("""flow StmtRemote = {
        |  stage customers = from [[1], [2]] as t(customer_id)
        |  stage investigate = from customers | for each _.customer_id activity 'agent.stmt'
        |}
        |
        |run flow StmtRemote
        |""".stripMargin)
    val compiled      = Compiler(CompilerOptions(workEnv = workEnv)).compileSingleUnit(unit)
    val ctx           = compiled.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    val queryExecutor = QueryExecutor(
      dbConnectorProvider,
      Profile.defaultDuckDBProfile,
      workEnv,
      QueryExecutorConfig().withTaskQueue(queue)
    )
    val (result, workers) =
      withQueueWorker(queue, "agent.stmt") {
        queryExecutor.execute(ExecutionPlanner.plan(unit, ctx), ctx)
      }
    result match
      case t: TableRows =>
        t.rows.map(_("stage")) shouldBe Seq("customers", "investigate")
        t.rows.map(_("state")) shouldBe Seq("success", "success")
      case other =>
        fail(s"Expected TableRows summary, but got: ${other}")
    workers shouldBe Set("1", "2")
  }

  test("reuse succeeded items from a durable item store when a run is resumed") {
    val dir         = Files.createTempDirectory("wv-activity-resume")
    val registry    = FlowRunRegistry(dir.resolve("runs"))
    val itemsDb     = dir.resolve("items.db")
    val (flow, ctx) = compileFlow("""flow DurableResume = {
        |  stage targets = from [[1], [2]] as t(id)
        |  stage work with { retry_failed: true } = from targets | for each _.id activity 'test.echo'
        |}""".stripMargin)

    // Each run opens its own store on the same file, as separate CLI processes do
    def runWith(executor: EchoExecutor, resumeFrom: Option[FlowRunRecord]): FlowExecutionResult =
      val store = SQLiteItemStore(itemsDb)
      try
        FlowExecutor(
          connector,
          workEnv,
          registry = Some(registry),
          activityExecutors = List(executor),
          itemStore = Some(store)
        ).execute(flow, resumeFrom = resumeFrom)(using ctx)
      finally store.close()

    val first = runWith(EchoExecutor(failKeys = Set("2")), None)
    first.isSuccess shouldBe false

    val resumedExecutor = EchoExecutor()
    val resumed         = runWith(resumedExecutor, registry.get(first.runId))
    resumed.runId shouldBe first.runId
    resumed.isSuccess shouldBe true
    // Item 1 succeeded in the first run and was read back from the store, not executed again
    resumedExecutor.requests.asScala.toSet shouldBe Set("2")
    stageRows(resumed, "work").map(r => r("item_key") -> r("attempts")) shouldBe
      List("1" -> "1", "2" -> "2")
  }

  test("re-run cancelled items of an activity stage when a run is resumed") {
    val dir         = Files.createTempDirectory("wv-activity-resume-cancelled")
    val registry    = FlowRunRegistry(dir.resolve("runs"))
    val itemsDb     = dir.resolve("items.db")
    val (flow, ctx) = compileFlow("""flow ResumeCancelled = {
        |  stage targets = from [[1], [2]] as t(id)
        |  stage work with { retries: 1, retry_delay: 1ms } = from targets | for each _.id activity 'test.self_cancel'
        |}""".stripMargin)

    def runWith(
        executor: SelfCancellingExecutor,
        resumeFrom: Option[FlowRunRecord]
    ): FlowExecutionResult =
      val store = SQLiteItemStore(itemsDb)
      try
        FlowExecutor(
          connector,
          workEnv,
          registry = Some(registry),
          activityExecutors = List(executor),
          itemStore = Some(store)
        ).execute(flow, resumeFrom = resumeFrom)(using ctx)
      finally store.close()

    // Item 1 cancels itself and item 2 keeps failing, so the run fails after its stage retry
    val first = SelfCancellingExecutor(cancelKeys = Set("1"), failures = Map("2" -> Int.MaxValue))
    val firstRun = runWith(first, None)
    firstRun.isSuccess shouldBe false
    first.callsOf("1") shouldBe 1

    // The resume re-admits the cancelled item; the failed item stays failed without retry_failed
    val resumedExecutor = SelfCancellingExecutor()
    val resumed         = runWith(resumedExecutor, registry.get(firstRun.runId))
    resumedExecutor.callsOf("1") shouldBe 1
    resumedExecutor.callsOf("2") shouldBe 0
    resumed.isSuccess shouldBe false
  }

  test("re-run items of a cancelled run when it is resumed") {
    val dir         = Files.createTempDirectory("wv-activity-resume-cancelled-run")
    val registry    = FlowRunRegistry(dir.resolve("runs"))
    val itemsDb     = dir.resolve("items.db")
    val (flow, ctx) = compileFlow("""flow ResumeCancelledRun = {
        |  stage targets = from [[1]] as t(id)
        |  stage work = from targets | for each _.id activity 'test.self_cancel'
        |}""".stripMargin)

    def flowExecutorWith(executor: ActivityExecutor, store: SQLiteItemStore): FlowExecutor =
      FlowExecutor(
        connector,
        workEnv,
        registry = Some(registry),
        activityExecutors = List(executor),
        itemStore = Some(store)
      )

    val gate         = ConfirmingGateExecutor()
    val store        = SQLiteItemStore(itemsDb)
    val pool         = Executors.newSingleThreadExecutor()
    val cancelledRun =
      try
        val flowExecutor = flowExecutorWith(gate, store)
        val future       = pool.submit(
          new Callable[FlowExecutionResult]:
            override def call(): FlowExecutionResult =
              flowExecutor.execute(flow, resumeFrom = None)(using ctx)
        )
        gate.started.await(5, TimeUnit.SECONDS) shouldBe true
        flowExecutor.cancel()
        future.get(10, TimeUnit.SECONDS)
      finally
        gate.gate.countDown()
        pool.shutdownNow()
        store.close()
    cancelledRun.stageResult("work").get.state shouldBe StageState.Cancelled

    val resumedExecutor = SelfCancellingExecutor()
    val resumedStore    = SQLiteItemStore(itemsDb)
    val resumed         =
      try
        flowExecutorWith(resumedExecutor, resumedStore).execute(
          flow,
          resumeFrom = registry.get(cancelledRun.runId)
        )(using ctx)
      finally resumedStore.close()
    resumedExecutor.callsOf("1") shouldBe 1
    resumed.isSuccess shouldBe true
  }

end FlowActivityTest

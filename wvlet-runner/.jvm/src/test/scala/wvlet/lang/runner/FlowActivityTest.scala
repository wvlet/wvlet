package wvlet.lang.runner

import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.runner.activity.ActivityExecutor
import wvlet.lang.runner.activity.ActivityJson
import wvlet.lang.runner.activity.ActivityRequest
import wvlet.lang.runner.activity.ActivityResult
import wvlet.lang.runner.activity.CancelAck
import wvlet.lang.runner.activity.InMemoryItemStore
import wvlet.lang.runner.activity.TaskQueue
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
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

  test("dispatch activity items to remote workers through the task queue") {
    val queue   = TaskQueue(InMemoryItemStore())
    val stop    = AtomicBoolean(false)
    val workers = ConcurrentHashMap.newKeySet[String]()
    // A worker polling the queue like a remote process would through TaskApi
    val worker = Thread { () =>
      while !stop.get() do
        queue
          .poll(List("agent.investigate"), "worker-1", 100)
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
      val result = runFlow(
        """flow RemoteBatch = {
          |  stage customers = from [[10], [20], [30]] as t(customer_id)
          |  stage investigate with { concurrency: 2 } =
          |    from customers | for each _.customer_id activity 'agent.investigate'
          |}""".stripMargin,
        taskQueue = Some(queue)
      )
      result.isSuccess shouldBe true
      workers.asScala.toSet shouldBe Set("10", "20", "30")
      val rows = stageRows(result, "investigate")
      rows.map(_("state")).distinct shouldBe List("succeeded")
      rows.map(r => ActivityJson.decode(r("output"))("session")) shouldBe
        List("session-10", "session-20", "session-30")
      // The queue's store records the claiming worker for inspection
      queue.store.listItems(result.runId, "investigate").flatMap(_.workerId).distinct shouldBe
        List("worker-1")
    finally
      stop.set(true)
      worker.join(5000)
  }

end FlowActivityTest

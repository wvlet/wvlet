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
package wvlet.lang.flow

import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.FlowDef
import wvlet.uni.test.UniTest

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
  * Tests for the portable flow scheduler using an in-memory stub port
  */
class FlowSchedulerTest extends UniTest:
  private val workEnv = WorkEnv(".")

  private def compileFlow(wv: String): FlowDef =
    val compiler = Compiler(CompilerOptions(workEnv = workEnv))
    val unit     = CompilationUnit.fromWvletString(wv)
    compiler.compileSingleUnit(unit)
    var flow: Option[FlowDef] = None
    unit
      .resolvedPlan
      .traverse { case f: FlowDef =>
        if flow.isEmpty then
          flow = Some(f)
      }
    flow.getOrElse(fail("No FlowDef found in the compiled plan"))

  /**
    * In-memory FlowEnginePort that records every port call and can be told to fail specific stages
    * a given number of times
    */
  private class StubPort extends FlowEnginePort:
    val executed        = ListBuffer.empty[String]
    val agentCalls      = ListBuffer.empty[(String, AgentInvocation)]
    val busRows         = ListBuffer.empty[BusRow]
    val sleeps          = ListBuffer.empty[Long]
    val persistedStates = ListBuffer.empty[RunState]

    private val failRemaining = mutable.Map.empty[String, Int]

    def failStage(stage: String, times: Int = Int.MaxValue): Unit = failRemaining(stage) = times

    private def maybeFail(stage: String): Option[Future[Nothing]] =
      failRemaining.get(stage) match
        case Some(n) if n > 0 =>
          failRemaining(stage) = n - 1
          Some(Future.failed(IllegalStateException(s"stage ${stage} failed (stub)")))
        case _ =>
          None

    // The scheduler embeds the stage name in the placeholder SQL it dispatches
    private def stageNameIn(sql: String): String = sql.split("stage ").last.takeWhile(_ != ':')

    override def runSql(sql: String): Future[Seq[Row]] =
      val stage = stageNameIn(sql)
      executed += stage
      maybeFail(stage).getOrElse(Future.successful(Seq(Row(Map("result" -> 1)))))

    override def invokeAgent(stage: String, spec: AgentInvocation): Future[AgentResult] =
      executed += stage
      agentCalls += stage -> spec
      maybeFail(stage).getOrElse(Future.successful(AgentResult(s"agent ${stage} ok")))

    override def appendBus(row: BusRow): Future[Unit] =
      busRows += row
      Future.unit

    override def sleep(millis: Long): Future[Unit] =
      sleeps += millis
      Future.unit

    override def persistRunState(state: RunState): Future[Unit] =
      persistedStates += state
      Future.unit

  end StubPort

  private def runFlow(
      wv: String,
      port: StubPort,
      config: FlowSchedulerConfig = FlowSchedulerConfig()
  ): FlowRunResult = Await.result(
    FlowScheduler(port, config).run(compileFlow(wv), runId = "test-run"),
    30.seconds
  )

  test("run a linear flow in dependency order") {
    val port   = StubPort()
    val result = runFlow(
      """flow SimpleFlow = {
        |  stage src = from [[1, 'a'], [2, 'b']] as t(id, name)
        |  stage filtered = from src | where name = 'a'
        |  stage output = from filtered | select id
        |}""".stripMargin,
      port
    )
    result.isSuccess shouldBe true
    result.stageStates.values.forall(_ == StageState.Success) shouldBe true
    port.executed.toList shouldBe List("src", "filtered", "output")
    result.attempts shouldBe Map("src" -> 1, "filtered" -> 1, "output" -> 1)
    // Every terminal transition is published on the bus with the scheduler-stamped sender
    port.busRows.size shouldBe 3
    port.busRows.forall(r => r.kind == "stage_state" && r.fromMember == "scheduler") shouldBe true
    port.persistedStates.last.stageStates.values.forall(_ == StageState.Success) shouldBe true
  }

  test("skip dependents of a failed stage") {
    val port = StubPort()
    port.failStage("primary")
    val result = runFlow(
      """flow FailingFlow = {
        |  stage primary = from [[1]] as t(id)
        |  stage transform = from primary | select *
        |}""".stripMargin,
      port
    )
    result.isSuccess shouldBe false
    result.stageStates("primary") shouldBe StageState.Failed
    result.stageStates("transform") shouldBe StageState.Skipped
    result.attempts("transform") shouldBe 0
    result.errors("primary") shouldContain "failed (stub)"
    port.executed.toList shouldBe List("primary")
  }

  test("run an if x.failed fallback and an if x.done stage after a failure") {
    val port = StubPort()
    port.failStage("primary")
    val result = runFlow(
      """flow ResilientFlow = {
        |  stage primary = from [[1]] as t(id)
        |  stage fallback if primary.failed = from [[1]] as t(id)
        |  stage cleanup if primary.done = from [[1], [2]] as t(id)
        |}""".stripMargin,
      port
    )
    result.stageStates("primary") shouldBe StageState.Failed
    result.stageStates("fallback") shouldBe StageState.Success
    result.stageStates("cleanup") shouldBe StageState.Success
  }

  test("skip a trigger stage when its condition does not hold") {
    val port   = StubPort()
    val result = runFlow(
      """flow HealthyFlow = {
        |  stage src = from [[1]] as t(id)
        |  stage alert if src.failed = from [[1]] as t(id)
        |  stage notify if src.done = from [[1]] as t(id)
        |}""".stripMargin,
      port
    )
    result.stageStates("src") shouldBe StageState.Success
    result.stageStates("alert") shouldBe StageState.Skipped
    result.stageStates("notify") shouldBe StageState.Success
  }

  test("evaluate and/or trigger combinations") {
    val port = StubPort()
    port.failStage("b")
    val result = runFlow(
      """flow TriggerFlow = {
        |  stage a = from [[1]] as t(id)
        |  stage b = from [[1]] as t(id)
        |  stage alert if a.failed or b.failed = from [[1]] as t(id)
        |  stage summary if a.done and b.done = from [[1]] as t(id)
        |  stage never if a.failed and b.failed = from [[1]] as t(id)
        |}""".stripMargin,
      port
    )
    result.stageStates("alert") shouldBe StageState.Success
    result.stageStates("summary") shouldBe StageState.Success
    result.stageStates("never") shouldBe StageState.Skipped
  }

  test("honor the retries config of a stage") {
    val port = StubPort()
    port.failStage("flaky", times = 2)
    val result = runFlow(
      """flow RetryFlow = {
        |  stage flaky with {
        |    retries: 3
        |    retry_delay: 10ms
        |  } = from [[1]] as t(id)
        |  stage next = from flaky | select *
        |}""".stripMargin,
      port
    )
    result.isSuccess shouldBe true
    result.stageStates("flaky") shouldBe StageState.Success
    result.attempts("flaky") shouldBe 3
    port.sleeps.toList shouldBe List(10L, 10L)
    result.stageStates("next") shouldBe StageState.Success
  }

  test("fail a stage after exhausting its retries") {
    val port = StubPort()
    port.failStage("flaky")
    val result = runFlow(
      """flow RetryFailFlow = {
        |  stage flaky with { retries: 2 } = from [[1]] as t(id)
        |}""".stripMargin,
      port
    )
    result.stageStates("flaky") shouldBe StageState.Failed
    result.attempts("flaky") shouldBe 3
    result.steps shouldBe 3L
  }

  test("dispatch a stage whose body calls agent(...) to invokeAgent") {
    val port   = StubPort()
    val result = runFlow(
      """flow AgentFlow = {
        |  stage plan = from [[1]] as t(id)
        |  stage summarize = from plan | agent('summarize results')
        |}""".stripMargin,
      port
    )
    result.isSuccess shouldBe true
    port.agentCalls.toList shouldBe
      List("summarize" -> AgentInvocation(name = "agent", args = List("summarize results")))
    // The plain query stage still goes through runSql, not invokeAgent
    port.executed.toList shouldBe List("plan", "summarize")
  }

  test("cancel stages once the max_steps budget is exhausted") {
    val port   = StubPort()
    val result = runFlow(
      """flow BudgetFlow = {
        |  stage a = from [[1]] as t(id)
        |  stage b = from [[1]] as t(id)
        |}""".stripMargin,
      port,
      config = FlowSchedulerConfig().withMaxSteps(1)
    )
    result.stageStates("a") shouldBe StageState.Success
    result.stageStates("b") shouldBe StageState.Cancelled
    result.steps shouldBe 1L
    result.errors("b") shouldContain "max_steps"
  }

end FlowSchedulerTest

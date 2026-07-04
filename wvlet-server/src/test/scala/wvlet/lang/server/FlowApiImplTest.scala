package wvlet.lang.server

import wvlet.lang.api.v1.flow.FlowApi
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.FlowRunRecord
import wvlet.lang.runner.FlowRunRegistry
import wvlet.lang.runner.StageRunRecord
import wvlet.lang.test.WvletDITest
import wvlet.uni.http.rpc.RPCException

import java.nio.file.Files

class FlowApiImplTest extends WvletDITest:

  private lazy val workDir: String =
    val dir = Files.createTempDirectory("wvlet-flow-api-test")
    // A .wv file makes the folder a wvlet workspace, so targetFolder stays under this temp dir
    Files.writeString(dir.resolve("dummy.wv"), "")
    val registry = FlowRunRegistry.forWorkEnv(WorkEnv(dir.toString))
    registry.save(
      FlowRunRecord(
        runId = "run1",
        flowName = "FlowA",
        state = FlowRunRecord.STATE_SUCCESS,
        startedAtMillis = 100L,
        finishedAtMillis = Some(150L),
        stages = List(
          StageRunRecord("src", "success", 1, None, Some("__wv_flow_run1_src")),
          StageRunRecord("out", "success", 1, None, Some("__wv_flow_run1_out"))
        ),
        args = Map("segment" -> "'a'"),
        runTimeMillis = Some(90L)
      )
    )
    registry.save(
      FlowRunRecord(
        runId = "run2",
        flowName = "FlowB",
        state = FlowRunRecord.STATE_FAILED,
        startedAtMillis = 200L,
        finishedAtMillis = Some(260L),
        stages = List(StageRunRecord("broken", "failed", 3, Some("boom"), None))
      )
    )
    dir.toString

  end workDir

  initDesign:
    _.bindImpl[FlowApi, FlowApiImpl].bindInstance[WorkEnv](WorkEnv(workDir))

  test("list recorded runs most recent first") {
    val api  = dep[FlowApi]
    val runs = api.listRuns(FlowApi.FlowRunListRequest())
    runs.map(_.runId) shouldBe List("run2", "run1")
    val r1 = runs.last
    r1.flowName shouldBe "FlowA"
    r1.flowCall shouldBe "FlowA(segment = 'a')"
    r1.state shouldBe FlowRunRecord.STATE_SUCCESS
    r1.startedAtMillis shouldBe 100L
    r1.finishedAtMillis shouldBe Some(150L)
    r1.runTimeMillis shouldBe Some(90L)
  }

  test("filter runs by flow name and bound the list size") {
    val api = dep[FlowApi]
    api.listRuns(FlowApi.FlowRunListRequest(flowName = Some("FlowA"))).map(_.runId) shouldBe
      List("run1")
    api.listRuns(FlowApi.FlowRunListRequest(limit = 1)).map(_.runId) shouldBe List("run2")
    api.listRuns(FlowApi.FlowRunListRequest(flowName = Some("NoSuchFlow"))) shouldBe Nil
  }

  test("return per-stage details of a run") {
    val api    = dep[FlowApi]
    val detail = api.getRun(FlowApi.FlowRunRequest("run2"))
    detail.run.runId shouldBe "run2"
    detail.run.state shouldBe FlowRunRecord.STATE_FAILED
    detail.stages.size shouldBe 1
    val stage = detail.stages.head
    stage.name shouldBe "broken"
    stage.state shouldBe "failed"
    stage.attempts shouldBe 3
    stage.error shouldBe Some("boom")
  }

  test("report an unknown run id as not found") {
    val api = dep[FlowApi]
    val e   = intercept[RPCException] {
      api.getRun(FlowApi.FlowRunRequest("nosuchrun"))
    }
    e.getMessage shouldContain "nosuchrun"
  }

end FlowApiImplTest

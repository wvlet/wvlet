package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

import java.nio.file.Files

/**
  * Tests for `wvlet flow run|list|show` subcommands
  */
class WvletFlowCommandTest extends UniTest:

  private lazy val flowDir: String =
    val dir = Files.createTempDirectory("wvlet-flow-test")
    Files.writeString(
      dir.resolve("pipeline.wv"),
      """flow SamplePipeline = {
        |  stage src = from [[1, 'a'], [2, 'b']] as t(id, name)
        |  stage filtered = from src | where name = 'a'
        |}
        |
        |flow FallbackPipeline = {
        |  stage primary = from nonexistent_table_xyz
        |  stage fallback if primary.failed = from [[0]] as t(id)
        |}
        |""".stripMargin
    )
    dir.toAbsolutePath.toString

  test("list flows in the working folder") {
    WvletMain.main(s"flow list -w ${flowDir}")
  }

  test("show a flow plan") {
    WvletMain.main(s"flow show SamplePipeline -w ${flowDir}")
  }

  test("run a flow from the CLI") {
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir}")
  }

  test("run a flow with failing and fallback stages") {
    // In sbt, failing flows do not System.exit; this verifies fallback execution completes
    WvletMain.main(s"flow run FallbackPipeline -w ${flowDir}")
  }

  test("list and show flow run sessions") {
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir}")
    WvletMain.main(s"flow session list -w ${flowDir}")

    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    val runs = FlowRunRegistry.forWorkEnv(WorkEnv(flowDir)).list()
    runs.nonEmpty shouldBe true
    runs.head.flowName shouldBe "SamplePipeline"
    WvletMain.main(s"flow session show ${runs.head.runId} -w ${flowDir}")
  }

  test("report an error for an unknown session sub command") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"flow session frobnicate -w ${flowDir}")
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("report an error when cancelling an unknown run") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"flow session cancel nosuchrun -w ${flowDir}")
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("report the terminal state when cancelling a finished run") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir}")
    val runs = FlowRunRegistry.forWorkEnv(WorkEnv(flowDir)).list()
    // Cancelling an already-finished run reports its state without failing
    WvletMain.main(s"flow session cancel ${runs.head.runId} -w ${flowDir}")
  }

  test("resume a failed flow run from the CLI") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    WvletMain.main(s"flow run FallbackPipeline -w ${flowDir}")
    val registry = FlowRunRegistry.forWorkEnv(WorkEnv(flowDir))
    val failed   =
      registry.list().find(r => r.flowName == "FallbackPipeline" && r.state == "failed").get
    // The primary stage fails again on resume, but the resume path completes and updates
    // the same run record
    WvletMain.main(s"flow session resume ${failed.runId} -w ${flowDir}")
    registry.get(failed.runId).get.state shouldBe "failed"
  }

  test("reject resuming a run that never failed") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir}")
    val succeeded =
      FlowRunRegistry
        .forWorkEnv(WorkEnv(flowDir))
        .list()
        .find(r => r.flowName == "SamplePipeline" && r.state == "success")
        .get
    // Resuming a successful run is a no-op message, not an error
    WvletMain.main(s"flow session resume ${succeeded.runId} -w ${flowDir}")
  }

  test("clean terminal flow run records") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir}")
    WvletMain.main(s"flow session clean -w ${flowDir}")
    FlowRunRegistry.forWorkEnv(WorkEnv(flowDir)).list() shouldBe Nil
  }

  test("run a flow and inspect sessions with the SQLite run store") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunStore
    WvletMain.main(s"flow run SamplePipeline -w ${flowDir} --run-store sqlite")
    WvletMain.main(s"flow session list -w ${flowDir} --run-store sqlite")
    val store = FlowRunStore.ofType(FlowRunStore.STORE_TYPE_SQLITE, WorkEnv(flowDir))
    try
      val runs = store.list()
      runs.nonEmpty shouldBe true
      runs.head.flowName shouldBe "SamplePipeline"
      WvletMain.main(s"flow session show ${runs.head.runId} -w ${flowDir} --run-store sqlite")
    finally
      store.close()
  }

  test("report an error for an unknown flow name") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"flow run NoSuchFlow -w ${flowDir}")
    }
    e.statusCode shouldBe StatusCode.FLOW_NOT_FOUND
  }

end WvletFlowCommandTest

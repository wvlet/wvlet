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

  test("report an error for an unknown flow name") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"flow run NoSuchFlow -w ${flowDir}")
    }
    e.statusCode shouldBe StatusCode.FLOW_NOT_FOUND
  }

end WvletFlowCommandTest

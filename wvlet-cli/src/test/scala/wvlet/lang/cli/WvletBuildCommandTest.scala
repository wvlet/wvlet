package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

import java.nio.file.Files

/**
  * Tests for `wvlet build`
  */
class WvletBuildCommandTest extends UniTest:

  private lazy val projectDir: String =
    val dir = Files.createTempDirectory("wvlet-build-cli-test")
    Files.writeString(
      dir.resolve("models.wv"),
      """model cli_src with { materialized: 'table' } = {
        |  from [[1, 'a'], [2, 'b']] as t(id, name)
        |}
        |
        |model cli_names with { materialized: 'view' } = {
        |  from cli_src
        |  select name
        |}
        |""".stripMargin
    )
    dir.toAbsolutePath.toString

  test("print the build plan with --dry-run") {
    WvletMain.main(s"build --dry-run -w ${projectDir}")
  }

  test("build all materialized models") {
    WvletMain.main(s"build -w ${projectDir}")
  }

  test("build a selection with its upstream models") {
    WvletMain.main(s"build -s +cli_names -w ${projectDir}")
  }

  test("report an unknown model in a selection") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"build -s no_such_model -w ${projectDir}")
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
  }

end WvletBuildCommandTest

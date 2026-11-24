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

  // The project has a view model, which needs a SQL engine
  test("build all materialized models on DuckDB with -t duckdb") {
    WvletMain.main(s"build -t duckdb -w ${projectDir}")
  }

  test("build a selection with its upstream models") {
    WvletMain.main(s"build -t duckdb -s +cli_names -w ${projectDir}")
  }

  test("build table models on the wvlet engine by default into target/warehouse") {
    import wvlet.lang.compiler.WorkEnv
    import wvlet.lang.runner.FlowRunRegistry
    val dir = Files.createTempDirectory("wvlet-build-engine")
    Files.writeString(
      dir.resolve("models.wv"),
      """model engine_src with { materialized: 'table' } = {
        |  from [[1, 'a'], [2, 'b']] as t(id, name)
        |}
        |
        |model engine_names with { materialized: 'table' } = {
        |  from engine_src
        |  select name
        |}
        |""".stripMargin
    )
    WvletMain.main(s"build -w ${dir}")
    val runs = FlowRunRegistry.forWorkEnv(WorkEnv(dir.toString)).list()
    runs.map(_.state) shouldBe List("success")
    runs.head.stages.map(_.state).toSet shouldBe Set("success")
    Files.isDirectory(dir.resolve("target/warehouse/main/engine_src")) shouldBe true

    // The selected model's upstream persists in the warehouse, so a selected build passes the
    // upstream check
    WvletMain.main(s"build -s engine_names -w ${dir}")
    FlowRunRegistry.forWorkEnv(WorkEnv(dir.toString)).list().map(_.state) shouldBe
      List("success", "success")
  }

  test("report an unknown model in a selection") {
    val e = intercept[WvletLangException] {
      WvletMain.main(s"build -s no_such_model -w ${projectDir}")
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
  }

end WvletBuildCommandTest

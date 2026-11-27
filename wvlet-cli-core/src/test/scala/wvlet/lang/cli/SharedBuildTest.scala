package wvlet.lang.cli

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.ModelBuildPlan
import wvlet.lang.runner.ModelBuilder
import wvlet.uni.io.IO
import wvlet.uni.test.UniTest

/**
  * `wvlet build` of the npm and Native `wvlet` (the sequential [[ModelBuilder]]), on the JVM,
  * Node.js, and Native
  */
class SharedBuildTest extends UniTest:

  private def cli = WvletCli(WvletCliGlobalOption())

  private def project(models: String): String =
    val dir = IO.createTempDirectory(prefix = "shared-build-test")
    IO.writeString(dir.resolve("models.wv"), models)
    dir.path

  private val twoTables =
    """model shared_src with { materialized: 'table' } = {
      |  from [[1, 'a'], [2, 'b']] as t(id, name)
      |}
      |
      |model shared_names with { materialized: 'table' } = {
      |  from shared_src
      |  select name
      |}
      |""".stripMargin

  test("build table models on the wvlet engine into target/warehouse") {
    val dir = project(twoTables)
    cli.build(WvletBuildOption(workFolder = dir))
    IO.isDirectory(IO.path(WorkEnv(dir).warehouseFolder, "main", "shared_src")) shouldBe true
    IO.isDirectory(IO.path(WorkEnv(dir).warehouseFolder, "main", "shared_names")) shouldBe true

    // A later build reads the persisted upstream, so building only the downstream model passes
    // the upstream check
    cli.build(WvletBuildOption(workFolder = dir).withSelect("shared_names"))
  }

  test("fail a selection whose upstream model is not built yet") {
    val dir = project(twoTables)
    val e   = intercept[WvletLangException] {
      cli.build(WvletBuildOption(workFolder = dir).withSelect("shared_names"))
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
    e.getMessage shouldContain "shared_src"
  }

  test("print the build SQL with --dry-run without building") {
    val dir = project(twoTables)
    cli.build(WvletBuildOption(workFolder = dir).withDryRun(true))
    IO.exists(IO.path(WorkEnv(dir).warehouseFolder, "main", "shared_src")) shouldBe false
  }

  test("report an unknown model in a selection") {
    val e = intercept[WvletLangException] {
      cli.build(WvletBuildOption(workFolder = project(twoTables)).withSelect("no_such_model"))
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
  }

  test("reject --run-store, which needs the JVM CLI") {
    val e = intercept[WvletLangException] {
      cli.build(WvletBuildOption(workFolder = project(twoTables)).withRunStore("sqlite"))
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    e.getMessage shouldContain "JVM"
  }

  test("retry a failed model, then skip its downstream models") {
    val dir = project(
      """model broken with { materialized: 'table', retries: 1, retry_delay: 1ms } = {
        |  from shared_no_such_table
        |}
        |
        |model after_broken with { materialized: 'table' } = {
        |  from broken
        |}
        |""".stripMargin
    )
    val workEnv = WorkEnv(dir)
    val opt     = WvletBuildOption(workFolder = dir)
    val plan    = ModelBuildPlan.plan(dir, workEnv, Nil, DBType.Wvlet)
    val builder = ModelBuilder(opt.resolveProfile, workEnv)
    val result  =
      try builder.build(plan)
      finally builder.close()
    val broken = result.stageResult("broken").get
    broken.state shouldBe StageState.Failed
    broken.attempts shouldBe 2
    result.stageResult("after_broken").map(_.state) shouldBe Some(StageState.Skipped)

    // The command reports the failure with a non-zero exit
    val e = intercept[WvletLangException] {
      cli.build(opt)
    }
    e.statusCode shouldBe StatusCode.QUERY_EXECUTION_FAILURE
    e.getMessage shouldContain "broken"
  }

end SharedBuildTest

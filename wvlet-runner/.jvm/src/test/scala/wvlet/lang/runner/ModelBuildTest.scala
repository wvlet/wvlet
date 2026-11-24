package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import java.nio.file.Files

/**
  * Tests for `wvlet build`: materialized models built in dependency order on DuckDB
  */
class ModelBuildTest extends UniTest:
  private val workEnv             = WorkEnv(".")
  private val profile             = Profile.defaultDuckDBProfile
  private val dbConnectorProvider = ConnectorProvider(workEnv)
  private val connector           = dbConnectorProvider.getConnector(profile)

  override def afterAll: Unit = dbConnectorProvider.close()

  /** Write the given files into a fresh project folder */
  private def project(files: (String, String)*): String =
    val dir = Files.createTempDirectory("wvlet-build-test")
    files.foreach { (name, content) =>
      Files.writeString(dir.resolve(name), content)
    }
    dir.toAbsolutePath.toString

  private def build(dir: String, selectors: List[String] = Nil): FlowExecutionResult =
    val plan = ModelBuildLauncher.plan(dir, workEnv, selectors, DBType.DuckDB)
    ModelBuildLauncher.checkUpstreamsBuilt(plan, _ => connector)
    FlowExecutor(connector, workEnv).execute(plan.flow)(using plan.context)

  private def rows(sql: String): List[List[String]] =
    connector.runQuery(sql) { rs =>
      val n   = rs.getMetaData.getColumnCount
      val out = List.newBuilder[List[String]]
      while rs.next() do
        out += (1 to n).map(i => String.valueOf(rs.getObject(i))).toList
      out.result()
    }

  private val shop = Seq(
    "staging.wv" -> """model raw_orders = {
        |  from [[1, 'alice', 100], [2, 'bob', 50], [3, 'alice', 30]] as t(id, customer, amount)
        |}
        |
        |model stg_orders with { materialized: 'view', alias: 'mb_stg_orders' } = {
        |  from raw_orders
        |  where amount > 40
        |}
        |""".stripMargin,
    "marts.wv" -> """-- An ephemeral model in between: the mart still depends on stg_orders
        |model big_orders = {
        |  from stg_orders
        |  where amount >= 50
        |}
        |
        |model customer_totals with { materialized: 'table', alias: 'mb_customer_totals' } = {
        |  from big_orders
        |  group by customer
        |  agg amount.sum as total
        |  order by customer
        |}
        |""".stripMargin
  )

  test("build materialized models in dependency order") {
    val dir    = project(shop*)
    val result = build(dir)
    result.hasError shouldBe false
    result.stageResult("stg_orders").map(_.state) shouldBe Some(StageState.Success)
    result.stageResult("customer_totals").map(_.state) shouldBe Some(StageState.Success)
    rows("select customer, total from mb_customer_totals order by customer") shouldBe
      List(List("alice", "100"), List("bob", "50"))
    // The view was created, not a table
    rows(
      "select table_type from information_schema.tables where table_name = 'mb_stg_orders'"
    ) shouldBe List(List("VIEW"))
  }

  test("build models without an alias into tables named after the model") {
    val dir = project(
      "m.wv" -> """model mb_plain_src with { materialized: 'table' } = { from [[1], [2]] as t(x) }
          |model mb_plain_sum with { materialized: 'table' } = {
          |  from mb_plain_src
          |  agg x.sum as total
          |}
          |""".stripMargin
    )
    build(dir).hasError shouldBe false
    rows("select total from mb_plain_sum") shouldBe List(List("3"))
  }

  test("persist to the location a table declaration binds") {
    // A binding on the current search path governs the bare model name
    val dir = project("m.wv" -> """table mb_bound_src in memory.main = { x: int }
          |model mb_bound_src with { materialized: 'table' } = { from [[1]] as t(x) }
          |model mb_bound_down with { materialized: 'table' } = { from mb_bound_src }
          |""".stripMargin)
    val plan = ModelBuildLauncher.plan(dir, workEnv, Nil, DBType.DuckDB)
    plan.graph.get("mb_bound_src").map(_.targetName) shouldBe Some("memory.main.mb_bound_src")
    plan.sqlStatements.find(_._1.name == "mb_bound_down").get._2.mkString shouldContain
      "memory.main.mb_bound_src"
    build(dir).hasError shouldBe false
    rows("select x from memory.main.mb_bound_down") shouldBe List(List("1"))
  }

  test("derive the graph through ephemeral models") {
    val plan = ModelBuildLauncher.plan(project(shop*), workEnv, Nil, DBType.DuckDB)
    plan.selected.map(_.name) shouldBe List("stg_orders", "customer_totals")
    plan.graph.upstreams("customer_totals") shouldBe List("stg_orders")
  }

  test("read a materialized upstream from its persisted relation") {
    val plan     = ModelBuildLauncher.plan(project(shop*), workEnv, Nil, DBType.DuckDB)
    val martSQL  = plan.sqlStatements.find(_._1.name == "customer_totals").get._2.mkString("\n")
    val stageSQL = plan.sqlStatements.find(_._1.name == "stg_orders").get._2.mkString("\n")
    martSQL shouldContain "mb_stg_orders"
    // The upstream filter is not inlined into the downstream model
    martSQL.contains("amount > 40") shouldBe false
    stageSQL shouldContain "create or replace view mb_stg_orders"
  }

  test("select models with dbt-style selectors") {
    val dir                                = project(shop*)
    def selected(s: String*): List[String] = ModelBuildLauncher
      .plan(dir, workEnv, s.toList, DBType.DuckDB)
      .selected
      .map(_.name)
    selected("customer_totals") shouldBe List("customer_totals")
    selected("+customer_totals") shouldBe List("stg_orders", "customer_totals")
    selected("stg_orders+") shouldBe List("stg_orders", "customer_totals")
    selected("stg_orders") shouldBe List("stg_orders")
    val e = intercept[WvletLangException] {
      selected("unknown_model")
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
  }

  test("reject selectors that name no model") {
    val dir = project(shop*)
    List(List(""), List(","), List(" , ")).foreach { selectors =>
      val e = intercept[WvletLangException] {
        ModelBuildLauncher.plan(dir, workEnv, selectors, DBType.DuckDB)
      }
      e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
  }

  private def cycleError(selectors: List[String], files: (String, String)*): WvletLangException =
    val dir = project(files*)
    intercept[WvletLangException] {
      ModelBuildLauncher.plan(dir, workEnv, selectors, DBType.DuckDB)
    }

  test("report a model that reads itself") {
    val e = cycleError(
      Nil,
      "m.wv" -> "model mb_self with { materialized: 'table' } = { from mb_self }"
    )
    e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
    e.getMessage shouldContain "mb_self -> mb_self"
  }

  test("report a model that reads itself through an ephemeral model") {
    val e = cycleError(
      Nil,
      "m.wv" -> """model mb_eph = { from mb_loop }
          |model mb_loop with { materialized: 'table' } = { from mb_eph }
          |""".stripMargin
    )
    e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
    e.getMessage shouldContain "mb_loop -> mb_loop"
  }

  test("report a cycle even when the selection covers only part of it") {
    val files = Seq("m.wv" -> """model mb_ca with { materialized: 'table' } = { from mb_cb }
          |model mb_cb with { materialized: 'table' } = { from mb_ca }
          |model mb_free with { materialized: 'table' } = { from [[1]] as t(x) }
          |""".stripMargin)
    List(Nil, List("mb_ca"), List("mb_free")).foreach { selectors =>
      val e = cycleError(selectors, files*)
      e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
      e.getMessage shouldContain "mb_ca -> mb_cb -> mb_ca"
    }
  }

  test("fail when an unselected upstream model is not built") {
    val dir = project(
      "m.wv" -> """model mb_up_unbuilt with { materialized: 'table' } = { from [[1]] as t(x) }
          |model mb_down with { materialized: 'table' } = { from mb_up_unbuilt }
          |""".stripMargin
    )
    val e = intercept[WvletLangException] {
      build(dir, List("mb_down"))
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
    e.getMessage shouldContain "mb_up_unbuilt"
  }

  test("skip downstream models when a model fails") {
    val dir = project(
      "m.wv" ->
        """model mb_broken with { materialized: 'table' } = { from [[1]] as t(x) | where x / 0 > 'a'::int }
          |model mb_after_broken with { materialized: 'table' } = { from mb_broken }
          |""".stripMargin
    )
    val result = build(dir)
    result.hasError shouldBe true
    result.stageResult("mb_after_broken").map(_.state) shouldNotBe Some(StageState.Success)
  }

  test("reject unknown config keys") {
    val dir = project("m.wv" -> "model m with { materialised: 'table' } = { from [[1]] as t(x) }")
    val e   = intercept[WvletLangException] {
      ModelBuildLauncher.plan(dir, workEnv, Nil, DBType.DuckDB)
    }
    e.statusCode shouldBe StatusCode.INVALID_MODEL_CONFIG
    e.getMessage shouldContain "materialised"
  }

  test("reject unknown materializations") {
    val dir = project(
      "m.wv" -> "model m with { materialized: 'snapshot' } = { from [[1]] as t(x) }"
    )
    val e = intercept[WvletLangException] {
      ModelBuildLauncher.plan(dir, workEnv, Nil, DBType.DuckDB)
    }
    e.statusCode shouldBe StatusCode.INVALID_MODEL_CONFIG
  }

  test("reject materialized models with parameters") {
    val dir = project(
      "m.wv" ->
        "model m(n: int) with { materialized: 'table' } = { from [[1]] as t(x) where x = n }"
    )
    val e = intercept[WvletLangException] {
      ModelBuildLauncher.plan(dir, workEnv, Nil, DBType.DuckDB)
    }
    e.statusCode shouldBe StatusCode.INVALID_MODEL_CONFIG
    e.getMessage shouldContain "parameters"
  }

  test("report a project without materialized models") {
    val dir = project("m.wv" -> "model m = { from [[1]] as t(x) }")
    val e   = intercept[WvletLangException] {
      ModelBuildLauncher.plan(dir, workEnv, Nil, DBType.DuckDB)
    }
    e.statusCode shouldBe StatusCode.MODEL_NOT_FOUND
  }

end ModelBuildTest

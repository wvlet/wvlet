package wvlet.lang.runner.connector.trino

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.runner.ActivationRequest
import wvlet.lang.runner.ActivationSink
import wvlet.lang.runner.FlowExecutor
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.test.UniTest

import scala.collection.mutable.ListBuffer

/**
  * Flow execution against an embedded Trino server: stage materialization uses engine-aware DDL (no
  * `create or replace table`), all statements run over the HTTP query path, and notification hooks
  * build their run-summary table with the same mechanism
  */
class TrinoFlowExecutorTest extends UniTest:
  private val server  = TestTrinoServer().withCustomMemoryPlugin
  private val workEnv = WorkEnv(".")
  private val profile = Profile(
    name = s"local-trino@${server.address}",
    connectors = Seq(
      ConnectorConfig(
        name = "trino",
        `type` = "trino",
        user = Some("test"),
        password = Some(""),
        host = Some(server.address),
        catalog = Some("memory"),
        schema = Some("main"),
        properties = Map("useSSL" -> false)
      )
    )
  )

  private val dbConnectorProvider = ConnectorProvider(workEnv)
  private val connector           = dbConnectorProvider.getConnector(profile)

  // Run-scoped flow tables land in the profile's catalog/schema, which must exist and be writable
  connector.createSchema("memory", "main")

  override def afterAll: Unit = Control.closeResources(dbConnectorProvider, server)

  private def compileFlow(wv: String): (FlowDef, Context) =
    val compiler = Compiler(
      CompilerOptions(workEnv = workEnv, catalog = Some("memory"), schema = Some("main"))
    )
    // Generate Trino SQL for the stage bodies
    compiler.setDefaultCatalog(connector.getCatalog("memory", "main"))
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

  private def countRows(table: String): Long =
    val rows = connector.queryJsonRows(s"""select count(*) as cnt from "${table}"""")
    rows.size shouldBe 1
    "\\d+".r.findFirstIn(rows.head).map(_.toLong).getOrElse(fail(s"No count in ${rows.head}"))

  test("materialize flow stages on Trino with engine-aware DDL") {
    val (flow, ctx) = compileFlow("""flow TrinoFlow = {
        |  stage src = from [[1, 'a'], [2, 'b'], [3, 'a']] as t(id, name)
        |  stage filtered = from src | where name = 'a'
        |  stage output = from filtered | select id
        |}""".stripMargin)
    val result = FlowExecutor(connector, workEnv).execute(flow)(using ctx)
    result.isSuccess shouldBe true
    def tableOf(stage: String): String = result
      .stageResult(stage)
      .flatMap(_.table)
      .getOrElse(fail(s"Stage ${stage} has no materialized table"))
    countRows(tableOf("filtered")) shouldBe 2L
    countRows(tableOf("output")) shouldBe 2L

    // Retrying a stage of the same run re-creates its table (drop + create table path)
    val again = FlowExecutor(connector, workEnv).execute(flow)(using ctx)
    again.isSuccess shouldBe true

    // Run-scoped tables are dropped over the same HTTP path
    FlowExecutor.dropRunTables(connector, result.runId, result.stageResults.map(_.name))
    val remaining = connector
      .listTables("memory", "main")
      .map(_.name)
      .filter(_.contains(result.runId.toLowerCase))
    remaining shouldBe Nil
    FlowExecutor.dropRunTables(connector, again.runId, again.stageResults.map(_.name))
  }

  test("deliver run notification summaries on Trino") {
    val recorded = ListBuffer.empty[List[String]]
    val sink     =
      new ActivationSink:
        override def name: String                               = "test_notify"
        override def activate(request: ActivationRequest): Unit =
          recorded +=
            request
              .connector
              .queryJsonRows(
                s"""select "stage", "state" from "${request.table}" order by "stage""""
              )

    val (flow, ctx) = compileFlow(
      """flow TrinoNotifyFlow with { on_finish: activate('test_notify') } = {
        |  stage src = from [[1]] as t(id)
        |}""".stripMargin
    )
    val result =
      FlowExecutor(connector, workEnv, activationSinks = List(sink)).execute(flow)(using ctx)
    result.isSuccess shouldBe true
    recorded.size shouldBe 1
    recorded.head.size shouldBe 1
    recorded.head.head shouldContain "src"
    recorded.head.head shouldContain "success"
    FlowExecutor.dropRunTables(connector, result.runId, result.stageResults.map(_.name))
  }

  test("export activate('file') output on Trino via the DuckDB handoff") {
    val exportPath = java.nio.file.Paths.get("target/trino-file-activation.csv")
    java.nio.file.Files.deleteIfExists(exportPath)
    val (flow, ctx) = compileFlow("""flow TrinoFileFlow = {
        |  stage src = from [[1, 'a'], [2, 'b']] as t(id, name)
        |  stage export = from src | activate('file', path: 'target/trino-file-activation.csv')
        |}""".stripMargin)
    val result = FlowExecutor(connector, workEnv).execute(flow)(using ctx)
    result.isSuccess shouldBe true
    val content = java.nio.file.Files.readString(exportPath)
    content shouldContain "id"
    content shouldContain "name"
    content shouldContain "1"
    content shouldContain "a"
    content shouldContain "2"
    content shouldContain "b"
    java.nio.file.Files.deleteIfExists(exportPath)
    FlowExecutor.dropRunTables(connector, result.runId, result.stageResults.map(_.name))
  }

  test("export an empty stage output on Trino as a schema-only file") {
    val exportPath = java.nio.file.Paths.get("target/trino-file-activation-empty.csv")
    java.nio.file.Files.deleteIfExists(exportPath)
    val (flow, ctx) = compileFlow("""flow TrinoEmptyFileFlow = {
        |  stage src = from [[1, 'a']] as t(id, name)
        |  stage export = from src | where id > 100 | activate('file', path: 'target/trino-file-activation-empty.csv')
        |}""".stripMargin)
    val result = FlowExecutor(connector, workEnv).execute(flow)(using ctx)
    result.isSuccess shouldBe true
    // Header-only CSV: the schema survives even with zero rows
    val content = java.nio.file.Files.readString(exportPath)
    content shouldContain "id"
    content shouldContain "name"
    java.nio.file.Files.deleteIfExists(exportPath)
    FlowExecutor.dropRunTables(connector, result.runId, result.stageResults.map(_.name))
  }

end TrinoFlowExecutorTest

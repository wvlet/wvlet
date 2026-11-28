package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.query.QueryDetailRequest
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.exec.MemoryPool
import wvlet.lang.runner.ThreadManager
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.activity.InMemoryItemStore
import wvlet.lang.runner.activity.TaskQueue
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.test.UniTest

import java.time.Instant

/** The query list, query detail, and cluster info the web UI reads */
class QueryMonitorTest extends UniTest:

  private val workDir = new java.io.File("target/query-monitor-test")
  workDir.mkdirs()
  private val workEnv       = WorkEnv(path = workDir.getPath)
  private val provider      = ConnectorProvider(workEnv)
  private val threadManager = ThreadManager()

  private def sessionsOf(engine: ConnectorConfig): ScriptRunnerSessions = ScriptRunnerSessions(
    workEnv,
    WvletScriptRunnerConfig(
      interactive = false,
      profile = Profile(name = "test", connectors = Seq(engine)),
      catalog = engine.catalog,
      schema = engine.schema
    ),
    provider,
    threadManager,
    TaskQueue(InMemoryItemStore()),
    ScriptRunnerSessions.DefaultIdleTimeout
  )

  private val duckdbSessions = sessionsOf(
    ConnectorConfig(
      name = "duckdb",
      `type` = "duckdb",
      default = true,
      catalog = Some("memory"),
      schema = Some("main")
    )
  )

  private val engineSessions = sessionsOf(
    ConnectorConfig(name = "wvlet", `type` = "wvlet", default = true)
  )

  private val engineService = QueryService(
    engineSessions,
    QueryManagerConfig(),
    QueryEventListener.noOp,
    QueryIdGenerator(),
    MemoryPool()
  )

  private val service = QueryService(duckdbSessions)
  private val api     = FrontendApiImpl(service, workEnv)

  override def afterAll: Unit =
    service.close()
    engineService.close()
    duckdbSessions.close()
    engineSessions.close()
    threadManager.close()
    provider.close()

  private def awaitFinished(s: QueryService, queryId: String): Unit =
    val deadline = System.currentTimeMillis() + 30000
    while !s.fetchNext(QueryInfoRequest(queryId, "0")).status.isFinished &&
      System.currentTimeMillis() < deadline
    do
      Thread.sleep(20)
    s.fetchNext(QueryInfoRequest(queryId, "0")).status.isFinished shouldBe true

  private def run(query: String, sessionId: Option[String] = None): String =
    val id = service.enqueue(QueryRequest(query = query, sessionId = sessionId)).queryId
    awaitFinished(service, id)
    id

  test("should list queries newest first with their session, times, and rows") {
    val first  = run("select 1 as monitor_a", Some("s1"))
    val second = run("select 2 as monitor_b", Some("s2"))
    val list   = api.listQueries(QueryListRequest(text = Some("monitor_")))
    list.queries.map(_.queryId) shouldBe Seq(second, first)
    list.totalMatched shouldBe 2
    list.nextPageToken shouldBe None
    val s = list.queries.head
    s.status shouldBe QueryStatus.FINISHED
    s.sessionId shouldBe Some("s2")
    s.queryHead shouldContain "monitor_b"
    s.resultRows shouldBe Some(1)
    s.startedAt.isDefined shouldBe true
    s.runMillis.isDefined shouldBe true
    // DuckDB queries count no engine memory
    s.memory.tracked shouldBe false
  }

  test("should filter by status, session, text, and creation time") {
    val before = Instant.now()
    val ok     = run("select 10 as filter_ok", Some("filter-session"))
    val failed = run("select filter_missing_column from filter_missing_table", Some("other"))

    def ids(r: QueryListRequest): Seq[String] = api
      .listQueries(r.copy(text = Some("filter_")))
      .queries
      .map(_.queryId)

    ids(QueryListRequest(statuses = Seq(QueryStatus.FAILED))) shouldBe Seq(failed)
    ids(QueryListRequest(statuses = Seq(QueryStatus.FINISHED, QueryStatus.FAILED))) shouldBe
      Seq(failed, ok)
    ids(QueryListRequest(sessionId = Some("filter-session"))) shouldBe Seq(ok)
    ids(QueryListRequest(createdBefore = Some(before))) shouldBe Nil
    ids(QueryListRequest(createdAfter = Some(before))) shouldBe Seq(failed, ok)
    // Text matches ignore case
    api.listQueries(QueryListRequest(text = Some("FILTER_OK"))).queries.map(_.queryId) shouldBe
      Seq(ok)
    // A failed query carries its first error
    api
      .listQueries(QueryListRequest(text = Some("filter_missing")))
      .queries
      .head
      .errorMessage
      .isDefined shouldBe true
  }

  test("should page through the list with page tokens") {
    val ids = (1 to 5).map(i => run(s"select ${i} as paging_${i}"))
    val p1  = api.listQueries(QueryListRequest(text = Some("paging_"), limit = 2))
    val p2  = api.listQueries(
      QueryListRequest(text = Some("paging_"), limit = 2, pageToken = p1.nextPageToken)
    )
    val p3 = api.listQueries(
      QueryListRequest(text = Some("paging_"), limit = 2, pageToken = p2.nextPageToken)
    )
    val pages = Seq(p1, p2, p3)
    pages.flatMap(_.queries.map(_.queryId)) shouldBe ids.reverse
    pages.map(_.totalMatched).distinct shouldBe Seq(5)
    p3.nextPageToken shouldBe None
    intercept[WvletLangException] {
      api.listQueries(QueryListRequest(pageToken = Some("not-a-token")))
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("should show a query's text, SQL, and errors in its detail") {
    val id     = run("select 1 as detail_x")
    val detail = api.getQueryDetail(QueryDetailRequest(id))
    detail.query shouldContain "detail_x"
    detail.summary.queryId shouldBe id
    // DuckDB receives generated SQL; the engine ran nothing
    detail.sql.mkString shouldContain "detail_x"
    detail.enginePlans shouldBe Nil

    val failed = run("select detail_missing from detail_missing_table")
    val d      = api.getQueryDetail(QueryDetailRequest(failed))
    d.summary.status shouldBe QueryStatus.FAILED
    d.errors.nonEmpty shouldBe true

    intercept[WvletLangException] {
      api.getQueryDetail(QueryDetailRequest("20260101_000000_00000_aaaaa"))
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("should record the engine plan and peak memory of in-process engine queries") {
    val id =
      engineService
        .enqueue(
          QueryRequest(query =
            "from tpch.tiny.orders group by o_orderstatus select o_orderstatus, count(*)"
          )
        )
        .queryId
    awaitFinished(engineService, id)
    val d = engineService.detail(id)
    d.summary.status shouldBe QueryStatus.FINISHED
    d.enginePlans.size shouldBe 1
    d.enginePlans.head shouldContain "↓"
    d.summary.memory.tracked shouldBe true
    d.summary.memory.peakBytes > 0 shouldBe true
    // Released once the query finished
    d.summary.memory.reservedBytes shouldBe 0L
  }

  test("should report query counts, limits, memory, and thread load") {
    val c = api.getClusterInfo
    c.serverId shouldBe service.serverId
    c.queries.maxRunning shouldBe QueryManagerConfig().maxRunningQueries
    c.queries.running shouldBe 0
    c.queries.finished > 0 shouldBe true
    c.queryWorkers.threads shouldBe QueryManagerConfig().maxRunningQueries
    c.engineThreads.threads > 0 shouldBe true
    c.memoryPool.killedQueries shouldBe 0L
    c.heap.maxBytes > 0 shouldBe true
  }

end QueryMonitorTest

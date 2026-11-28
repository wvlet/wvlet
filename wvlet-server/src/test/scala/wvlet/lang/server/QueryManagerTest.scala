package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.query.QueryInfo
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
  * Admission, per-session ordering, events, retention, and request-id dedup of [[QueryService]].
  * Queries are held in their slot by a listener that blocks their STARTED event, so tests don't
  * depend on query run times.
  */
class QueryManagerTest extends UniTest:

  private val workDir = new java.io.File("target/query-manager-test")
  workDir.mkdirs()
  private val workEnv = WorkEnv(path = workDir.getPath)

  private val engine = ConnectorConfig(
    name = "duckdb",
    `type` = "duckdb",
    default = true,
    catalog = Some("memory"),
    schema = Some("main")
  )

  private val profile       = Profile(name = "test", connectors = Seq(engine))
  private val provider      = ConnectorProvider(workEnv)
  private val threadManager = ThreadManager()
  private val sessions      = ScriptRunnerSessions(
    workEnv,
    WvletScriptRunnerConfig(
      interactive = false,
      profile = profile,
      catalog = engine.catalog,
      schema = engine.schema
    ),
    provider,
    threadManager,
    TaskQueue(InMemoryItemStore()),
    ScriptRunnerSessions.DefaultIdleTimeout
  )

  override def afterAll: Unit =
    sessions.close()
    threadManager.close()
    provider.close()

  /** Records events; STARTED blocks until the query is released, holding its worker slot */
  private class GateListener extends QueryEventListener:
    val events                   = ConcurrentLinkedQueue[QueryEvent]()
    private val gates            = ConcurrentHashMap[String, CountDownLatch]()
    private def gate(id: String) = gates.computeIfAbsent(id, _ => CountDownLatch(1))

    override def onEvent(e: QueryEvent): Unit =
      events.add(e)
      if e.eventType == QueryEventType.STARTED then
        gate(e.queryId).await(30, TimeUnit.SECONDS)

    def release(queryId: String): Unit = gate(queryId).countDown()
    def releaseAll(): Unit             = gates.values().asScala.foreach(_.countDown())

    def started(queryId: String): Boolean = eventsOf(queryId).contains(QueryEventType.STARTED)
    def eventsOf(queryId: String): List[QueryEventType] =
      events.asScala.filter(_.queryId == queryId).map(_.eventType).toList

  private def withService[A](config: QueryManagerConfig, pool: MemoryPool = MemoryPool())(
      body: (QueryService, GateListener) => A
  ): A =
    val listener = GateListener()
    val service  = QueryService(sessions, config, listener, QueryIdGenerator(), pool)
    try
      body(service, listener)
    finally
      listener.releaseAll()
      service.close()

  private def statusOf(service: QueryService, queryId: String): QueryStatus =
    service.fetchNext(QueryInfoRequest(queryId, pageToken = "0")).status

  private def eventually(cond: => Boolean, timeoutMillis: Long = 30000): Unit =
    val deadline = System.currentTimeMillis() + timeoutMillis
    while !cond && System.currentTimeMillis() < deadline do
      Thread.sleep(20)
    cond shouldBe true

  private def awaitFinished(service: QueryService, queryId: String): QueryInfo =
    eventually(statusOf(service, queryId).isFinished)
    service.fetchNext(QueryInfoRequest(queryId, pageToken = "2"))

  test("keep a query queued until a running slot frees") {
    withService(QueryManagerConfig().withMaxRunningQueries(1)) { (service, listener) =>
      val first  = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("s1")))
      val second = service.enqueue(QueryRequest(query = "select 2", sessionId = Some("s2")))
      eventually(listener.started(first.queryId))
      statusOf(service, second.queryId) shouldBe QueryStatus.QUEUED
      service.stats shouldBe QueryServiceStats(running = 1, queued = 1, finished = 0)

      listener.release(first.queryId)
      eventually(listener.started(second.queryId))
      listener.release(second.queryId)
      awaitFinished(service, second.queryId).status shouldBe QueryStatus.FINISHED
    }
  }

  test("hold queued queries while the engine memory pool is over its capacity") {
    val pool = MemoryPool(capacityBytes = 100)
    withService(QueryManagerConfig().withMaxRunningQueries(2), pool) { (service, listener) =>
      // Memory a running query holds
      val held = pool.newQuery()
      held.reserve(200)
      // With nothing running, a query starts anyway: only running queries release memory
      val first = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("s1")))
      eventually(listener.started(first.queryId))
      // A free slot, but the pool is over its capacity
      val second = service.enqueue(QueryRequest(query = "select 2", sessionId = Some("s2")))
      Thread.sleep(200)
      statusOf(service, second.queryId) shouldBe QueryStatus.QUEUED
      service.stats shouldBe QueryServiceStats(running = 1, queued = 1, finished = 0)

      // The memory is released, and the first query's completion dispatches the second
      held.close()
      listener.release(first.queryId)
      eventually(listener.started(second.queryId))
      listener.release(second.queryId)
      awaitFinished(service, second.queryId).status shouldBe QueryStatus.FINISHED
    }
  }

  test("kill the largest query when the pool stays over its capacity without new reservations") {
    val pool = QueryManagerConfig()
      .withMemoryPoolSize(100)
      .withLowMemoryKillerDelay(300.millis)
      .newMemoryPool()
    // Over the capacity, but not yet for the delay: only the service's timer can kill
    val big   = pool.newQuery()
    val small = pool.newQuery()
    small.reserve(10)
    big.reserve(150)
    big.isKilled shouldBe false
    withService(QueryManagerConfig(), pool) { (_, _) =>
      eventually(big.isKilled)
      small.isKilled shouldBe false
    }
    big.close()
    small.close()
  }

  test("parse low-memory killer delays") {
    QueryManagerConfig.parseKillerDelay("5s") shouldBe Some(5.seconds)
    QueryManagerConfig.parseKillerDelay("500ms") shouldBe Some(500.millis)
    QueryManagerConfig.parseKillerDelay("OFF") shouldBe None
    Seq("soon", "-1s", "Inf").foreach { bad =>
      intercept[WvletLangException] {
        QueryManagerConfig.parseKillerDelay(bad)
      }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
    QueryManagerConfig().newMemoryPool().killDelay shouldBe Some(5.seconds)
    QueryManagerConfig().noLowMemoryKiller().newMemoryPool().killDelay shouldBe None
    intercept[WvletLangException] {
      QueryManagerConfig.parseKillerDelay("1000000d")
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    intercept[IllegalArgumentException] {
      QueryManagerConfig().withLowMemoryKillerDelay(-1.second)
    }
  }

  test("parse memory sizes of the server options") {
    QueryManagerConfig.parseBytes("512MB") shouldBe 512L * 1024 * 1024
    QueryManagerConfig.parseBytes("4gb") shouldBe 4L * 1024 * 1024 * 1024
    QueryManagerConfig.parseBytes("1.5G") shouldBe 3L * 512 * 1024 * 1024
    QueryManagerConfig.parseBytes("2048") shouldBe 2048L
    Seq("lots", "0", "1.5", "99999999TB", "-1GB").foreach { bad =>
      intercept[WvletLangException] {
        QueryManagerConfig.parseBytes(bad)
      }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
    val pool = QueryManagerConfig().withMemoryPoolSize(1024).withMaxQueryMemory(64).newMemoryPool()
    pool.capacityBytes shouldBe 1024L
    pool.newQuery().limitBytes shouldBe 64L
  }

  test("reject submissions past the queue limit") {
    withService(QueryManagerConfig().withMaxRunningQueries(1).withMaxQueuedQueries(1)) {
      (service, listener) =>
        val first = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("s1")))
        eventually(listener.started(first.queryId))
        service.enqueue(QueryRequest(query = "select 2", sessionId = Some("s2")))
        intercept[WvletLangException] {
          service.enqueue(QueryRequest(query = "select 3", sessionId = Some("s3")))
        }.statusCode shouldBe StatusCode.RESOURCE_EXHAUSTED
    }
  }

  test("run a session's queries in order while other sessions use free slots") {
    withService(QueryManagerConfig().withMaxRunningQueries(2)) { (service, listener) =>
      val a1 = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("a")))
      val a2 = service.enqueue(QueryRequest(query = "select 2", sessionId = Some("a")))
      val b1 = service.enqueue(QueryRequest(query = "select 3", sessionId = Some("b")))
      // a2 waits for a1 without holding the second slot, which b1 takes
      eventually(listener.started(a1.queryId) && listener.started(b1.queryId))
      statusOf(service, a2.queryId) shouldBe QueryStatus.QUEUED

      listener.release(a1.queryId)
      eventually(listener.started(a2.queryId))
      listener.releaseAll()
      Seq(a1, a2, b1).foreach(r =>
        awaitFinished(service, r.queryId).status shouldBe QueryStatus.FINISHED
      )
    }
  }

  test("drop a cancelled queued query without starting it") {
    withService(QueryManagerConfig().withMaxRunningQueries(1)) { (service, listener) =>
      val first  = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("s1")))
      val second = service.enqueue(QueryRequest(query = "select 2", sessionId = Some("s2")))
      eventually(listener.started(first.queryId))

      service.cancel(second.queryId).status shouldBe QueryStatus.CANCELED
      service.stats.queued shouldBe 0
      listener.eventsOf(second.queryId) shouldBe
        List(QueryEventType.CREATED, QueryEventType.COMPLETED)

      listener.release(first.queryId)
      awaitFinished(service, first.queryId).status shouldBe QueryStatus.FINISHED
      listener.started(second.queryId) shouldBe false
    }
  }

  test("free the slot of a cancelled running query") {
    withService(QueryManagerConfig().withMaxRunningQueries(1)) { (service, listener) =>
      val first  = service.enqueue(QueryRequest(query = "select 1", sessionId = Some("s1")))
      val second = service.enqueue(QueryRequest(query = "select 2", sessionId = Some("s2")))
      eventually(listener.started(first.queryId))
      service.cancel(first.queryId).status shouldBe QueryStatus.CANCELED
      // The interrupt ends the blocked STARTED event, so the worker returns and frees the slot
      eventually(listener.started(second.queryId))
      listener.release(second.queryId)
      awaitFinished(service, second.queryId).status shouldBe QueryStatus.FINISHED
      statusOf(service, first.queryId) shouldBe QueryStatus.CANCELED
    }
  }

  test("report created, started, and completed events with stats") {
    withService(QueryManagerConfig()) { (service, listener) =>
      val ok = service.enqueue(QueryRequest(query = "select 1 as x"))
      listener.release(ok.queryId)
      awaitFinished(service, ok.queryId)
      eventually(listener.eventsOf(ok.queryId).size == 3)
      listener.eventsOf(ok.queryId) shouldBe
        List(QueryEventType.CREATED, QueryEventType.STARTED, QueryEventType.COMPLETED)

      val events    = listener.events.asScala.filter(_.queryId == ok.queryId).toList
      val created   = events.head
      val completed = events.last
      created.query shouldBe Some("select 1 as x")
      completed.query shouldBe None
      completed.status shouldBe QueryStatus.FINISHED
      completed.resultRows shouldBe Some(1)
      completed.queuedMillis.isDefined shouldBe true
      completed.runMillis.isDefined shouldBe true

      val bad = service.enqueue(QueryRequest(query = "from table_that_does_not_exist_xyz"))
      listener.release(bad.queryId)
      awaitFinished(service, bad.queryId)
      eventually(listener.eventsOf(bad.queryId).size == 3)
      val failed = listener.events.asScala.filter(_.queryId == bad.queryId).last
      failed.status shouldBe QueryStatus.FAILED
      failed.errorMessage.isDefined shouldBe true
    }
  }

  test("evict finished queries beyond the count limit, oldest first") {
    withService(QueryManagerConfig().withMaxFinishedQueries(1)) { (service, listener) =>
      val first = service.enqueue(QueryRequest(query = "select 1"))
      listener.release(first.queryId)
      awaitFinished(service, first.queryId)

      val second = service.enqueue(QueryRequest(query = "select 2"))
      listener.release(second.queryId)
      awaitFinished(service, second.queryId)
      eventually(service.stats.finished == 1)

      intercept[WvletLangException] {
        statusOf(service, first.queryId)
      }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
  }

  test("evict finished queries past the retention") {
    withService(QueryManagerConfig().withFinishedQueryRetention(1.millis)) { (service, listener) =>
      val q = service.enqueue(QueryRequest(query = "select 1"))
      listener.release(q.queryId)
      eventually(service.stats.running == 0 && listener.eventsOf(q.queryId).size == 3)
      Thread.sleep(10)
      // Eviction runs on the next submission
      val next = service.enqueue(QueryRequest(query = "select 2"))
      listener.release(next.queryId)
      intercept[WvletLangException] {
        statusOf(service, q.queryId)
      }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
  }

  test("return the existing query for a resubmitted request id") {
    withService(QueryManagerConfig()) { (service, listener) =>
      val request = QueryRequest(query = "select 1")
      val first   = service.enqueue(request)
      val again   = service.enqueue(request)
      again.queryId shouldBe first.queryId
      listener.release(first.queryId)
      awaitFinished(service, first.queryId)
      service.enqueue(request).queryId shouldBe first.queryId
    }
  }

end QueryManagerTest

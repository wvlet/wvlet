package wvlet.lang.server

import wvlet.lang.BuildInfo
import wvlet.lang.api.v1.frontend.ClusterInfo
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.frontend.HeapInfo
import wvlet.lang.api.v1.frontend.MemoryPoolInfo
import wvlet.lang.api.v1.frontend.QueryCounts
import wvlet.lang.api.v1.frontend.ThreadPoolLoad
import wvlet.lang.api.v1.query.QueryDetail
import wvlet.lang.api.v1.query.QueryDetailRequest
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryList
import wvlet.lang.api.v1.query.QueryListRequest
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.exec.TaskRunner
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ElapsedTime

import java.time.Instant

class FrontendApiImpl(queryService: QueryService, workEnv: WorkEnv)
    extends FrontendApi
    with LogSupport:
  private val startTimeNs = System.nanoTime()
  private val startedAt   = Instant.now()

  override def status: ServerStatus =
    val stats = queryService.stats
    ServerStatus(
      upTime = ElapsedTime.nanosSince(startTimeNs),
      runningQueries = stats.running,
      queuedQueries = stats.queued
    )

  override def submitQuery(request: QueryRequest): QueryResponse =
    debug(s"Received:\n${request}")
    val resp = queryService.enqueue(request)
    resp

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo =
    debug(request)
    queryService.fetchNext(request)

  override def cancelQuery(request: QueryCancelRequest): QueryInfo =
    debug(request)
    queryService.cancel(request.queryId)

  override def listQueries(request: QueryListRequest): QueryList = queryService.list(request)

  override def getQueryDetail(request: QueryDetailRequest): QueryDetail = queryService.detail(
    request.queryId
  )

  override def getClusterInfo: ClusterInfo =
    val stats                              = queryService.stats
    val config                             = queryService.config
    val pool                               = queryService.memoryPool
    val engine                             = TaskRunner.default.load
    val rt                                 = Runtime.getRuntime
    def bounded(bytes: Long): Option[Long] = Option.when(bytes != Long.MaxValue)(bytes)
    ClusterInfo(
      version = BuildInfo.version,
      serverId = queryService.serverId,
      startedAt = startedAt,
      upTimeMillis = (System.nanoTime() - startTimeNs) / 1_000_000L,
      processors = rt.availableProcessors(),
      queries = QueryCounts(
        running = stats.running,
        queued = stats.queued,
        finished = stats.finished,
        maxRunning = config.maxRunningQueries,
        maxQueued = config.maxQueuedQueries
      ),
      memoryPool = MemoryPoolInfo(
        reservedBytes = pool.reservedBytes,
        capacityBytes = bounded(pool.capacityBytes),
        queryLimitBytes = bounded(pool.queryLimitBytes),
        openQueries = pool.openQueries,
        killedQueries = pool.killedQueries,
        lowMemoryKillerDelayMillis = pool.killDelay.map(_.toMillis)
      ),
      queryWorkers = ThreadPoolLoad(
        threads = config.maxRunningQueries,
        busy = stats.running,
        waitingTasks = stats.queued
      ),
      engineThreads = ThreadPoolLoad(engine.threads, engine.busy, engine.waitingTasks),
      heap = HeapInfo(
        usedBytes = rt.totalMemory() - rt.freeMemory(),
        committedBytes = rt.totalMemory(),
        maxBytes = rt.maxMemory()
      )
    )

  end getClusterInfo

end FrontendApiImpl

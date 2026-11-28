package wvlet.lang.server

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.MemoryPool

import scala.concurrent.duration.*

/**
  * Admission and retention limits of [[QueryService]].
  *
  * @param maxRunningQueries
  *   Queries running at once; others wait in the queue
  * @param maxQueuedQueries
  *   Queries waiting for a slot; submissions past it fail with RESOURCE_EXHAUSTED
  * @param finishedQueryRetention
  *   How long a finished query (and its result rows) stays readable by id
  * @param maxFinishedQueries
  *   At most this many finished queries are kept; the oldest are dropped first
  * @param memoryPoolSize
  *   Bytes the in-process engine's queries may hold together before queued queries wait; None for
  *   70% of the maximum heap
  * @param maxQueryMemory
  *   Bytes one in-process engine query may hold before it fails with RESOURCE_EXHAUSTED; None for
  *   no limit
  * @param lowMemoryKillerDelay
  *   How long the memory pool may stay over its capacity, with more than one query running, before
  *   the query holding the most memory fails with RESOURCE_EXHAUSTED; None never kills
  */
case class QueryManagerConfig(
    maxRunningQueries: Int = QueryManagerConfig.defaultMaxRunningQueries,
    maxQueuedQueries: Int = 1000,
    finishedQueryRetention: FiniteDuration = 1.hour,
    maxFinishedQueries: Int = 1000,
    memoryPoolSize: Option[Long] = None,
    maxQueryMemory: Option[Long] = None,
    lowMemoryKillerDelay: Option[FiniteDuration] = Some(5.seconds)
):
  require(maxRunningQueries >= 1, s"maxRunningQueries must be >= 1: ${maxRunningQueries}")
  require(maxQueuedQueries >= 1, s"maxQueuedQueries must be >= 1: ${maxQueuedQueries}")
  require(maxFinishedQueries >= 0, s"maxFinishedQueries must be >= 0: ${maxFinishedQueries}")

  def withMaxRunningQueries(n: Int): QueryManagerConfig = copy(maxRunningQueries = n)
  def withMaxQueuedQueries(n: Int): QueryManagerConfig  = copy(maxQueuedQueries = n)
  def withFinishedQueryRetention(d: FiniteDuration): QueryManagerConfig = copy(
    finishedQueryRetention = d
  )

  def withMaxFinishedQueries(n: Int): QueryManagerConfig  = copy(maxFinishedQueries = n)
  def withMemoryPoolSize(bytes: Long): QueryManagerConfig = copy(memoryPoolSize = Some(bytes))
  def noMemoryPoolSize(): QueryManagerConfig              = copy(memoryPoolSize = None)
  def withMaxQueryMemory(bytes: Long): QueryManagerConfig = copy(maxQueryMemory = Some(bytes))
  def noMaxQueryMemory(): QueryManagerConfig              = copy(maxQueryMemory = None)
  def withLowMemoryKillerDelay(d: FiniteDuration): QueryManagerConfig = copy(lowMemoryKillerDelay =
    Some(d)
  )

  def noLowMemoryKiller(): QueryManagerConfig = copy(lowMemoryKillerDelay = None)

  /** The memory pool these limits describe, for the server's engines and admission */
  def newMemoryPool(): MemoryPool = MemoryPool(
    capacityBytes = memoryPoolSize.getOrElse(QueryManagerConfig.defaultMemoryPoolSize),
    queryLimitBytes = maxQueryMemory.getOrElse(Long.MaxValue),
    killDelay = lowMemoryKillerDelay
  )

end QueryManagerConfig

object QueryManagerConfig:
  /** Half the processors (at least one): the in-process engine parallelizes each query itself */
  def defaultMaxRunningQueries: Int = (Runtime.getRuntime.availableProcessors() / 2).max(1)

  /** 70% of the maximum heap: the rest is for everything the engine doesn't count */
  def defaultMemoryPoolSize: Long = (Runtime.getRuntime.maxMemory() * 0.7).toLong

  private val sizePattern = """(?i)\s*(\d+(?:\.\d+)?)\s*([KMGT]?)B?\s*""".r

  /**
    * Bytes of a size such as `512MB`, `4GB`, `1.5g`, or `1048576` (units are powers of 1024).
    * Throws INVALID_ARGUMENT for anything else, including zero, sizes past Long.MaxValue, and a
    * fraction without a unit (`1.5` is likely meant as GB, not 1 byte)
    */
  def parseBytes(s: String): Long =
    def invalid = StatusCode
      .INVALID_ARGUMENT
      .newException(s"Invalid memory size: ${s} (use e.g. 512MB or 4GB)")
    s match
      case sizePattern(n, unit) if unit.nonEmpty || !n.contains('.') =>
        val scale =
          unit.toUpperCase match
            case "K" =>
              1L << 10
            case "M" =>
              1L << 20
            case "G" =>
              1L << 30
            case "T" =>
              1L << 40
            case _ =>
              1L
        val bytes = BigDecimal(n) * scale
        if bytes < 1 || bytes > Long.MaxValue then
          throw invalid
        bytes.toLong
      case _ =>
        throw invalid

  /**
    * The low-memory killer delay of a server option: a duration such as `5s`, `500ms`, or `1min`,
    * or `off` to never kill. Throws INVALID_ARGUMENT for anything else, including negative values
    */
  def parseKillerDelay(s: String): Option[FiniteDuration] =
    if s.trim.equalsIgnoreCase("off") then
      None
    else
      val d =
        try
          Duration(s.trim)
        catch
          case _: NumberFormatException =>
            Duration.Undefined
      d match
        case f: FiniteDuration if f >= Duration.Zero =>
          Some(f)
        case _ =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Invalid delay: ${s} (use e.g. 5s, 500ms, or off)")

end QueryManagerConfig

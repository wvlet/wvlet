package wvlet.lang.server

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
  */
case class QueryManagerConfig(
    maxRunningQueries: Int = QueryManagerConfig.defaultMaxRunningQueries,
    maxQueuedQueries: Int = 1000,
    finishedQueryRetention: FiniteDuration = 1.hour,
    maxFinishedQueries: Int = 1000
):
  require(maxRunningQueries >= 1, s"maxRunningQueries must be >= 1: ${maxRunningQueries}")
  require(maxQueuedQueries >= 0, s"maxQueuedQueries must be >= 0: ${maxQueuedQueries}")
  require(maxFinishedQueries >= 0, s"maxFinishedQueries must be >= 0: ${maxFinishedQueries}")

  def withMaxRunningQueries(n: Int): QueryManagerConfig = copy(maxRunningQueries = n)
  def withMaxQueuedQueries(n: Int): QueryManagerConfig  = copy(maxQueuedQueries = n)
  def withFinishedQueryRetention(d: FiniteDuration): QueryManagerConfig = copy(
    finishedQueryRetention = d
  )

  def withMaxFinishedQueries(n: Int): QueryManagerConfig = copy(maxFinishedQueries = n)

object QueryManagerConfig:
  /** Half the processors (at least one): the in-process engine parallelizes each query itself */
  def defaultMaxRunningQueries: Int = (Runtime.getRuntime.availableProcessors() / 2).max(1)

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.activity

import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Condition
import scala.jdk.CollectionConverters.*

/**
  * A pending task waiting to be dispatched to a worker
  *
  * @param activityType
  *   The activity type this task requires
  * @param invocationId
  *   Stable invocation identity (same across retries)
  * @param attemptId
  *   Unique attempt identity for fencing
  * @param input
  *   Input parameters for the activity
  * @param config
  *   Execution configuration (timeout, heartbeat interval)
  * @param enqueuedAtMillis
  *   When the task was enqueued
  */
case class PendingTask(
    activityType: String,
    invocationId: String,
    attemptId: String,
    input: Map[String, Any],
    config: ActivityConfig,
    enqueuedAtMillis: Long
)

/**
  * Tracks an active attempt dispatched to a worker
  *
  * @param attemptId
  *   The attempt identity
  * @param invocationId
  *   The invocation identity
  * @param itemId
  *   The item record id in the store
  * @param workerId
  *   The worker that claimed this attempt
  * @param leaseExpiresAtMillis
  *   When the lease expires if no heartbeat is received
  * @param cancelRequested
  *   Whether the orchestrator has requested cancellation
  */
case class ActiveAttempt(
    attemptId: String,
    invocationId: String,
    itemId: String,
    workerId: String,
    leaseExpiresAtMillis: Long,
    cancelRequested: Boolean = false
)

/**
  * Server-side task queue that coordinates the dispatch of activity items to remote workers via the
  * poll/result/heartbeat protocol. The queue holds pending tasks in memory and tracks active
  * attempts. Persistent state is delegated to the [[ItemStore]].
  *
  * Thread-safe: all methods may be called concurrently from HTTP request threads
  */
class TaskQueue(store: ItemStore, defaultLeaseMillis: Long = 60_000L) extends LogSupport:

  private val pending = ConcurrentLinkedQueue[PendingTask]()
  private val active  = ConcurrentHashMap[String, ActiveAttempt]()

  private val lock      = ReentrantLock()
  private val taskReady = lock.newCondition()

  /**
    * Enqueue a task for dispatch to a remote worker. The caller (e.g. a flow dispatcher) provides
    * the task details; the queue makes it available to polling workers.
    */
  def enqueue(task: PendingTask): Unit =
    pending.add(task)
    lock.lock()
    try taskReady.signalAll()
    finally lock.unlock()

  /**
    * Poll for a task matching any of the given activity types. Blocks up to `timeoutMillis` if no
    * task is immediately available (long-poll). Returns None if no matching task is found within
    * the timeout.
    */
  def poll(
      activityTypes: List[String],
      workerId: String,
      timeoutMillis: Long
  ): Option[PendingTask] =
    val typeSet  = activityTypes.toSet
    val deadline = System.currentTimeMillis() + timeoutMillis

    def tryTake(): Option[PendingTask] =
      val it = pending.iterator()
      while it.hasNext do
        val task = it.next()
        if typeSet.contains(task.activityType) then
          it.remove()
          val now     = System.currentTimeMillis()
          val attempt = ActiveAttempt(
            attemptId = task.attemptId,
            invocationId = task.invocationId,
            itemId = task.invocationId,
            workerId = workerId,
            leaseExpiresAtMillis = now + defaultLeaseMillis
          )
          active.put(task.attemptId, attempt)
          store.saveItem(
            ItemRecord(
              itemId = task.invocationId,
              runId = "",
              stageName = "",
              itemKey = task.invocationId,
              state = ItemState.Running.stateName,
              attemptId = task.attemptId,
              attempts = 1,
              startedAtMillis = Some(now),
              leaseExpiresAtMillis = Some(now + defaultLeaseMillis)
            )
          )
          return Some(task)
      None
    end tryTake

    lock.lock()
    try
      tryTake() match
        case some @ Some(_) =>
          return some
        case None =>
          if timeoutMillis <= 0 then
            return None

      var remaining = deadline - System.currentTimeMillis()
      while remaining > 0 do
        taskReady.await(remaining, TimeUnit.MILLISECONDS)
        tryTake() match
          case some @ Some(_) =>
            return some
          case None =>
            remaining = deadline - System.currentTimeMillis()
      None
    finally
      lock.unlock()

  end poll

  /**
    * Report the result of a task attempt. Validates the attemptId against the active attempts for
    * fencing.
    *
    * @return
    *   true if the result was accepted, false if the attempt was stale or unknown
    */
  def reportResult(
      attemptId: String,
      status: String,
      output: Option[String],
      error: Option[String]
  ): Boolean =
    active.remove(attemptId) match
      case null =>
        debug(s"Rejecting result for unknown attempt: ${attemptId}")
        false
      case attempt =>
        val state =
          status match
            case "completed" =>
              ItemState.Succeeded.stateName
            case "failed" =>
              ItemState.Failed.stateName
            case "cancelled" =>
              ItemState.Cancelled.stateName
            case other =>
              warn(s"Unknown result status '${other}' for attempt ${attemptId}, treating as failed")
              ItemState.Failed.stateName
        val accepted = store.reportResult(
          attempt.itemId,
          attemptId,
          state,
          output,
          error,
          System.currentTimeMillis()
        )
        if !accepted then
          debug(s"Store rejected result for attempt ${attemptId} (fenced)")
        accepted

  /**
    * Renew the liveness lease of a running attempt.
    *
    * @return
    *   "continue" if the worker should keep working, "cancel" if the orchestrator wants the attempt
    *   to stop, or "unknown" if the attempt is not tracked
    */
  def heartbeat(attemptId: String): String =
    active.computeIfPresent(
      attemptId,
      (_, current) =>
        current.copy(leaseExpiresAtMillis = System.currentTimeMillis() + defaultLeaseMillis)
    ) match
      case null =>
        "unknown"
      case attempt =>
        store.refreshItemLease(attempt.itemId, attemptId, attempt.leaseExpiresAtMillis)
        if attempt.cancelRequested then
          "cancel"
        else
          "continue"

  /**
    * Request cancellation of an active attempt. The next heartbeat response will signal the worker
    * to stop.
    */
  def requestCancel(attemptId: String): Unit = active.computeIfPresent(
    attemptId,
    (_, current) => current.copy(cancelRequested = true)
  )

  /** The number of tasks waiting to be dispatched */
  def pendingCount: Int = pending.size()

  /** The number of attempts currently being executed by workers */
  def activeCount: Int = active.size()

  /** All currently active attempts (for inspection) */
  def activeAttempts: List[ActiveAttempt] = active.values().asScala.toList

end TaskQueue

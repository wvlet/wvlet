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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/**
  * A pending task waiting to be dispatched to a worker
  *
  * @param activityType
  *   The activity type this task requires
  * @param invocationId
  *   Stable invocation identity (same across retries); the item id in the store
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
  * @param leaseMillis
  *   The lease duration granted by each heartbeat
  * @param cancelRequested
  *   Whether the orchestrator has requested cancellation
  */
case class ActiveAttempt(
    attemptId: String,
    invocationId: String,
    itemId: String,
    workerId: String,
    leaseExpiresAtMillis: Long,
    leaseMillis: Long,
    cancelRequested: Boolean = false
)

/**
  * Server-side task queue that coordinates the dispatch of activity items to remote workers via the
  * poll/result/heartbeat protocol. The [[ItemStore]] is the source of truth: enqueued tasks are
  * persisted as queued items, a poll claims an item atomically (queued -> running, fenced by the
  * attemptId), and results are fenced by the store.
  *
  * Recovery: constructing a queue reconciles the store. Queued items dispatched through a queue
  * return to the pending queue, and running or cancelling attempts claimed by a worker are adopted
  * with their persisted lease, so a live worker keeps heartbeating and reporting after a restart.
  * Nothing is re-executed.
  *
  * Lease expiry: an attempt whose worker stops heartbeating becomes uncertain once its lease
  * expires ([[expireLeases]]). Lost contact does not prove that the work stopped, so the attempt is
  * never retried automatically.
  *
  * In-process callers (e.g. a flow stage) dispatch through [[dispatch]] and receive the outcome as
  * a future. Thread-safe: all methods may be called concurrently from HTTP request threads
  *
  * @param store
  *   The item store holding queued, running, and finished attempts
  * @param defaultLeaseMillis
  *   The lease of an attempt whose task declares no heartbeat interval
  * @param clock
  *   The current time in milliseconds (injectable for testing lease expiry)
  */
class TaskQueue(
    val store: ItemStore,
    defaultLeaseMillis: Long = 60_000L,
    clock: () => Long = () => System.currentTimeMillis()
) extends AutoCloseable
    with LogSupport:

  private val pending = mutable.Queue.empty[PendingTask]
  private val active  = ConcurrentHashMap[String, ActiveAttempt]()

  // In-process callers awaiting the outcome of an attempt, keyed by attemptId
  private case class Waiter(result: CompletableFuture[ActivityResult], onHeartbeat: () => Unit)
  private val waiters = ConcurrentHashMap[String, Waiter]()

  // Guards `pending` and orders claims against cancellation
  private val lock      = ReentrantLock()
  private val taskReady = lock.newCondition()

  @volatile
  private var sweeper: Option[ScheduledExecutorService] = None

  recover()

  /**
    * The lease granted to an attempt: three heartbeat intervals when the task declares one, so a
    * single delayed heartbeat does not expire it
    */
  private def leaseMillisFor(config: ActivityConfig): Long = config
    .heartbeatMillis
    .map(_ * 3)
    .getOrElse(defaultLeaseMillis)

  private def withLock[A](body: => A): A =
    lock.lock()
    try body
    finally lock.unlock()

  /** Reconcile in-memory dispatch state with the store after a restart */
  private def recover(): Unit =
    val now       = clock()
    var requeued  = 0
    var adopted   = 0
    var cancelled = 0
    store
      .listActiveItems()
      .foreach { r =>
        (ItemState.fromString(r.state), r.activityType, r.workerId) match
          case (Some(ItemState.Queued), Some(activityType), _) =>
            pending.enqueue(
              PendingTask(
                activityType = activityType,
                invocationId = r.itemId,
                attemptId = r.attemptId,
                input = r.input.map(ActivityJson.decode).getOrElse(Map.empty),
                config = ActivityConfig(r.timeoutMillis, r.heartbeatMillis),
                enqueuedAtMillis = now
              )
            )
            requeued += 1
          case (Some(ItemState.Running | ItemState.Cancelling), Some(_), Some(workerId)) =>
            active.put(
              r.attemptId,
              ActiveAttempt(
                attemptId = r.attemptId,
                invocationId = r.itemId,
                itemId = r.itemId,
                workerId = workerId,
                leaseExpiresAtMillis = r.leaseExpiresAtMillis.getOrElse(now),
                leaseMillis = leaseMillisFor(ActivityConfig(r.timeoutMillis, r.heartbeatMillis)),
                cancelRequested = r.state == ItemState.Cancelling.stateName
              )
            )
            adopted += 1
          case (Some(ItemState.Cancelling), Some(_), None) =>
            // Cancellation was requested before any worker claimed the task: it never ran
            store.reportResult(
              r.itemId,
              r.attemptId,
              ItemState.Cancelled.stateName,
              None,
              None,
              now
            )
            cancelled += 1
          case _ =>
          // Not dispatched through a task queue (e.g. a batch running a local executor)
      }
    if requeued + adopted + cancelled > 0 then
      info(
        s"Recovered task queue: ${requeued} queued, ${adopted} in-flight, ${cancelled} cancelled"
      )

  end recover

  /**
    * Enqueue a task for dispatch to a remote worker. The task is persisted as a queued item; an
    * existing record of the item (e.g. created by a batch) keeps its run, stage, and key.
    */
  def enqueue(task: PendingTask): Unit = withLock {
    val input  = Some(ActivityJson.encode(task.input))
    val record =
      store.getItem(task.invocationId) match
        case Some(r) =>
          r.copy(
            state = ItemState.Queued.stateName,
            attemptId = task.attemptId,
            attempts = r.attempts.max(1),
            output = None,
            error = None,
            startedAtMillis = None,
            finishedAtMillis = None,
            leaseExpiresAtMillis = None,
            activityType = Some(task.activityType),
            input = input,
            timeoutMillis = task.config.timeoutMillis,
            heartbeatMillis = task.config.heartbeatMillis,
            workerId = None
          )
        case None =>
          ItemRecord(
            itemId = task.invocationId,
            runId = "",
            stageName = "",
            itemKey = task.invocationId,
            state = ItemState.Queued.stateName,
            attemptId = task.attemptId,
            attempts = 1,
            activityType = Some(task.activityType),
            input = input,
            timeoutMillis = task.config.timeoutMillis,
            heartbeatMillis = task.config.heartbeatMillis
          )
    store.saveItem(record)
    pending.enqueue(task)
    taskReady.signalAll()
  }

  /**
    * Enqueue a task and return a future completed with its outcome: the worker's result, an
    * uncertain result when the lease expires, or cancelled when it is cancelled before dispatch. If
    * the attempt is already known (e.g. recovered after a restart), the future awaits it without
    * enqueueing the task again.
    *
    * @param onHeartbeat
    *   Called on every worker heartbeat of the attempt, to propagate liveness to the caller
    */
  def dispatch(task: PendingTask, onHeartbeat: () => Unit): CompletableFuture[ActivityResult] =
    val waiter = Waiter(CompletableFuture[ActivityResult](), onHeartbeat)
    waiters.put(task.attemptId, waiter)
    withLock {
      if !isKnown(task.attemptId) then
        enqueue(task)
    }
    waiter.result

  /** Whether the attempt is waiting for a worker or being executed by one */
  def isKnown(attemptId: String): Boolean = isPending(attemptId) || active.containsKey(attemptId)

  /** Whether the attempt is still waiting for a worker */
  def isPending(attemptId: String): Boolean = withLock {
    pending.exists(_.attemptId == attemptId)
  }

  private def complete(attemptId: String, result: ActivityResult): Unit = Option(
    waiters.remove(attemptId)
  ).foreach(_.result.complete(result))

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

    // Take the first matching task the store lets this worker claim. A task whose item was
    // cancelled or reassigned since it was enqueued fails the claim and is dropped
    def tryTake(): Option[PendingTask] =
      var claimed: Option[PendingTask] = None
      while claimed.isEmpty && pending.exists(t => typeSet.contains(t.activityType)) do
        val task        = pending.dequeueFirst(t => typeSet.contains(t.activityType)).get
        val now         = clock()
        val leaseMillis = leaseMillisFor(task.config)
        if store.claimItem(task.invocationId, task.attemptId, workerId, now, now + leaseMillis) then
          active.put(
            task.attemptId,
            ActiveAttempt(
              attemptId = task.attemptId,
              invocationId = task.invocationId,
              itemId = task.invocationId,
              workerId = workerId,
              leaseExpiresAtMillis = now + leaseMillis,
              leaseMillis = leaseMillis
            )
          )
          claimed = Some(task)
        else
          debug(s"Dropping task ${task.attemptId}: its item is no longer queued for this attempt")
      claimed

    withLock {
      var result    = tryTake()
      var remaining = deadline - System.currentTimeMillis()
      while result.isEmpty && remaining > 0 do
        taskReady.await(remaining, TimeUnit.MILLISECONDS)
        result = tryTake()
        remaining = deadline - System.currentTimeMillis()
      result
    }

  end poll

  /**
    * Report the result of a task attempt. The store fences the result: it is rejected when the
    * attempt is stale or unknown, or the item already has a final outcome.
    *
    * @return
    *   true if the result was accepted
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
        val (state, result) =
          status match
            case "completed" =>
              ItemState.Succeeded.stateName ->
                ActivityResult.Completed(output.map(ActivityJson.decode).getOrElse(Map.empty))
            case "failed" =>
              ItemState.Failed.stateName ->
                ActivityResult.Failed(error.getOrElse("Activity failed"))
            case "cancelled" =>
              ItemState.Cancelled.stateName -> ActivityResult.Cancelled
            case other =>
              warn(s"Unknown result status '${other}' for attempt ${attemptId}, treating as failed")
              ItemState.Failed.stateName ->
                ActivityResult.Failed(error.getOrElse(s"Unknown result status: ${other}"))
        val accepted = store.reportResult(attempt.itemId, attemptId, state, output, error, clock())
        if !accepted then
          debug(s"Store rejected result for attempt ${attemptId} (fenced)")
        // The waiter fences on its own side; it needs the worker's outcome either way
        complete(attemptId, result)
        accepted

  end reportResult

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
      (_, current) => current.copy(leaseExpiresAtMillis = clock() + current.leaseMillis)
    ) match
      case null =>
        "unknown"
      case attempt =>
        store.refreshItemLease(attempt.itemId, attemptId, attempt.leaseExpiresAtMillis)
        Option(waiters.get(attemptId)).foreach { w =>
          try
            w.onHeartbeat()
          catch
            case NonFatal(e) =>
              debug(s"Heartbeat listener of attempt ${attemptId} failed: ${e.getMessage}")
        }
        if attempt.cancelRequested then
          "cancel"
        else
          "continue"

  /**
    * Request cancellation of an attempt. A task still waiting for a worker is removed and recorded
    * as cancelled right away. A claimed attempt is marked cancelling; the next heartbeat response
    * signals the worker to stop, and the worker's report decides the final state.
    *
    * @return
    *   true if the cancellation is confirmed (the task never reached a worker)
    */
  def requestCancel(attemptId: String): Boolean = withLock {
    pending.dequeueFirst(_.attemptId == attemptId) match
      case Some(task) =>
        store.reportResult(
          task.invocationId,
          attemptId,
          ItemState.Cancelled.stateName,
          None,
          None,
          clock()
        )
        complete(attemptId, ActivityResult.Cancelled)
        true
      case None =>
        active.computeIfPresent(
          attemptId,
          (_, current) => current.copy(cancelRequested = true)
        ) match
          case null =>
            false
          case attempt =>
            store.markCancelling(attempt.itemId, attemptId)
            false
  }

  /**
    * Resolve attempts whose lease expired: each becomes uncertain, since the worker stopped
    * heartbeating but its work may still have taken effect.
    *
    * @return
    *   The attemptIds that expired
    */
  def expireLeases(): List[String] =
    val now = clock()
    active
      .values()
      .asScala
      .toList
      .filter(_.leaseExpiresAtMillis < now)
      .filter(a => active.remove(a.attemptId, a))
      .map { a =>
        val reason = s"Lease expired: worker ${a.workerId} stopped heartbeating"
        warn(s"Attempt ${a.attemptId} of item ${a.itemId} is uncertain. ${reason}")
        store.reportResult(
          a.itemId,
          a.attemptId,
          ItemState.Uncertain.stateName,
          None,
          Some(reason),
          now
        )
        complete(a.attemptId, ActivityResult.Uncertain(reason))
        a.attemptId
      }

  /** Run [[expireLeases]] periodically on a daemon thread until the queue is closed */
  def startLeaseSweeper(intervalMillis: Long): Unit = synchronized {
    if sweeper.isEmpty then
      val executor = Executors.newSingleThreadScheduledExecutor { r =>
        val t = Thread(r, "wvlet-task-queue-lease-sweeper")
        t.setDaemon(true)
        t
      }
      executor.scheduleWithFixedDelay(
        () =>
          try
            expireLeases()
          catch
            case NonFatal(e) =>
              warn(s"Lease sweep failed: ${e.getMessage}")
        ,
        intervalMillis,
        intervalMillis,
        TimeUnit.MILLISECONDS
      )
      sweeper = Some(executor)
  }

  override def close(): Unit = synchronized {
    sweeper.foreach(_.shutdownNow())
    sweeper = None
  }

  /** The number of tasks waiting to be dispatched */
  def pendingCount: Int = withLock(pending.size)

  /** The number of attempts currently being executed by workers */
  def activeCount: Int = active.size()

  /** All currently active attempts (for inspection) */
  def activeAttempts: List[ActiveAttempt] = active.values().asScala.toList

end TaskQueue

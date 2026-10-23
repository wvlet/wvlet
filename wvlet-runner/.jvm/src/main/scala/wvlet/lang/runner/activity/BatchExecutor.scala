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
import wvlet.uni.util.ULID

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/**
  * Configuration for a batch (fan-out) execution
  *
  * @param concurrency
  *   Maximum number of items that can execute concurrently within this batch. Excess items queue
  *   until a slot is freed by completion
  * @param timeoutMillis
  *   Per-item timeout. A timed-out item transitions to uncertain (not failed) and does not free its
  *   slot until the executor confirms termination or the lease expires
  * @param heartbeatMillis
  *   Per-item heartbeat interval. An item that produces no heartbeat within the interval is treated
  *   as a liveness failure
  * @param leaseTimeoutMillis
  *   Liveness lease for each item attempt. On restart, running items whose lease expired transition
  *   to uncertain
  * @param retryUncertain
  *   Whether to automatically retry items in the uncertain state on restart. Defaults to false for
  *   non-idempotent activities
  */
case class BatchConfig(
    concurrency: Int = 4,
    timeoutMillis: Option[Long] = None,
    heartbeatMillis: Option[Long] = None,
    leaseTimeoutMillis: Long = 60_000L,
    retryUncertain: Boolean = false
):
  def withConcurrency(n: Int): BatchConfig              = copy(concurrency = n)
  def withTimeoutMillis(millis: Long): BatchConfig      = copy(timeoutMillis = Some(millis))
  def withHeartbeatMillis(millis: Long): BatchConfig    = copy(heartbeatMillis = Some(millis))
  def withLeaseTimeoutMillis(millis: Long): BatchConfig = copy(leaseTimeoutMillis = millis)
  def withRetryUncertain(retry: Boolean): BatchConfig   = copy(retryUncertain = retry)
  def noTimeout(): BatchConfig                          = copy(timeoutMillis = None)
  def noHeartbeat(): BatchConfig                        = copy(heartbeatMillis = None)

/**
  * The result of executing a batch of items
  */
case class BatchResult(stageName: String, items: List[ItemRecord], cancelled: Boolean = false):
  def succeeded: List[ItemRecord] = items.filter(_.state == ItemState.Succeeded.stateName)
  def failed: List[ItemRecord]    = items.filter(_.state == ItemState.Failed.stateName)
  def uncertain: List[ItemRecord] = items.filter(_.state == ItemState.Uncertain.stateName)
  def hasErrors: Boolean          = items.exists(r =>
    r.state == ItemState.Failed.stateName || r.state == ItemState.Uncertain.stateName
  )

/**
  * Orchestrates a fan-out execution: one activity invocation per item, with queued admission and
  * bounded concurrency. Completed items admit the next queued item immediately — no waiting for the
  * slowest sibling.
  *
  * Thread model:
  *   - The caller thread drives the event loop (similar to [[wvlet.lang.runner.FlowExecutor]])
  *   - Worker threads execute activity invocations and post results to a queue
  *   - A timer thread enforces heartbeat and timeout deadlines
  *
  * On restart, the batch executor checks the item store for existing records: completed items are
  * skipped and queued items are re-admitted. An item that was in flight is awaited again when the
  * executor still tracks its attempt ([[ActivityExecutor.canResume]], e.g. a task-queue attempt
  * held by a remote worker); otherwise it transitions to uncertain instead of being re-executed.
  * This provides restart recovery without re-executing completed or possibly-running work.
  *
  * If the calling thread is interrupted (e.g. the flow is cancelled), queued items are cancelled
  * and running items receive a cancellation request before the interruption propagates.
  */
class BatchExecutor(
    executor: ActivityExecutor,
    store: ItemStore,
    config: BatchConfig = BatchConfig()
) extends LogSupport:

  @volatile
  private var cancelRequested = false

  /** Request cancellation of this batch. Stops new admission and cancels running items */
  def cancel(): Unit = cancelRequested = true

  /**
    * Execute the batch: one activity invocation per item key, returning when all items reach a
    * terminal state.
    *
    * @param runId
    *   The flow run id
    * @param stageName
    *   The stage name within the flow
    * @param activityType
    *   The activity type to invoke for each item
    * @param itemKeys
    *   The fan-out values (one per item)
    * @param baseInput
    *   Input parameters shared by all items (the stage's input data); each item additionally gets
    *   `item` bound to its key
    * @return
    *   The batch result with all item records
    */
  def execute(
      runId: String,
      stageName: String,
      activityType: String,
      itemKeys: List[String],
      baseInput: Map[String, Any] = Map.empty
  ): BatchResult =
    val now = System.currentTimeMillis()

    // Recover existing items from the store (restart path)
    val existingItems = store.listItems(runId, stageName).map(r => r.itemKey -> r).toMap
    // In-flight attempts the executor still tracks; they are awaited instead of re-executed
    val resumable = mutable.Set.empty[String]

    // Build the item list: reuse completed items, resume or mark uncertain in-flight items,
    // create new records for unseen keys
    val allItems = itemKeys.map { key =>
      val itemId = ItemRecord.itemId(runId, stageName, key)
      existingItems.get(key) match
        case Some(r) if r.isTerminal =>
          r
        case Some(r) if executor.canResume(r.attemptId) =>
          resumable += r.itemId
          r
        case Some(r) if r.state == ItemState.Queued.stateName =>
          r
        case Some(r) =>
          // The attempt was in flight when the batch stopped and nobody tracks it anymore: its
          // outcome is unknown, so it must not be re-executed blindly
          val reason =
            if r.isStaleAt(now) then
              "Lease expired while the batch was not running"
            else
              "Attempt was in flight when the batch restarted"
          val uncertain = r.copy(
            state = ItemState.Uncertain.stateName,
            error = Some(reason),
            finishedAtMillis = Some(now),
            leaseExpiresAtMillis = None
          )
          if store.reportResult(r.itemId, r.attemptId, uncertain.state, None, uncertain.error, now)
          then
            uncertain
          else
            store.getItem(r.itemId).getOrElse(uncertain)
        case None =>
          val record = ItemRecord(
            itemId = itemId,
            runId = runId,
            stageName = stageName,
            itemKey = key,
            state = ItemState.Queued.stateName,
            attemptId = ULID.newULIDString,
            attempts = 0
          )
          store.saveItem(record)
          record
      end match
    }

    // Split into already-terminal (skip) and pending (queue for execution)
    val pending = mutable
      .Queue
      .from(allItems.filterNot(r => ItemState.fromString(r.state).exists(_.isTerminal)))
    // Re-queue uncertain items if retry is enabled
    if config.retryUncertain then
      allItems
        .filter(_.state == ItemState.Uncertain.stateName)
        .foreach { r =>
          val retrying = r.copy(
            state = ItemState.Queued.stateName,
            attemptId = ULID.newULIDString,
            attempts = r.attempts,
            output = None,
            error = None,
            finishedAtMillis = None
          )
          store.saveItem(retrying)
          pending.enqueue(retrying)
        }

    if pending.isEmpty then
      return BatchResult(stageName, allItems)

    // Event types for the completion queue
    sealed trait BatchEvent
    case class ItemCompleted(itemId: String, attemptId: String, result: ActivityResult)
        extends BatchEvent
    case class HeartbeatMissed(itemId: String, attemptId: String) extends BatchEvent
    case class ItemTimedOut(itemId: String, attemptId: String)    extends BatchEvent

    val eventQueue = LinkedBlockingQueue[BatchEvent]()

    // Track running items
    val runningItems  = ConcurrentHashMap[String, ItemRecord]()
    val lastBeats     = ConcurrentHashMap[String, java.lang.Long]()
    val heartbeatJobs = ConcurrentHashMap[String, java.util.concurrent.Future[?]]()

    val threadFactory =
      new ThreadFactory:
        override def newThread(r: Runnable): Thread =
          val t = Thread(r, s"wvlet-batch-${stageName}")
          t.setDaemon(true)
          t

    val workerPool = Executors.newFixedThreadPool(config.concurrency.max(1), threadFactory)
    val timer      = Executors.newSingleThreadScheduledExecutor(threadFactory)

    // The mutable state of each item, keyed by itemId
    val itemStates = mutable.Map.from(allItems.map(r => r.itemId -> r))

    def beat(itemId: String): Unit = lastBeats.put(itemId, System.currentTimeMillis())

    // Record a state transition of the current attempt. The store fences it; when rejected
    // (e.g. a task queue already recorded the worker's outcome), the stored record wins
    def settle(record: ItemRecord): Unit =
      val accepted = store.reportResult(
        record.itemId,
        record.attemptId,
        record.state,
        record.output,
        record.error,
        record.finishedAtMillis.getOrElse(System.currentTimeMillis())
      )
      itemStates(record.itemId) =
        if accepted then
          record
        else
          store.getItem(record.itemId).getOrElse(record)

    def submitItem(record: ItemRecord): Unit =
      // A resumed attempt keeps its identity: the executor awaits the attempt it still tracks
      val resume    = resumable.remove(record.itemId)
      val attemptId =
        if resume then
          record.attemptId
        else
          ULID.newULIDString
      val startedAt = System.currentTimeMillis()
      val running   = record.copy(
        state =
          if resume && record.state == ItemState.Cancelling.stateName then
            record.state
          else
            ItemState.Running.stateName
        ,
        attemptId = attemptId,
        attempts =
          if resume then
            record.attempts
          else
            record.attempts + 1
        ,
        startedAtMillis = record.startedAtMillis.filter(_ => resume).orElse(Some(startedAt)),
        finishedAtMillis = None,
        leaseExpiresAtMillis = Some(startedAt + config.leaseTimeoutMillis)
      )
      if !resume then
        store.saveItem(running)
      itemStates(record.itemId) = running
      runningItems.put(record.itemId, running)
      beat(record.itemId)

      val request = ActivityRequest(
        activityType = activityType,
        invocationId = record.itemId,
        attemptId = attemptId,
        input = baseInput + ("item" -> record.itemKey),
        config = ActivityConfig(
          timeoutMillis = config.timeoutMillis,
          heartbeatMillis = config.heartbeatMillis
        )
      )

      workerPool.submit(
        new Runnable:
          override def run(): Unit =
            val result =
              try
                executor.execute(
                  request,
                  () =>
                    beat(record.itemId)
                    store.refreshItemLease(
                      record.itemId,
                      attemptId,
                      System.currentTimeMillis() + config.leaseTimeoutMillis
                    )
                )
              catch
                case NonFatal(e) =>
                  ActivityResult.Failed(e.getMessage, retryable = false)
            eventQueue.put(ItemCompleted(record.itemId, attemptId, result))
      )

      // Schedule heartbeat checks
      config
        .heartbeatMillis
        .foreach { interval =>
          val check = timer.scheduleAtFixedRate(
            new Runnable:
              override def run(): Unit =
                val last = Option(lastBeats.get(record.itemId)).map(_.longValue).getOrElse(0L)
                if System.currentTimeMillis() - last > interval then
                  eventQueue.put(HeartbeatMissed(record.itemId, attemptId))
            ,
            interval,
            interval,
            TimeUnit.MILLISECONDS
          )
          heartbeatJobs.put(record.itemId, check)
        }

      // Schedule timeout
      config
        .timeoutMillis
        .foreach { timeout =>
          timer.schedule(
            new Runnable:
              override def run(): Unit = eventQueue.put(ItemTimedOut(record.itemId, attemptId))
            ,
            timeout,
            TimeUnit.MILLISECONDS
          )
        }

    end submitItem

    def cleanupItem(itemId: String): Unit =
      runningItems.remove(itemId)
      lastBeats.remove(itemId)
      Option(heartbeatJobs.remove(itemId)).foreach(_.cancel(false))

    def admitNext(): Unit =
      while runningItems.size() < config.concurrency && pending.nonEmpty && !cancelRequested do
        val next = pending.dequeue()
        // Skip items that reached a terminal state during recovery
        if !ItemState.fromString(next.state).exists(_.isTerminal) then
          submitItem(next)

    def handleCancellation(): Unit =
      // Cancel queued items
      while pending.nonEmpty do
        val item = pending.dequeue()
        settle(
          item.copy(
            state = ItemState.Cancelled.stateName,
            finishedAtMillis = Some(System.currentTimeMillis())
          )
        )

      // Cancel running items. Conditional store updates keep fields other writers (e.g. a task
      // queue recording the claiming worker) intact
      runningItems
        .values()
        .asScala
        .foreach { item =>
          val cancelling = item.copy(state = ItemState.Cancelling.stateName)
          store.markCancelling(item.itemId, item.attemptId)
          itemStates(item.itemId) = cancelling
          try
            executor.cancel(item.attemptId) match
              case CancelAck.Acknowledged =>
                settle(
                  cancelling.copy(
                    state = ItemState.Cancelled.stateName,
                    finishedAtMillis = Some(System.currentTimeMillis()),
                    leaseExpiresAtMillis = None
                  )
                )
                cleanupItem(item.itemId)
              case CancelAck.Unknown =>
                // Leave in cancelling state; the event loop will handle timeout/completion
                ()
          catch
            case NonFatal(e) =>
              debug(s"Failed to cancel item ${item.itemKey}: ${e.getMessage}")
        }
    end handleCancellation

    // Lease refresh for running items
    val leaseInterval = (config.leaseTimeoutMillis / 3).max(1L)
    timer.scheduleAtFixedRate(
      new Runnable:
        override def run(): Unit = runningItems
          .values()
          .asScala
          .foreach { item =>
            try
              store.refreshItemLease(
                item.itemId,
                item.attemptId,
                System.currentTimeMillis() + config.leaseTimeoutMillis
              )
            catch
              case NonFatal(e) =>
                debug(s"Failed to refresh lease for item ${item.itemKey}: ${e.getMessage}")
          }
      ,
      leaseInterval,
      leaseInterval,
      TimeUnit.MILLISECONDS
    )

    try
      admitNext()

      var cancellationHandled = false
      while runningItems.size() > 0 || (pending.nonEmpty && !cancelRequested) do
        if cancelRequested && !cancellationHandled then
          handleCancellation()
          cancellationHandled = true

        eventQueue.poll(1, TimeUnit.SECONDS) match
          case null =>
            if runningItems.isEmpty && pending.nonEmpty && !cancelRequested then
              admitNext()
          case ItemCompleted(itemId, attemptId, result) =>
            val current = itemStates(itemId)
            // Reject stale completions
            if current.attemptId != attemptId then
              debug(
                s"Rejecting stale completion for item ${current.itemKey}: " +
                  s"expected attempt ${current.attemptId}, got ${attemptId}"
              )
            else
              settle(terminalRecord(current, result))
              cleanupItem(itemId)
              // No-op after a cancellation: admission has stopped
              admitNext()

          case HeartbeatMissed(itemId, attemptId) =>
            val current = itemStates(itemId)
            if current.attemptId == attemptId && current.state == ItemState.Running.stateName then
              settle(terminalRecord(current, ActivityResult.Uncertain("Heartbeat missed")))
              cleanupItem(itemId)
              admitNext()

          case ItemTimedOut(itemId, attemptId) =>
            val current = itemStates(itemId)
            if current.attemptId == attemptId && (
                current.state == ItemState.Running.stateName ||
                  current.state == ItemState.Cancelling.stateName
              )
            then
              settle(terminalRecord(current, ActivityResult.Uncertain("Activity timed out")))
              cleanupItem(itemId)
        end match
      end while
    catch
      case e: InterruptedException =>
        // The caller stopped waiting (e.g. flow cancellation): stop admission and request
        // cancellation so no item keeps running unnoticed, then propagate the interruption
        cancelRequested = true
        handleCancellation()
        throw e
    finally
      workerPool.shutdownNow()
      timer.shutdownNow()
    end try

    BatchResult(
      stageName = stageName,
      items = allItems.map(r => itemStates.getOrElse(r.itemId, r)),
      cancelled = cancelRequested
    )

  end execute

  /** The record of an attempt that reached the given outcome */
  private def terminalRecord(current: ItemRecord, result: ActivityResult): ItemRecord =
    val finished = current.copy(
      finishedAtMillis = Some(System.currentTimeMillis()),
      leaseExpiresAtMillis = None
    )
    result match
      case ActivityResult.Completed(output) =>
        finished.copy(
          state = ItemState.Succeeded.stateName,
          output = Some(ActivityJson.encode(output))
        )
      case ActivityResult.Failed(error, _) =>
        finished.copy(state = ItemState.Failed.stateName, error = Some(error))
      case ActivityResult.Cancelled =>
        finished.copy(state = ItemState.Cancelled.stateName)
      case ActivityResult.Uncertain(reason) =>
        finished.copy(state = ItemState.Uncertain.stateName, error = Some(reason))

end BatchExecutor

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
  * skipped, running items whose lease expired transition to uncertain, and queued items are
  * re-admitted. This provides restart recovery without re-executing completed work.
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

    // Build the item list: reuse completed items, transition stale running to uncertain,
    // create new records for unseen keys
    val allItems = itemKeys.map { key =>
      val itemId = ItemRecord.itemId(runId, stageName, key)
      existingItems.get(key) match
        case Some(r) if ItemState.fromString(r.state).exists(_.isTerminal) =>
          r
        case Some(r) if r.isStaleAt(now) =>
          val uncertain = r.copy(
            state = ItemState.Uncertain.stateName,
            finishedAtMillis = Some(now),
            leaseExpiresAtMillis = None
          )
          store.saveItem(uncertain)
          uncertain
        case Some(r) =>
          r
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

    def submitItem(record: ItemRecord): Unit =
      val attemptId = ULID.newULIDString
      val startedAt = System.currentTimeMillis()
      val running   = record.copy(
        state = ItemState.Running.stateName,
        attemptId = attemptId,
        attempts = record.attempts + 1,
        startedAtMillis = Some(startedAt),
        finishedAtMillis = None,
        leaseExpiresAtMillis = Some(startedAt + config.leaseTimeoutMillis)
      )
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
        val item      = pending.dequeue()
        val cancelled = item.copy(
          state = ItemState.Cancelled.stateName,
          finishedAtMillis = Some(System.currentTimeMillis())
        )
        store.saveItem(cancelled)
        itemStates(item.itemId) = cancelled

      // Cancel running items
      runningItems
        .values()
        .asScala
        .foreach { item =>
          val cancelling = item.copy(state = ItemState.Cancelling.stateName)
          store.saveItem(cancelling)
          itemStates(item.itemId) = cancelling
          try
            executor.cancel(item.attemptId) match
              case CancelAck.Acknowledged =>
                val cancelled = cancelling.copy(
                  state = ItemState.Cancelled.stateName,
                  finishedAtMillis = Some(System.currentTimeMillis()),
                  leaseExpiresAtMillis = None
                )
                store.saveItem(cancelled)
                itemStates(item.itemId) = cancelled
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

      while runningItems.size() > 0 || (pending.nonEmpty && !cancelRequested) do
        if cancelRequested then
          handleCancellation()
        else
          eventQueue.poll(1, TimeUnit.SECONDS) match
            case null =>
              ()
            case ItemCompleted(itemId, attemptId, result) =>
              val current = itemStates(itemId)
              // Reject stale completions
              if current.attemptId != attemptId then
                debug(
                  s"Rejecting stale completion for item ${current.itemKey}: " +
                    s"expected attempt ${current.attemptId}, got ${attemptId}"
                )
              else if current.state == ItemState.Cancelling.stateName then
                // Item was being cancelled; treat completion as the terminal state
                val terminal =
                  result match
                    case ActivityResult.Cancelled =>
                      current.copy(
                        state = ItemState.Cancelled.stateName,
                        finishedAtMillis = Some(System.currentTimeMillis()),
                        leaseExpiresAtMillis = None
                      )
                    case ActivityResult.Completed(output) =>
                      current.copy(
                        state = ItemState.Succeeded.stateName,
                        output = Some(serializeOutput(output)),
                        finishedAtMillis = Some(System.currentTimeMillis()),
                        leaseExpiresAtMillis = None
                      )
                    case ActivityResult.Failed(error, _) =>
                      current.copy(
                        state = ItemState.Failed.stateName,
                        error = Some(error),
                        finishedAtMillis = Some(System.currentTimeMillis()),
                        leaseExpiresAtMillis = None
                      )
                val accepted = store.reportResult(
                  itemId,
                  attemptId,
                  terminal.state,
                  terminal.output,
                  terminal.error,
                  terminal.finishedAtMillis.get
                )
                if accepted then
                  itemStates(itemId) = terminal
                cleanupItem(itemId)
              else
                val finishedAt = System.currentTimeMillis()
                val terminal   =
                  result match
                    case ActivityResult.Completed(output) =>
                      current.copy(
                        state = ItemState.Succeeded.stateName,
                        output = Some(serializeOutput(output)),
                        finishedAtMillis = Some(finishedAt),
                        leaseExpiresAtMillis = None
                      )
                    case ActivityResult.Failed(error, _) =>
                      current.copy(
                        state = ItemState.Failed.stateName,
                        error = Some(error),
                        finishedAtMillis = Some(finishedAt),
                        leaseExpiresAtMillis = None
                      )
                    case ActivityResult.Cancelled =>
                      current.copy(
                        state = ItemState.Cancelled.stateName,
                        finishedAtMillis = Some(finishedAt),
                        leaseExpiresAtMillis = None
                      )

                val accepted = store.reportResult(
                  itemId,
                  attemptId,
                  terminal.state,
                  terminal.output,
                  terminal.error,
                  finishedAt
                )
                if accepted then
                  itemStates(itemId) = terminal
                cleanupItem(itemId)
                admitNext()
              end if

            case HeartbeatMissed(itemId, attemptId) =>
              val current = itemStates(itemId)
              if current.attemptId == attemptId && current.state == ItemState.Running.stateName then
                val uncertain = current.copy(
                  state = ItemState.Uncertain.stateName,
                  error = Some("Heartbeat missed"),
                  finishedAtMillis = Some(System.currentTimeMillis()),
                  leaseExpiresAtMillis = None
                )
                store.reportResult(
                  itemId,
                  attemptId,
                  uncertain.state,
                  None,
                  uncertain.error,
                  uncertain.finishedAtMillis.get
                )
                itemStates(itemId) = uncertain
                cleanupItem(itemId)
                admitNext()

            case ItemTimedOut(itemId, attemptId) =>
              val current = itemStates(itemId)
              if current.attemptId == attemptId && (
                  current.state == ItemState.Running.stateName ||
                    current.state == ItemState.Cancelling.stateName
                )
              then
                // Timeout transitions to uncertain, not failed. The slot is NOT freed for a
                // replacement — the original executor may still be acting
                val uncertain = current.copy(
                  state = ItemState.Uncertain.stateName,
                  error = Some("Activity timed out"),
                  finishedAtMillis = Some(System.currentTimeMillis()),
                  leaseExpiresAtMillis = None
                )
                store.reportResult(
                  itemId,
                  attemptId,
                  uncertain.state,
                  None,
                  uncertain.error,
                  uncertain.finishedAtMillis.get
                )
                itemStates(itemId) = uncertain
                cleanupItem(itemId)
                // Admit next only after cleanup frees the slot
                admitNext()
      end while
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

  private def serializeOutput(output: Map[String, Any]): String =
    // Simple JSON serialization for activity output
    val entries = output.map { (k, v) =>
      val escaped = v.toString.replace("\\", "\\\\").replace("\"", "\\\"")
      s""""${k}":"${escaped}""""
    }
    s"{${entries.mkString(",")}}"

end BatchExecutor

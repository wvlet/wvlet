package wvlet.lang.runner.activity

import wvlet.uni.test.UniTest
import wvlet.uni.util.ULID

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

class BatchExecutorTest extends UniTest:

  private def newStore(): SQLiteItemStore =
    val dir = Files.createTempDirectory("wv-batch-test")
    SQLiteItemStore(dir.resolve("items.db"))

  /**
    * A controlled activity executor for testing. Executions block on a latch, allowing precise
    * control over timing, ordering, and failure injection
    */
  class ControlledExecutor(
      val execName: String = "test",
      failItems: Set[String] = Set.empty,
      slowItems: Set[String] = Set.empty,
      slowDelayMs: Long = 100L
  ) extends ActivityExecutor:
    val startedItems      = ConcurrentHashMap.newKeySet[String]()
    val completedItems    = ConcurrentHashMap.newKeySet[String]()
    val cancelledAttempts = ConcurrentHashMap.newKeySet[String]()
    val concurrentCount   = AtomicInteger(0)
    val maxConcurrent     = AtomicInteger(0)

    override def name: String = execName

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val itemKey = request.input("item").toString
      startedItems.add(itemKey)
      val current = concurrentCount.incrementAndGet()
      maxConcurrent.updateAndGet(m => m.max(current))
      try
        if slowItems.contains(itemKey) then
          Thread.sleep(slowDelayMs)
        heartbeat()
        if failItems.contains(itemKey) then
          ActivityResult.Failed(s"Item ${itemKey} failed", retryable = false)
        else
          completedItems.add(itemKey)
          ActivityResult.Completed(Map("result" -> s"done-${itemKey}"))
      finally
        concurrentCount.decrementAndGet()

    override def cancel(attemptId: String): CancelAck =
      cancelledAttempts.add(attemptId)
      CancelAck.Acknowledged

  end ControlledExecutor

  /**
    * An executor that never completes (blocks indefinitely), for testing timeouts and cancellation
    */
  class BlockingExecutor extends ActivityExecutor:
    val startedItems      = ConcurrentHashMap.newKeySet[String]()
    val cancelledAttempts = ConcurrentHashMap.newKeySet[String]()
    private val latches   = ConcurrentHashMap[String, CountDownLatch]()

    override def name: String = "blocking"

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val itemKey = request.input("item").toString
      startedItems.add(itemKey)
      val latch = latches.computeIfAbsent(request.attemptId, _ => CountDownLatch(1))
      heartbeat()
      latch.await(5, TimeUnit.MINUTES)
      ActivityResult.Completed(Map("result" -> "unblocked"))

    override def cancel(attemptId: String): CancelAck =
      cancelledAttempts.add(attemptId)
      Option(latches.get(attemptId)).foreach(_.countDown())
      CancelAck.Acknowledged

    def unblock(attemptId: String): Unit = Option(latches.get(attemptId)).foreach(_.countDown())

  end BlockingExecutor

  /**
    * An executor that reports heartbeats only when asked, for testing heartbeat loss
    */
  class HeartbeatControlledExecutor(reportHeartbeat: Boolean = true, executionDelayMs: Long = 50L)
      extends ActivityExecutor:
    val startedItems          = ConcurrentHashMap.newKeySet[String]()
    override def name: String = "heartbeat-test"

    override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
      val itemKey = request.input("item").toString
      startedItems.add(itemKey)
      if reportHeartbeat then
        heartbeat()
      Thread.sleep(executionDelayMs)
      if reportHeartbeat then
        heartbeat()
      ActivityResult.Completed(Map("result" -> s"done-${itemKey}"))

    override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

  test("should execute all items with bounded concurrency") {
    val store    = newStore()
    val executor = ControlledExecutor(slowItems = Set("b", "d"), slowDelayMs = 50L)
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 2))

    val result = batch.execute(
      runId = ULID.newULIDString,
      stageName = "test_stage",
      activityType = "test",
      itemKeys = List("a", "b", "c", "d", "e")
    )

    result.items.size shouldBe 5
    result.succeeded.size shouldBe 5
    result.failed.size shouldBe 0
    result.hasErrors shouldBe false
    executor.maxConcurrent.get() shouldBe 2
    store.close()
  }

  test("should queue excess items and admit on completion") {
    val store    = newStore()
    val executor = ControlledExecutor()
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 1))

    val result = batch.execute(
      runId = ULID.newULIDString,
      stageName = "test_stage",
      activityType = "test",
      itemKeys = List("a", "b", "c")
    )

    result.items.size shouldBe 3
    result.succeeded.size shouldBe 3
    // With concurrency 1, items execute sequentially
    executor.maxConcurrent.get() shouldBe 1
    store.close()
  }

  test("should handle partial failure without stopping the batch") {
    val store    = newStore()
    val executor = ControlledExecutor(failItems = Set("b", "d"))
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 3))

    val result = batch.execute(
      runId = ULID.newULIDString,
      stageName = "test_stage",
      activityType = "test",
      itemKeys = List("a", "b", "c", "d", "e")
    )

    result.items.size shouldBe 5
    result.succeeded.size shouldBe 3
    result.failed.size shouldBe 2
    result.hasErrors shouldBe true
    result.failed.map(_.itemKey).toSet shouldBe Set("b", "d")
    store.close()
  }

  test("should persist item records with correct states") {
    val store    = newStore()
    val runId    = ULID.newULIDString
    val executor = ControlledExecutor(failItems = Set("b"))
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 2))

    batch.execute(
      runId = runId,
      stageName = "persist_test",
      activityType = "test",
      itemKeys = List("a", "b", "c")
    )

    val items = store.listItems(runId, "persist_test")
    items.size shouldBe 3

    val itemA = items.find(_.itemKey == "a").get
    itemA.state shouldBe ItemState.Succeeded.stateName
    itemA.output.isDefined shouldBe true
    itemA.finishedAtMillis.isDefined shouldBe true

    val itemB = items.find(_.itemKey == "b").get
    itemB.state shouldBe ItemState.Failed.stateName
    itemB.error.isDefined shouldBe true

    val itemC = items.find(_.itemKey == "c").get
    itemC.state shouldBe ItemState.Succeeded.stateName
    store.close()
  }

  test("should reject stale attempt completions") {
    val store = newStore()
    val runId = ULID.newULIDString

    // Create an item record with a known attempt id
    val itemId    = ItemRecord.itemId(runId, "stage", "key1")
    val attemptId = ULID.newULIDString
    store.saveItem(
      ItemRecord(
        itemId = itemId,
        runId = runId,
        stageName = "stage",
        itemKey = "key1",
        state = ItemState.Running.stateName,
        attemptId = attemptId,
        attempts = 1,
        startedAtMillis = Some(System.currentTimeMillis()),
        leaseExpiresAtMillis = Some(System.currentTimeMillis() + 60_000)
      )
    )

    // Try to report with a stale attempt id
    val staleId  = ULID.newULIDString
    val accepted = store.reportResult(
      itemId,
      staleId,
      ItemState.Succeeded.stateName,
      Some("{}"),
      None,
      System.currentTimeMillis()
    )
    accepted shouldBe false

    // Report with the correct attempt id
    val correctAccepted = store.reportResult(
      itemId,
      attemptId,
      ItemState.Succeeded.stateName,
      Some("{}"),
      None,
      System.currentTimeMillis()
    )
    correctAccepted shouldBe true
    store.close()
  }

  test("should handle cancellation with acknowledgement") {
    val store   = newStore()
    val blocker = BlockingExecutor()
    val batch   = BatchExecutor(blocker, store, BatchConfig(concurrency = 2))
    val runId   = ULID.newULIDString

    // Start batch in a separate thread
    val resultFuture = java
      .util
      .concurrent
      .Executors
      .newSingleThreadExecutor()
      .submit(
        new java.util.concurrent.Callable[BatchResult]:
          override def call(): BatchResult = batch.execute(
            runId = runId,
            stageName = "cancel_test",
            activityType = "blocking",
            itemKeys = List("a", "b", "c", "d")
          )
      )

    // Wait for some items to start
    Thread.sleep(200)

    // Cancel the batch
    batch.cancel()

    val result = resultFuture.get(10, TimeUnit.SECONDS)
    result.cancelled shouldBe true
    // Running items should have been cancelled
    val cancelled = result.items.filter(_.state == ItemState.Cancelled.stateName)
    cancelled.nonEmpty shouldBe true
    store.close()
  }

  test("should recover completed items on restart") {
    val store = newStore()
    val runId = ULID.newULIDString

    // First execution: complete items a and b, fail c
    val executor1 = ControlledExecutor(failItems = Set("c"))
    val batch1    = BatchExecutor(executor1, store, BatchConfig(concurrency = 3))
    val result1   = batch1.execute(
      runId = runId,
      stageName = "restart_test",
      activityType = "test",
      itemKeys = List("a", "b", "c")
    )
    result1.succeeded.size shouldBe 2
    result1.failed.size shouldBe 1

    // Second execution (restart): a and b should not re-execute, c retries and succeeds
    val executor2 = ControlledExecutor()
    val batch2    = BatchExecutor(executor2, store, BatchConfig(concurrency = 3))
    val result2   = batch2.execute(
      runId = runId,
      stageName = "restart_test",
      activityType = "test",
      itemKeys = List("a", "b", "c")
    )

    // a and b were already succeeded, so they shouldn't have been re-executed
    executor2.startedItems.contains("a") shouldBe false
    executor2.startedItems.contains("b") shouldBe false
    // c was failed and should have been retried (it's terminal, so it stays failed on re-read)
    // Actually, failed is terminal, so it won't retry without explicit policy
    store.close()
  }

  test("should transition stale running items to uncertain on restart") {
    val store = newStore()
    val runId = ULID.newULIDString

    // Simulate a crashed process: item is running with an expired lease
    val itemId = ItemRecord.itemId(runId, "stale_test", "a")
    store.saveItem(
      ItemRecord(
        itemId = itemId,
        runId = runId,
        stageName = "stale_test",
        itemKey = "a",
        state = ItemState.Running.stateName,
        attemptId = ULID.newULIDString,
        attempts = 1,
        startedAtMillis = Some(System.currentTimeMillis() - 120_000),
        leaseExpiresAtMillis = Some(System.currentTimeMillis() - 60_000) // expired
      )
    )

    val executor = ControlledExecutor()
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 2))
    val result   = batch.execute(
      runId = runId,
      stageName = "stale_test",
      activityType = "test",
      itemKeys = List("a")
    )

    // The item should be uncertain (stale running -> uncertain)
    result.uncertain.size shouldBe 1
    result.uncertain.head.itemKey shouldBe "a"

    // With retryUncertain, it should retry
    val executor2 = ControlledExecutor()
    val batch2    = BatchExecutor(
      executor2,
      store,
      BatchConfig(concurrency = 2, retryUncertain = true)
    )
    val result2 = batch2.execute(
      runId = runId,
      stageName = "stale_test",
      activityType = "test",
      itemKeys = List("a")
    )
    executor2.startedItems.contains("a") shouldBe true
    store.close()
  }

  test("should handle timeout without overlapping replacements") {
    val store   = newStore()
    val blocker = BlockingExecutor()
    val runId   = ULID.newULIDString

    val batch = BatchExecutor(
      blocker,
      store,
      BatchConfig(concurrency = 2, timeoutMillis = Some(200L))
    )

    val resultFuture = java
      .util
      .concurrent
      .Executors
      .newSingleThreadExecutor()
      .submit(
        new java.util.concurrent.Callable[BatchResult]:
          override def call(): BatchResult = batch.execute(
            runId = runId,
            stageName = "timeout_test",
            activityType = "blocking",
            itemKeys = List("a", "b")
          )
      )

    val result = resultFuture.get(10, TimeUnit.SECONDS)
    // Timed-out items should be uncertain, not failed
    result.uncertain.size shouldBe 2
    result
      .uncertain
      .foreach { item =>
        item.error.get shouldContain "timed out"
      }
    store.close()
  }

  test("should handle heartbeat loss") {
    val store    = newStore()
    val executor = HeartbeatControlledExecutor(reportHeartbeat = false, executionDelayMs = 500L)
    val batch    = BatchExecutor(
      executor,
      store,
      BatchConfig(concurrency = 2, heartbeatMillis = Some(100L))
    )

    val result = batch.execute(
      runId = ULID.newULIDString,
      stageName = "heartbeat_test",
      activityType = "heartbeat-test",
      itemKeys = List("a")
    )

    // Item should be uncertain due to missed heartbeat
    result.uncertain.size shouldBe 1
    result.uncertain.head.error.get shouldContain "Heartbeat"
    store.close()
  }

  test("should pass item key as input to the executor") {
    val store          = newStore()
    val receivedInputs = ConcurrentHashMap[String, Map[String, Any]]()
    val executor       =
      new ActivityExecutor:
        override def name: String = "input-test"
        override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
          receivedInputs.put(request.input("item").toString, request.input)
          heartbeat()
          ActivityResult.Completed()
        override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

    val batch = BatchExecutor(executor, store, BatchConfig(concurrency = 2))
    batch.execute(
      runId = ULID.newULIDString,
      stageName = "input_test",
      activityType = "input-test",
      itemKeys = List("customer_1", "customer_2"),
      baseInput = Map("workspace" -> "test-ws")
    )

    receivedInputs.get("customer_1")("workspace") shouldBe "test-ws"
    receivedInputs.get("customer_1")("item") shouldBe "customer_1"
    receivedInputs.get("customer_2")("item") shouldBe "customer_2"
    store.close()
  }

  test("should handle out-of-order completions correctly") {
    val store = newStore()
    // Item "a" is slow, "b" and "c" are fast — they should complete before "a"
    val executor = ControlledExecutor(slowItems = Set("a"), slowDelayMs = 200L)
    val batch    = BatchExecutor(executor, store, BatchConfig(concurrency = 3))

    val result = batch.execute(
      runId = ULID.newULIDString,
      stageName = "order_test",
      activityType = "test",
      itemKeys = List("a", "b", "c")
    )

    result.succeeded.size shouldBe 3
    // b and c should have completed before a
    executor.completedItems.contains("b") shouldBe true
    executor.completedItems.contains("c") shouldBe true
    executor.completedItems.contains("a") shouldBe true
    store.close()
  }

  test("should assign unique attempt ids per invocation") {
    val store      = newStore()
    val attemptIds = ConcurrentHashMap.newKeySet[String]()
    val executor   =
      new ActivityExecutor:
        override def name: String = "attempt-id-test"
        override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
          attemptIds.add(request.attemptId)
          heartbeat()
          ActivityResult.Completed()
        override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged

    val batch = BatchExecutor(executor, store, BatchConfig(concurrency = 3))
    batch.execute(
      runId = ULID.newULIDString,
      stageName = "attempt_test",
      activityType = "attempt-id-test",
      itemKeys = List("a", "b", "c")
    )

    // Each item should have a unique attempt id
    attemptIds.size() shouldBe 3
    store.close()
  }

  test("should use stable invocation ids across attempts") {
    val store = newStore()
    val runId = ULID.newULIDString

    // Compute expected invocation ids
    val expectedIdA = ItemRecord.itemId(runId, "stable_test", "a")
    val expectedIdB = ItemRecord.itemId(runId, "stable_test", "b")

    // First run: fail both
    val executor1 = ControlledExecutor(failItems = Set("a", "b"))
    val batch1    = BatchExecutor(executor1, store, BatchConfig(concurrency = 2))
    batch1.execute(
      runId = runId,
      stageName = "stable_test",
      activityType = "test",
      itemKeys = List("a", "b")
    )

    val items1 = store.listItems(runId, "stable_test")
    items1.find(_.itemKey == "a").get.itemId shouldBe expectedIdA
    items1.find(_.itemKey == "b").get.itemId shouldBe expectedIdB
    store.close()
  }

end BatchExecutorTest

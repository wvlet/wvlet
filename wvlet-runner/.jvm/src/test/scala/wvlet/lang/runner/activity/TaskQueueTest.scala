package wvlet.lang.runner.activity

import wvlet.uni.test.UniTest

class TaskQueueTest extends UniTest:

  private def newQueue(): TaskQueue = TaskQueue(InMemoryItemStore())

  test("poll returns None when queue is empty") {
    val q      = newQueue()
    val result = q.poll(List("agent.test"), "worker-1", timeoutMillis = 0)
    result shouldBe None
    q.pendingCount shouldBe 0
    q.activeCount shouldBe 0
  }

  test("enqueue and poll a matching task") {
    val q    = newQueue()
    val task = PendingTask(
      activityType = "agent.investigate",
      invocationId = "inv-1",
      attemptId = "attempt-1",
      input = Map("item" -> "customer-42"),
      config = ActivityConfig(timeoutMillis = Some(30000L), heartbeatMillis = Some(5000L)),
      enqueuedAtMillis = System.currentTimeMillis()
    )
    q.enqueue(task)
    q.pendingCount shouldBe 1

    val polled = q.poll(List("agent.investigate"), "worker-1", timeoutMillis = 0)
    polled.isDefined shouldBe true
    polled.get.attemptId shouldBe "attempt-1"
    polled.get.activityType shouldBe "agent.investigate"

    q.pendingCount shouldBe 0
    q.activeCount shouldBe 1
  }

  test("poll skips tasks with non-matching activity type") {
    val q = newQueue()
    q.enqueue(
      PendingTask(
        activityType = "agent.summarize",
        invocationId = "inv-2",
        attemptId = "attempt-2",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )

    val result = q.poll(List("agent.investigate"), "worker-1", timeoutMillis = 0)
    result shouldBe None
    q.pendingCount shouldBe 1
  }

  test("poll matches any of the worker's declared activity types") {
    val q = newQueue()
    q.enqueue(
      PendingTask(
        activityType = "agent.summarize",
        invocationId = "inv-3",
        attemptId = "attempt-3",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )

    val result = q.poll(List("agent.investigate", "agent.summarize"), "worker-1", timeoutMillis = 0)
    result.isDefined shouldBe true
    result.get.activityType shouldBe "agent.summarize"
  }

  test("reportResult accepts valid attempt") {
    val q    = newQueue()
    val task = PendingTask(
      activityType = "agent.test",
      invocationId = "inv-4",
      attemptId = "attempt-4",
      input = Map.empty,
      config = ActivityConfig(),
      enqueuedAtMillis = System.currentTimeMillis()
    )
    q.enqueue(task)
    q.poll(List("agent.test"), "worker-1", timeoutMillis = 0)

    val accepted = q.reportResult("attempt-4", "completed", Some("""{"ok":"true"}"""), None)
    accepted shouldBe true
    q.activeCount shouldBe 0
  }

  test("reportResult rejects unknown attempt") {
    val q        = newQueue()
    val rejected = q.reportResult("nonexistent-attempt", "completed", None, None)
    rejected shouldBe false
  }

  test("heartbeat returns continue for active attempt") {
    val q = newQueue()
    q.enqueue(
      PendingTask(
        activityType = "agent.test",
        invocationId = "inv-5",
        attemptId = "attempt-5",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )
    q.poll(List("agent.test"), "worker-1", timeoutMillis = 0)

    val action = q.heartbeat("attempt-5")
    action shouldBe "continue"
  }

  test("heartbeat returns cancel after cancellation requested") {
    val q = newQueue()
    q.enqueue(
      PendingTask(
        activityType = "agent.test",
        invocationId = "inv-6",
        attemptId = "attempt-6",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )
    q.poll(List("agent.test"), "worker-1", timeoutMillis = 0)
    q.requestCancel("attempt-6")

    val action = q.heartbeat("attempt-6")
    action shouldBe "cancel"
  }

  test("heartbeat returns unknown for inactive attempt") {
    val q      = newQueue()
    val action = q.heartbeat("nonexistent")
    action shouldBe "unknown"
  }

  test("multiple tasks dispatched in FIFO order") {
    val q = newQueue()
    for i <- 1 to 5 do
      q.enqueue(
        PendingTask(
          activityType = "agent.test",
          invocationId = s"inv-fifo-${i}",
          attemptId = s"attempt-fifo-${i}",
          input = Map("order" -> i.toString),
          config = ActivityConfig(),
          enqueuedAtMillis = System.currentTimeMillis()
        )
      )

    q.pendingCount shouldBe 5

    for i <- 1 to 5 do
      val polled = q.poll(List("agent.test"), "worker-1", timeoutMillis = 0)
      polled.isDefined shouldBe true
      polled.get.attemptId shouldBe s"attempt-fifo-${i}"

    q.pendingCount shouldBe 0
    q.activeCount shouldBe 5
  }

end TaskQueueTest

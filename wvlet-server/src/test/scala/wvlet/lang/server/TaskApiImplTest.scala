package wvlet.lang.server

import wvlet.lang.api.v1.task.TaskApi
import wvlet.lang.api.v1.task.TaskApi.*
import wvlet.lang.runner.activity.{ActivityConfig, InMemoryItemStore, PendingTask, TaskQueue}
import wvlet.lang.test.WvletDITest
import wvlet.uni.http.rpc.RPCException

class TaskApiImplTest extends WvletDITest:

  private val itemStore = InMemoryItemStore()
  private val taskQueue = TaskQueue(itemStore)

  initDesign:
    _.bindInstance[TaskQueue](taskQueue).bindImpl[TaskApi, TaskApiImpl]

  test("poll returns empty when no tasks available") {
    val api      = dep[TaskApi]
    val response = api.poll(PollRequest(List("agent.test"), "worker-1"))
    response.hasTask shouldBe false
    response.retryAfterMillis shouldBe Some(1000L)
  }

  test("poll rejects empty activity types") {
    val api = dep[TaskApi]
    val e   = intercept[RPCException] {
      api.poll(PollRequest(List.empty, "worker-1"))
    }
    e.getMessage shouldContain "activityTypes"
  }

  test("poll returns a matching task") {
    val api = dep[TaskApi]
    taskQueue.enqueue(
      PendingTask(
        activityType = "agent.investigate",
        invocationId = "inv-api-1",
        attemptId = "attempt-api-1",
        input = Map("customer" -> "cust-42", "priority" -> "high"),
        config = ActivityConfig(timeoutMillis = Some(30000L), heartbeatMillis = Some(5000L)),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )

    val response = api.poll(PollRequest(List("agent.investigate"), "worker-1"))
    response.hasTask shouldBe true
    response.activityType shouldBe "agent.investigate"
    response.attemptId shouldBe "attempt-api-1"
    response.input("customer") shouldBe "cust-42"
    response.timeoutMillis shouldBe Some(30000L)
    response.heartbeatMillis shouldBe Some(5000L)
  }

  test("reportResult accepts valid completion") {
    val api = dep[TaskApi]

    taskQueue.enqueue(
      PendingTask(
        activityType = "agent.test",
        invocationId = "inv-api-2",
        attemptId = "attempt-api-2",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )
    api.poll(PollRequest(List("agent.test"), "worker-1"))

    val result = api.reportResult(
      TaskResultRequest(
        attemptId = "attempt-api-2",
        status = "completed",
        output = Map("result" -> "success")
      )
    )
    result.accepted shouldBe true
  }

  test("reportResult rejects invalid status") {
    val api = dep[TaskApi]
    val e   = intercept[RPCException] {
      api.reportResult(TaskResultRequest(attemptId = "attempt-x", status = "bogus"))
    }
    e.getMessage shouldContain "Invalid status"
  }

  test("reportResult rejects empty attemptId") {
    val api = dep[TaskApi]
    val e   = intercept[RPCException] {
      api.reportResult(TaskResultRequest(attemptId = "", status = "completed"))
    }
    e.getMessage shouldContain "attemptId"
  }

  test("heartbeat returns continue for active attempt") {
    val api = dep[TaskApi]

    taskQueue.enqueue(
      PendingTask(
        activityType = "agent.test",
        invocationId = "inv-api-3",
        attemptId = "attempt-api-3",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )
    api.poll(PollRequest(List("agent.test"), "worker-1"))

    val response = api.heartbeat(HeartbeatRequest("attempt-api-3"))
    response.action shouldBe "continue"
  }

  test("heartbeat returns cancel after cancellation") {
    val api = dep[TaskApi]

    taskQueue.enqueue(
      PendingTask(
        activityType = "agent.test",
        invocationId = "inv-api-4",
        attemptId = "attempt-api-4",
        input = Map.empty,
        config = ActivityConfig(),
        enqueuedAtMillis = System.currentTimeMillis()
      )
    )
    api.poll(PollRequest(List("agent.test"), "worker-1"))
    taskQueue.requestCancel("attempt-api-4")

    val response = api.heartbeat(HeartbeatRequest("attempt-api-4"))
    response.action shouldBe "cancel"
  }

  test("heartbeat rejects unknown attempt") {
    val api = dep[TaskApi]
    val e   = intercept[RPCException] {
      api.heartbeat(HeartbeatRequest("nonexistent"))
    }
    e.getMessage shouldContain "not active"
  }

  test("heartbeat rejects empty attemptId") {
    val api = dep[TaskApi]
    val e   = intercept[RPCException] {
      api.heartbeat(HeartbeatRequest(""))
    }
    e.getMessage shouldContain "attemptId"
  }

end TaskApiImplTest

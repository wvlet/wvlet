package wvlet.lang.runner.activity

import wvlet.uni.test.UniTest
import wvlet.uni.util.ULID

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.util.Using

/**
  * Durability of the task queue: queued and in-flight attempts survive a restart (a new queue on
  * the same SQLite store), lease expiry resolves lost workers as uncertain, and nothing is executed
  * twice
  */
class TaskQueueRecoveryTest extends UniTest:

  private def newDbPath(): Path = Files.createTempDirectory("wv-task-queue").resolve("items.db")

  private def newTask(
      itemId: String = ULID.newULIDString,
      activityType: String = "agent.investigate",
      heartbeatMillis: Option[Long] = None
  ): PendingTask = PendingTask(
    activityType = activityType,
    invocationId = itemId,
    attemptId = ULID.newULIDString,
    input = Map("item" -> "customer-1"),
    config = ActivityConfig(heartbeatMillis = heartbeatMillis),
    enqueuedAtMillis = System.currentTimeMillis()
  )

  test("persist enqueued tasks and keep the batch's item coordinates on claim") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val itemId = ItemRecord.itemId("run1", "investigate", "c1")
      store.saveItem(
        ItemRecord(itemId, "run1", "investigate", "c1", ItemState.Running.stateName, "a0", 1)
      )
      val queue = TaskQueue(store)
      val task  = newTask(itemId)
      queue.enqueue(task)

      val queued = store.getItem(itemId).get
      queued.state shouldBe ItemState.Queued.stateName
      queued.attemptId shouldBe task.attemptId
      queued.activityType shouldBe Some("agent.investigate")
      ActivityJson.decode(queued.input.get) shouldBe Map("item" -> "customer-1")

      queue.poll(List("agent.investigate"), "worker-1", 0).map(_.attemptId) shouldBe
        Some(task.attemptId)
      val running = store.getItem(itemId).get
      running.state shouldBe ItemState.Running.stateName
      running.runId shouldBe "run1"
      running.stageName shouldBe "investigate"
      running.itemKey shouldBe "c1"
      running.attempts shouldBe 1
      running.workerId shouldBe Some("worker-1")
    }
  }

  test("re-dispatch queued tasks after a restart") {
    val db   = newDbPath()
    val task = newTask()
    Using.resource(SQLiteItemStore(db)) { store =>
      TaskQueue(store).enqueue(task)
    }
    Using.resource(SQLiteItemStore(db)) { store =>
      val restarted = TaskQueue(store)
      restarted.pendingCount shouldBe 1
      val polled = restarted.poll(List("agent.investigate"), "worker-2", 0).get
      polled.attemptId shouldBe task.attemptId
      polled.input("item") shouldBe "customer-1"
    }
  }

  test("adopt in-flight attempts after a restart so the worker can finish") {
    val db   = newDbPath()
    val task = newTask()
    Using.resource(SQLiteItemStore(db)) { store =>
      val queue = TaskQueue(store)
      queue.enqueue(task)
      queue.poll(List("agent.investigate"), "worker-1", 0)
      queue.requestCancel(task.attemptId) shouldBe false
    }
    Using.resource(SQLiteItemStore(db)) { store =>
      val restarted = TaskQueue(store)
      // Not re-dispatched: the original worker still holds it
      restarted.pendingCount shouldBe 0
      restarted.activeCount shouldBe 1
      restarted.activeAttempts.head.workerId shouldBe "worker-1"
      // The cancellation request survives the restart
      restarted.heartbeat(task.attemptId) shouldBe "cancel"
      restarted.reportResult(task.attemptId, "cancelled", None, None) shouldBe true
      store.getItem(task.invocationId).get.state shouldBe ItemState.Cancelled.stateName
    }
  }

  test("resolve an attempt whose lease expired as uncertain without re-dispatching it") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val now   = AtomicLong(1_000_000L)
      val queue = TaskQueue(store, defaultLeaseMillis = 60_000L, clock = () => now.get())
      val task  = newTask(heartbeatMillis = Some(1_000L))
      queue.enqueue(task)
      queue.poll(List("agent.investigate"), "worker-1", 0)

      // The lease is three heartbeat intervals
      now.addAndGet(2_000L)
      queue.expireLeases() shouldBe Nil
      queue.heartbeat(task.attemptId) shouldBe "continue"
      now.addAndGet(3_001L)
      queue.expireLeases() shouldBe List(task.attemptId)

      val item = store.getItem(task.invocationId).get
      item.state shouldBe ItemState.Uncertain.stateName
      item.error.get shouldContain "worker-1"
      queue.pendingCount shouldBe 0
      queue.heartbeat(task.attemptId) shouldBe "unknown"
      queue.poll(List("agent.investigate"), "worker-2", 0) shouldBe None
    }
  }

  test("drop a task whose item was reassigned to a newer attempt") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val queue  = TaskQueue(store)
      val first  = newTask()
      val second = first.copy(attemptId = ULID.newULIDString)
      queue.enqueue(first)
      queue.enqueue(second)
      queue.poll(List("agent.investigate"), "worker-1", 0).map(_.attemptId) shouldBe
        Some(second.attemptId)
      queue.pendingCount shouldBe 0
    }
  }

  test("confirm cancellation of a task no worker has claimed") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val queue  = TaskQueue(store)
      val task   = newTask()
      val future = queue.dispatch(task, () => ())
      queue.requestCancel(task.attemptId) shouldBe true
      future.get(1, TimeUnit.SECONDS) shouldBe ActivityResult.Cancelled
      store.getItem(task.invocationId).get.state shouldBe ItemState.Cancelled.stateName
      queue.poll(List("agent.investigate"), "worker-1", 0) shouldBe None
    }
  }

  test("never overwrite a final result") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val queue = TaskQueue(store)
      val task  = newTask()
      queue.enqueue(task)
      queue.poll(List("agent.investigate"), "worker-1", 0)
      queue.reportResult(task.attemptId, "completed", Some("""{"ok":"1"}"""), None) shouldBe true
      store.reportResult(
        task.invocationId,
        task.attemptId,
        ItemState.Failed.stateName,
        None,
        Some("late"),
        System.currentTimeMillis()
      ) shouldBe false
      store.getItem(task.invocationId).get.state shouldBe ItemState.Succeeded.stateName
    }
  }

  test("resume an in-flight queue attempt after a batch restart instead of re-executing it") {
    val db        = newDbPath()
    val runId     = ULID.newULIDString
    val itemId    = ItemRecord.itemId(runId, "investigate", "c1")
    var attemptId = ""

    // First process: the batch dispatches the item, a worker claims it, then the process stops
    Using.resource(SQLiteItemStore(db)) { store =>
      store.saveItem(
        ItemRecord(itemId, runId, "investigate", "c1", ItemState.Running.stateName, "a1", 1)
      )
      val queue = TaskQueue(store)
      val task  = newTask(itemId).copy(attemptId = "a1")
      queue.enqueue(task)
      attemptId = queue.poll(List("agent.investigate"), "worker-1", 0).get.attemptId
    }

    // Second process: the same batch runs again while the worker is still working
    Using.resource(SQLiteItemStore(db)) { store =>
      val queue    = TaskQueue(store)
      val executor = TaskQueueActivityExecutor("agent.investigate", queue, pollIntervalMillis = 20)
      executor.canResume(attemptId) shouldBe true
      val worker = Thread { () =>
        Thread.sleep(100)
        queue.reportResult(attemptId, "completed", Some("""{"session":"s1"}"""), None)
        ()
      }
      worker.start()
      val result = BatchExecutor(executor, store, BatchConfig(concurrency = 1)).execute(
        runId = runId,
        stageName = "investigate",
        activityType = "agent.investigate",
        itemKeys = List("c1")
      )
      worker.join(5000)
      val item = result.items.head
      item.state shouldBe ItemState.Succeeded.stateName
      item.attemptId shouldBe attemptId
      item.attempts shouldBe 1
      ActivityJson.decode(item.output.get) shouldBe Map("session" -> "s1")
      queue.pendingCount shouldBe 0
    }
  }

  test("mark an untracked in-flight item uncertain on batch restart instead of re-executing it") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val runId = ULID.newULIDString
      store.saveItem(
        ItemRecord(
          ItemRecord.itemId(runId, "s", "a"),
          runId,
          "s",
          "a",
          ItemState.Running.stateName,
          "a1",
          1,
          leaseExpiresAtMillis = Some(System.currentTimeMillis() + 60_000)
        )
      )
      val executions = AtomicInteger(0)
      val executor   =
        new ActivityExecutor:
          override def name: String = "local"
          override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
            executions.incrementAndGet()
            ActivityResult.Completed()
          override def cancel(attemptId: String): CancelAck = CancelAck.Acknowledged
      val result = BatchExecutor(executor, store).execute(runId, "s", "local", List("a"))
      executions.get() shouldBe 0
      result.uncertain.map(_.itemKey) shouldBe List("a")
    }
  }

  test("surface a lost worker as an uncertain batch item") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val now      = AtomicLong(System.currentTimeMillis())
      val queue    = TaskQueue(store, defaultLeaseMillis = 1_000L, clock = () => now.get())
      val executor = TaskQueueActivityExecutor("agent.investigate", queue, pollIntervalMillis = 20)
      // A worker claims the task and dies without reporting
      val reaper = Thread { () =>
        while queue.poll(List("agent.investigate"), "worker-1", 50).isEmpty do
          (
        )
        now.addAndGet(5_000L)
        queue.expireLeases()
        ()
      }
      reaper.start()
      val result = BatchExecutor(executor, store).execute(
        ULID.newULIDString,
        "investigate",
        "agent.investigate",
        List("c1")
      )
      reaper.join(5000)
      result.uncertain.size shouldBe 1
      result.uncertain.head.error.get shouldContain "Lease expired"
    }
  }

  test("accept a late result of an expired attempt to resolve the uncertain item") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val now   = AtomicLong(1_000_000L)
      val queue = TaskQueue(store, defaultLeaseMillis = 1_000L, clock = () => now.get())
      val task  = newTask()
      queue.enqueue(task)
      queue.poll(List("agent.investigate"), "worker-1", 0)
      now.addAndGet(2_000L)
      queue.expireLeases() shouldBe List(task.attemptId)
      store.getItem(task.invocationId).get.state shouldBe ItemState.Uncertain.stateName

      queue.reportResult(task.attemptId, "completed", Some("""{"ok":"1"}"""), None) shouldBe true
      store.getItem(task.invocationId).get.state shouldBe ItemState.Succeeded.stateName
    }
  }

  test("report the stored outcome of a resumed attempt that finished before dispatch") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      val queue = TaskQueue(store)
      val task  = newTask()
      queue.enqueue(task)
      queue.poll(List("agent.investigate"), "worker-1", 0)
      queue.reportResult(task.attemptId, "completed", Some("""{"session":"s1"}"""), None)

      // The caller resumes the attempt only now: no second execution
      val result = queue.dispatch(task, () => ()).get(1, TimeUnit.SECONDS)
      result shouldBe ActivityResult.Completed(Map("session" -> "s1"))
      queue.pendingCount shouldBe 0
      store.getItem(task.invocationId).get.state shouldBe ItemState.Succeeded.stateName
    }
  }

  test("add dispatch columns to an item table created before them") {
    val db = newDbPath()
    Using.resource(DriverManager.getConnection(s"jdbc:sqlite:${db}")) { conn =>
      Using.resource(conn.createStatement()) {
        _.execute("""create table items(
            |  item_id text primary key, run_id text not null, stage_name text not null,
            |  item_key text not null, state text not null, attempt_id text not null,
            |  attempts integer not null default 0, output text, error text, started_at integer,
            |  finished_at integer, lease_expires_at integer)""".stripMargin)
      }
    }
    Using.resource(SQLiteItemStore(db)) { store =>
      val record = ItemRecord(
        "i1",
        "r",
        "s",
        "k",
        ItemState.Queued.stateName,
        "a1",
        1,
        activityType = Some("t"),
        workerId = Some("w")
      )
      store.saveItem(record)
      store.getItem("i1") shouldBe Some(record)
    }
  }

  test("list a stage's items by page and state with counts") {
    Using.resource(SQLiteItemStore(newDbPath())) { store =>
      (1 to 5).foreach { i =>
        val state =
          if i % 2 == 0 then
            ItemState.Failed
          else
            ItemState.Succeeded
        store.saveItem(ItemRecord(s"i${i}", "r", "s", s"k${i}", state.stateName, s"a${i}", 1))
      }
      store.listItemsPage("r", "s", None, 1, 2).map(_.itemKey) shouldBe List("k2", "k3")
      store.listItemsPage("r", "s", Some("failed"), 0, 10).map(_.itemKey) shouldBe List("k2", "k4")
      store.countItemsByState("r", "s") shouldBe Map("succeeded" -> 3, "failed" -> 2)
    }
  }

end TaskQueueRecoveryTest

package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.test.UniTest

import java.nio.file.Files

/**
  * Behavioral tests shared by the file-based and SQLite-backed flow run stores
  */
class FlowRunStoreTest extends UniTest:

  private def newStores(): List[(String, FlowRunStore)] =
    val dir = Files.createTempDirectory("wv-flow-store")
    List(
      "file"   -> FlowRunRegistry(dir.resolve("file")),
      "sqlite" -> SQLiteFlowRunStore(dir.resolve("registry.db"))
    )

  private def withStores(body: (String, FlowRunStore) => Unit): Unit = newStores().foreach {
    (kind, store) =>
      try body(kind, store)
      finally store.close()
  }

  private def record(
      runId: String,
      flowName: String,
      state: String,
      startedAt: Long
  ): FlowRunRecord = FlowRunRecord(
    runId,
    flowName,
    state,
    startedAt,
    finishedAtMillis =
      if state == FlowRunRecord.STATE_RUNNING then
        None
      else
        Some(startedAt + 10)
    ,
    stages = List(
      StageRunRecord("src", "success", 1, None, Some(s"__wv_flow_${runId}_src")),
      StageRunRecord("out", "failed", 2, Some("boom"), None)
    )
  )

  test("save, get, and list runs most recent first") {
    withStores { (kind, store) =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_FAILED, 100L))
      store.save(record("run2", "FlowB", FlowRunRecord.STATE_RUNNING, 200L))
      store.save(record("run3", "FlowA", FlowRunRecord.STATE_SUCCESS, 300L))

      val r1 = store.get("run1").getOrElse(fail(s"run1 not found in ${kind} store"))
      r1.flowName shouldBe "FlowA"
      r1.state shouldBe FlowRunRecord.STATE_FAILED
      r1.finishedAtMillis shouldBe Some(110L)
      r1.stages.map(_.name) shouldBe List("src", "out")
      r1.stages.head.table shouldBe Some("__wv_flow_run1_src")
      r1.stages.last.error shouldBe Some("boom")

      store.get("run2").get.finishedAtMillis shouldBe None
      store.list().map(_.runId) shouldBe List("run3", "run2", "run1")
      store.latestRunOf("FlowA").get.runId shouldBe "run3"
      store.latestRunOf("NoSuchFlow") shouldBe None
    }
  }

  test("overwrite a run record on save") {
    withStores { (kind, store) =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
      val updated = record("run1", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L).copy(stages =
        List(StageRunRecord("src", "success", 1))
      )
      store.save(updated)
      val r = store.get("run1").get
      r.state shouldBe FlowRunRecord.STATE_SUCCESS
      r.stages.size shouldBe 1
      store.list().size shouldBe 1
    }
  }

  test("track cancellation requests") {
    withStores { (kind, store) =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
      store.cancelRequested("run1") shouldBe false
      store.requestCancel("run1")
      store.cancelRequested("run1") shouldBe true
      store.clearCancelRequest("run1")
      store.cancelRequested("run1") shouldBe false
    }
  }

  test("delete run records") {
    withStores { (kind, store) =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L))
      store.requestCancel("run1")
      store.delete("run1")
      store.get("run1") shouldBe None
      store.cancelRequested("run1") shouldBe false
      store.list() shouldBe Nil
    }
  }

  test("claim run slots up to the concurrency limit") {
    withStores { (kind, store) =>
      val first = record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L)
      store.claimRunSlot(first, concurrencyLimit = 2) shouldBe true
      store.get("run1").get.stages.map(_.name) shouldBe List("src", "out")
      store.claimRunSlot(
        record("run2", "FlowA", FlowRunRecord.STATE_RUNNING, 200L),
        concurrencyLimit = 2
      ) shouldBe true
      // The limit is reached: the third concurrent run of FlowA is rejected
      store.claimRunSlot(
        record("run3", "FlowA", FlowRunRecord.STATE_RUNNING, 300L),
        concurrencyLimit = 2
      ) shouldBe false
      store.get("run3") shouldBe None
      // Other flows have their own slots
      store.claimRunSlot(
        record("runB", "FlowB", FlowRunRecord.STATE_RUNNING, 300L),
        concurrencyLimit = 1
      ) shouldBe true

      // Finishing a run frees its slot
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L))
      store.claimRunSlot(
        record("run3", "FlowA", FlowRunRecord.STATE_RUNNING, 300L),
        concurrencyLimit = 2
      ) shouldBe true
    }
  }

  test("persist and refresh run leases") {
    withStores { (kind, store) =>
      store.save(
        record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(leaseExpiresAtMillis =
          Some(1000L)
        )
      )
      store.get("run1").get.leaseExpiresAtMillis shouldBe Some(1000L)

      store.refreshLease("run1", 5000L)
      val r = store.get("run1").get
      r.leaseExpiresAtMillis shouldBe Some(5000L)
      // The stage records survive a lease refresh
      r.stages.map(_.name) shouldBe List("src", "out")
      r.isStaleAt(4000L) shouldBe false
      r.isStaleAt(6000L) shouldBe true
      r.effectiveStateAt(6000L) shouldBe FlowRunRecord.STATE_FAILED

      // Terminal records are never stale, regardless of their lease
      store.save(
        record("run2", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L).copy(leaseExpiresAtMillis =
          Some(10L)
        )
      )
      store.get("run2").get.isStaleAt(6000L) shouldBe false
      // A running record without a lease (e.g. written by an older version) is not stale
      store.save(record("run3", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
      store.get("run3").get.isStaleAt(6000L) shouldBe false
    }
  }

  test("free the run slot of a running record whose lease expired") {
    withStores { (kind, store) =>
      val now = System.currentTimeMillis()
      // A crashed process left this record running, but its lease has expired
      store.save(
        record("crashed", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(leaseExpiresAtMillis =
          Some(now - 10000)
        )
      )
      store.claimRunSlot(
        record("run2", "FlowA", FlowRunRecord.STATE_RUNNING, 200L).copy(leaseExpiresAtMillis =
          Some(now + 60000)
        ),
        concurrencyLimit = 1
      ) shouldBe true
      // A run holding a live lease still occupies its slot
      store.claimRunSlot(
        record("run3", "FlowA", FlowRunRecord.STATE_RUNNING, 300L).copy(leaseExpiresAtMillis =
          Some(now + 60000)
        ),
        concurrencyLimit = 1
      ) shouldBe false
    }
  }

  test("persist bound arguments and the logical run time") {
    withStores { (kind, store) =>
      val invoked = record("run1", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L).copy(
        args = Map("segment" -> "'a'", "min_id" -> "3"),
        runTimeMillis = Some(1234L)
      )
      store.save(invoked)
      val r = store.get("run1").getOrElse(fail(s"run1 not found in ${kind} store"))
      r.args shouldBe Map("segment" -> "'a'", "min_id" -> "3")
      r.runTimeMillis shouldBe Some(1234L)
      r.flowCallForm shouldBe "FlowA(min_id = 3, segment = 'a')"

      // Records without arguments keep an empty map and no run time
      store.save(record("run2", "FlowB", FlowRunRecord.STATE_SUCCESS, 200L))
      val plain = store.get("run2").get
      plain.args shouldBe Map.empty
      plain.runTimeMillis shouldBe None
      plain.flowCallForm shouldBe "FlowB"

      // claimRunSlot persists the invocation like save
      val claimed = record("run3", "FlowC", FlowRunRecord.STATE_RUNNING, 300L).copy(
        args = Map("segment" -> "'b'"),
        runTimeMillis = Some(5678L)
      )
      store.claimRunSlot(claimed, concurrencyLimit = 1) shouldBe true
      store.get("run3").get.args shouldBe Map("segment" -> "'b'")
      store.get("run3").get.runTimeMillis shouldBe Some(5678L)
    }
  }

  test("migrate a SQLite database created before the args and run_time columns") {
    val dir    = Files.createTempDirectory("wv-flow-store-migrate")
    val dbPath = dir.resolve("registry.db")
    // Create a database with the pre-args schema and one row
    val conn = java.sql.DriverManager.getConnection(s"jdbc:sqlite:${dbPath}")
    try
      val stmt = conn.createStatement()
      stmt.execute("""create table runs(
          |  run_id           text primary key,
          |  flow_name        text not null,
          |  state            text not null,
          |  started_at       integer not null,
          |  finished_at      integer,
          |  cancel_requested integer not null default 0,
          |  lease_expires_at integer
          |)""".stripMargin)
      stmt.execute(
        "insert into runs(run_id, flow_name, state, started_at) values('old1', 'FlowA', 'success', 100)"
      )
      stmt.close()
    finally
      conn.close()

    val store = SQLiteFlowRunStore(dbPath)
    try
      val old = store.get("old1").getOrElse(fail("old1 not found after migration"))
      old.args shouldBe Map.empty
      old.runTimeMillis shouldBe None
      // New records persist the added columns
      store.save(
        record("new1", "FlowA", FlowRunRecord.STATE_SUCCESS, 200L).copy(
          args = Map("segment" -> "'a'"),
          runTimeMillis = Some(999L)
        )
      )
      store.get("new1").get.args shouldBe Map("segment" -> "'a'")
      store.get("new1").get.runTimeMillis shouldBe Some(999L)
    finally
      store.close()
  }

  test("share state between store instances on the same path") {
    // Simulates cross-process observation: a second store instance sees runs and cancellation
    // requests written by the first one
    val dir = Files.createTempDirectory("wv-flow-store-shared")
    List(
      "file"   -> (() => FlowRunRegistry(dir.resolve("file"))),
      "sqlite" -> (() => SQLiteFlowRunStore(dir.resolve("registry.db")))
    ).foreach { (kind, newStore) =>
      val writer = newStore()
      val reader = newStore()
      try
        writer.save(record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
        reader.get("run1").isDefined shouldBe true
        reader.requestCancel("run1")
        writer.cancelRequested("run1") shouldBe true
      finally
        writer.close()
        reader.close()
    }
  }

  test("create stores via the factory") {
    val dir = Files.createTempDirectory("wv-flow-store-factory")
    // A .wv file makes the folder a wvlet workspace, so targetFolder stays under this temp dir
    Files.writeString(dir.resolve("dummy.wv"), "")
    val workEnv = WorkEnv(dir.toString)
    val file    = FlowRunStore.ofType(FlowRunStore.STORE_TYPE_FILE, workEnv)
    file.close()
    file.getClass shouldBe classOf[FlowRunRegistry]
    val sqlite = FlowRunStore.ofType(FlowRunStore.STORE_TYPE_SQLITE, workEnv)
    sqlite.close()
    sqlite.getClass shouldBe classOf[SQLiteFlowRunStore]
    val e = intercept[WvletLangException] {
      FlowRunStore.ofType("bogus", workEnv)
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

end FlowRunStoreTest

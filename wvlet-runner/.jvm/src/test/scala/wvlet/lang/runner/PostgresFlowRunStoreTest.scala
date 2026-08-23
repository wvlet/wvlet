package wvlet.lang.runner

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import wvlet.uni.test.UniTest

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/**
  * Tests for the PostgreSQL-backed flow run store, running against an in-process embedded
  * PostgreSQL server (no Docker required; real binaries resolved as test dependencies). Set
  * WVLET_TEST_PG_URL (+ _USER/_PASSWORD) to run against an external server instead. Auto-skips when
  * the embedded server cannot start in this environment
  */
class PostgresFlowRunStoreTest extends UniTest:

  private lazy val embedded: Option[EmbeddedPostgres] =
    try
      Some(EmbeddedPostgres.builder().start())
    catch
      case NonFatal(e) =>
        warn(s"Embedded PostgreSQL is not available: ${e.getMessage}")
        None

  override def afterAll: Unit = embedded.foreach(_.close())

  private def newStore(): Option[PostgresFlowRunStore] =
    sys.env.get("WVLET_TEST_PG_URL") match
      case Some(url) =>
        Some(
          PostgresFlowRunStore(
            url,
            sys.env.getOrElse("WVLET_TEST_PG_USER", "postgres"),
            sys.env.getOrElse("WVLET_TEST_PG_PASSWORD", "")
          )
        )
      case None =>
        embedded.map(pg =>
          PostgresFlowRunStore(pg.getJdbcUrl("postgres", "postgres"), "postgres", "")
        )

  /** Run the body against a store over a clean database, skipping when PostgreSQL is absent */
  private def withStore(body: PostgresFlowRunStore => Unit): Unit =
    newStore() match
      case None =>
        ignore("PostgreSQL is not available in this environment")
      case Some(store) =>
        try
          // The store constructor ensured the tables exist; empty them for test isolation
          store.list().foreach(r => store.delete(r.runId))
          body(store)
        finally
          store.close()

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
    withStore { store =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_FAILED, 100L))
      store.save(record("run2", "FlowB", FlowRunRecord.STATE_RUNNING, 200L))
      store.save(record("run3", "FlowA", FlowRunRecord.STATE_SUCCESS, 300L))

      val r1 = store.get("run1").getOrElse(fail("run1 not found"))
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
    withStore { store =>
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

  test("track and clear cancellation requests") {
    withStore { store =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
      store.cancelRequested("run1") shouldBe false
      store.requestCancel("run1")
      store.cancelRequested("run1") shouldBe true
      store.clearCancelRequest("run1")
      store.cancelRequested("run1") shouldBe false
    }
  }

  test("delete run records") {
    withStore { store =>
      store.save(record("run1", "FlowA", FlowRunRecord.STATE_SUCCESS, 100L))
      store.delete("run1")
      store.get("run1") shouldBe None
      store.list() shouldBe Nil
    }
  }

  test("claim run slots up to the concurrency limit, ignoring expired leases") {
    withStore { store =>
      val now = System.currentTimeMillis()
      store.claimRunSlot(
        record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(leaseExpiresAtMillis =
          Some(now + 60000)
        ),
        concurrencyLimit = 1
      ) shouldBe true
      // The limit is reached: a second concurrent run of FlowA is rejected
      store.claimRunSlot(
        record("run2", "FlowA", FlowRunRecord.STATE_RUNNING, 200L).copy(leaseExpiresAtMillis =
          Some(now + 60000)
        ),
        concurrencyLimit = 1
      ) shouldBe false
      store.get("run2") shouldBe None
      // Other flows have their own slots
      store.claimRunSlot(
        record("runB", "FlowB", FlowRunRecord.STATE_RUNNING, 200L),
        concurrencyLimit = 1
      ) shouldBe true
      // A running record whose lease expired belongs to a dead process and frees its slot
      store.save(
        record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(leaseExpiresAtMillis =
          Some(now - 10000)
        )
      )
      store.claimRunSlot(
        record("run3", "FlowA", FlowRunRecord.STATE_RUNNING, 300L).copy(leaseExpiresAtMillis =
          Some(now + 60000)
        ),
        concurrencyLimit = 1
      ) shouldBe true
    }
  }

  test("serialize concurrent slot claims from separate connections") {
    withStore { store =>
      newStore() match
        case None =>
          ignore("PostgreSQL is not available in this environment")
        case Some(other) =>
          try
            // Two processes (separate connections) race for a single slot of the same flow;
            // the advisory lock serializes the guarded inserts so exactly one claim wins
            val ready                                                   = CountDownLatch(2)
            val go                                                      = CountDownLatch(1)
            val claimed                                                 = AtomicInteger(0)
            val now                                                     = System.currentTimeMillis()
            def claimer(s: PostgresFlowRunStore, runId: String): Thread = Thread { () =>
              ready.countDown()
              go.await()
              val ok = s.claimRunSlot(
                record(runId, "RacedFlow", FlowRunRecord.STATE_RUNNING, now).copy(
                  leaseExpiresAtMillis = Some(now + 60000)
                ),
                concurrencyLimit = 1
              )
              if ok then
                claimed.incrementAndGet()
            }
            val t1 = claimer(store, "race1")
            val t2 = claimer(other, "race2")
            t1.start()
            t2.start()
            ready.await()
            go.countDown()
            t1.join()
            t2.join()
            claimed.get() shouldBe 1
            store.list().count(_.flowName == "RacedFlow") shouldBe 1
          finally
            other.close()
    }
  }

  test("refresh run leases in place") {
    withStore { store =>
      store.save(
        record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(leaseExpiresAtMillis =
          Some(1000L)
        )
      )
      store.refreshLease("run1", 5000L)
      val r = store.get("run1").get
      r.leaseExpiresAtMillis shouldBe Some(5000L)
      // The stage records survive a lease refresh
      r.stages.map(_.name) shouldBe List("src", "out")
    }
  }

  test("persist bound arguments, the logical run time, and sensor liveness fields") {
    withStore { store =>
      store.save(
        record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L).copy(
          args = Map("segment" -> "'a'", "min_id" -> "3"),
          runTimeMillis = Some(1234L),
          stages = List(
            StageRunRecord(
              "gate",
              "running",
              1,
              waitingSinceMillis = Some(150L),
              lastPollAtMillis = Some(170L)
            )
          )
        )
      )
      val r = store.get("run1").get
      r.args shouldBe Map("segment" -> "'a'", "min_id" -> "3")
      r.runTimeMillis shouldBe Some(1234L)
      r.flowCallForm shouldBe "FlowA(min_id = 3, segment = 'a')"
      r.stages.head.waitingSinceMillis shouldBe Some(150L)
      r.stages.head.lastPollAtMillis shouldBe Some(170L)
    }
  }

  test("share state between store instances on the same database") {
    withStore { store =>
      newStore() match
        case None =>
          ignore("PostgreSQL is not available in this environment")
        case Some(observer) =>
          try
            store.save(record("run1", "FlowA", FlowRunRecord.STATE_RUNNING, 100L))
            observer.get("run1").isDefined shouldBe true
            observer.requestCancel("run1")
            store.cancelRequested("run1") shouldBe true
          finally
            observer.close()
    }
  }

end PostgresFlowRunStoreTest

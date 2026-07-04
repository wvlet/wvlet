package wvlet.lang.runner

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.uni.test.UniTest

import java.nio.file.Files

class FlowRunRetentionTest extends UniTest:
  private val workEnv             = WorkEnv(".")
  private val dbConnectorProvider = DBConnectorProvider(workEnv)
  private val connector           = dbConnectorProvider.getConnector(Profile.defaultDuckDBProfile)

  override def afterAll: Unit = dbConnectorProvider.close()

  // Note: an explicit registry path — WorkEnv.targetFolder of a folder without .wv files
  // falls back to the shared ~/.cache/wvlet/target, which would leak records across tests
  private def newStore(): FlowRunStore = FlowRunRegistry(
    Files.createTempDirectory("wvlet-retention-test")
  )

  private def terminalRun(
      runId: String,
      flow: String,
      startedAt: Long,
      finishedAt: Long,
      stage: String = "src"
  ): FlowRunRecord = FlowRunRecord(
    runId,
    flow,
    FlowRunRecord.STATE_SUCCESS,
    startedAt,
    Some(finishedAt),
    stages = List(StageRunRecord(stage, "success", 1))
  )

  private def createStageTable(runId: String, stage: String): String =
    val table = FlowExecutor.stageTableName(runId, stage)
    connector.execute(s"""create or replace table "${table}" as select 1 as id""")
    table

  private def tableExists(table: String): Boolean =
    connector.runQuery(
      s"select count(*) cnt from information_schema.tables where table_name = '${table}'"
    ) { rs =>
      rs.next()
      rs.getLong(1) > 0
    }

  test("keep the latest terminal run and delete runs beyond keep_runs") {
    val store  = newStore()
    val tables = (1 to 3).map { i =>
      store.save(terminalRun(s"keeprun${i}", "F", startedAt = i * 1000L, finishedAt = i * 1000L))
      createStageTable(s"keeprun${i}", "src")
    }

    val summary = FlowRunRetention.sweep(store, connector, keepRunsOf = _ => Some(1))
    summary.deletedRuns shouldBe 2
    store.list().map(_.runId) shouldBe List("keeprun3")
    tableExists(tables(2)) shouldBe true
    tableExists(tables(0)) shouldBe false
    tableExists(tables(1)) shouldBe false
  }

  test("delete terminal runs older than the ttl but never the latest") {
    val store = newStore()
    store.save(terminalRun("ttlrun1", "F", startedAt = 1000L, finishedAt = 1000L))
    store.save(terminalRun("ttlrun2", "F", startedAt = 2000L, finishedAt = 2000L))

    // Both runs are far older than the ttl, but the latest terminal run survives because
    // cross-flow dependencies read it via latestRunOf
    val summary = FlowRunRetention.sweep(
      store,
      connector,
      ttlMillis = Some(1000L),
      nowMillis = 1_000_000L
    )
    summary.deletedRuns shouldBe 1
    store.list().map(_.runId) shouldBe List("ttlrun2")
  }

  test("finalize stale running records and keep live ones") {
    val store = newStore()
    val now   = 100_000L
    store.save(
      FlowRunRecord(
        "stalerun",
        "F",
        FlowRunRecord.STATE_RUNNING,
        startedAtMillis = now - 60_000,
        leaseExpiresAtMillis = Some(now - 30_000)
      )
    )
    store.save(
      FlowRunRecord(
        "liverun",
        "F",
        FlowRunRecord.STATE_RUNNING,
        startedAtMillis = now,
        leaseExpiresAtMillis = Some(now + 60_000)
      )
    )

    val summary = FlowRunRetention.sweep(store, connector, nowMillis = now)
    summary.finalizedStale shouldBe 1
    summary.deletedRuns shouldBe 0
    store.get("stalerun").map(_.state) shouldBe Some(FlowRunRecord.STATE_FAILED)
    store.get("liverun").map(_.state) shouldBe Some(FlowRunRecord.STATE_RUNNING)
  }

  test("retention applies to runs finalized in the same sweep") {
    val store = newStore()
    val now   = 100_000L
    store.save(terminalRun("newterm", "F", startedAt = now - 1000, finishedAt = now - 1000))
    store.save(
      FlowRunRecord(
        "oldstale",
        "F",
        FlowRunRecord.STATE_RUNNING,
        startedAtMillis = now - 60_000,
        leaseExpiresAtMillis = Some(now - 30_000)
      )
    )

    // The stale run is finalized as failed, ranks below the newer terminal run, and is
    // deleted by the keep_runs cap in the same sweep
    val summary = FlowRunRetention.sweep(
      store,
      connector,
      keepRunsOf = _ => Some(1),
      nowMillis = now
    )
    summary.finalizedStale shouldBe 1
    summary.deletedRuns shouldBe 1
    store.list().map(_.runId) shouldBe List("newterm")
  }

end FlowRunRetentionTest

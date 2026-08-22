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
package wvlet.lang.tablestore.catalog

import wvlet.lang.tablestore.catalog.{PendingColumnStat, PendingFileEntry}
import wvlet.lang.tablestore.{DataFormat, EntryKind, SnapshotKind, TableOptions}
import wvlet.lang.tablestore.schema.{ColumnDesc, ColumnType}
import wvlet.uni.test.UniTest
import wvlet.uni.test.empty

/**
  * Deterministic protocol conformance suite shared by every catalog driver. The embedded profile
  * never exercises lease contention by accident — its correctness confidence must come from these
  * fault-injection tests (zombie mergers, double retirement, crash idempotency).
  */
trait CatalogProtocolSpec extends UniTest:
  protected def newCatalog: CatalogStore

  /** A catalog with a fresh 'main' database */
  protected def freshCatalog: CatalogStore =
    val c = newCatalog
    c.initialize()
    c.createDatabase("main")
    c

  private def pending(
      id: Long,
      rows: Long = 10L,
      schemaJson: String = """{"columns":[{"name":"id","type":"long"}]}"""
  ): PendingFileEntry = PendingFileEntry(
    entryId = id,
    s3Key = s"raw/table=1/${id}.jsonl",
    format = DataFormat.Jsonl,
    checksum = f"checksum-${id}%04d",
    rowCount = rows,
    byteSize = rows * 20L,
    minEventTs = None,
    maxEventTs = None,
    observedSchemaJson = schemaJson,
    stats = Seq(PendingColumnStat("id", Some("0"), Some(rows.toString), 0, None))
  )

  test("allocate monotonic file id ranges") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable("main", "ids", TableOptions.default)
      val ids1  = catalog.allocateFileIds(table.id, 3)
      val ids2  = catalog.allocateFileIds(table.id, 2)
      ids1 shouldBe Seq(ids1.head, ids1.head + 1, ids1.head + 2)
      (ids2.toSet & ids1.toSet) shouldBe empty
      (ids2.max > ids1.max) shouldBe true
    finally
      catalog.close()
  }

  test("register ingest atomically and expose entries immediately") {
    val catalog = freshCatalog
    try
      val table  = catalog.createTable("main", "events", TableOptions.default)
      val ids    = catalog.allocateFileIds(table.id, 2)
      val result = catalog.registerIngest(table.id, "writer-1", ids.map(id => pending(id)))
      result.snapshotId.isDefined shouldBe true
      result.registeredEntryIds shouldBe ids
      result.skippedEntryIds shouldBe empty

      val snapId = result.snapshotId.get
      val live   = catalog.liveEntries(table.id, snapId)
      live.map(_.id) shouldBe ids
      live.foreach(e =>
        e.isVisibleAt(snapId) shouldBe true
        e.endSnapshot shouldBe None
        e.kind shouldBe EntryKind.File
      )
      // Stats registered in the same transaction
      catalog.statsFor(ids).size shouldBe 2
    finally
      catalog.close()
  }

  test("re-registering issued ids is a no-op (crash idempotency)") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable("main", "events", TableOptions.default)
      val ids   = catalog.allocateFileIds(table.id, 1)
      val first = catalog.registerIngest(table.id, "writer-1", Seq(pending(ids.head)))
      // Simulate a retry after a crash before the writer received the ack
      val retry = catalog.registerIngest(table.id, "writer-1", Seq(pending(ids.head)))
      retry.snapshotId shouldBe None
      retry.skippedEntryIds shouldBe Seq(ids.head)
      retry.registeredEntryIds shouldBe empty
      catalog.snapshotsOf(table.id).size shouldBe 1
    finally
      catalog.close()
  }

  test("interval-encoded liveness resolves AS OF reads") {
    val catalog = freshCatalog
    try
      val table             = catalog.createTable("main", "events", TableOptions.default)
      val idA :: idB :: Nil = catalog.allocateFileIds(table.id, 2).toList: @unchecked
      val s1 = catalog.registerIngest(table.id, "w", Seq(pending(idA))).snapshotId.get
      val s2 = catalog.registerIngest(table.id, "w", Seq(pending(idB))).snapshotId.get

      catalog.liveEntries(table.id, s1).map(_.id) shouldBe Seq(idA)
      catalog.liveEntries(table.id, s2).map(_.id).toSet shouldBe Set(idA, idB)

      // Retire A at snapshot s3 (folding A into a merged file) and check both sides of the interval
      val lease    = catalog.acquireLease("test-retire:all", Some(table.id), "retirer", 60000)
      val mergedId = catalog.allocateFileIds(table.id, 1).head
      val s3       =
        catalog
          .commitMerge(
            MergeCommit(
              leaseName = "test-retire:all",
              fencingToken = lease.fencingToken,
              tableId = table.id,
              sourceEntryIds = Seq(idA),
              mergedEntry = pending(mergedId),
              escalatedSchema = None,
              writer = "retirer"
            )
          )
          .snapshotId
      catalog.releaseLease("test-retire:all", "retirer", lease.fencingToken)

      catalog.liveEntries(table.id, s1).map(_.id) shouldBe Seq(idA)
      catalog.liveEntries(table.id, s2).map(_.id).toSet shouldBe Set(idA, idB)
      // At s3 both B and the new merged entry are live; A is retired
      catalog.liveEntries(table.id, s3).map(_.id).toSet shouldBe Set(idB, mergedId)
    finally
      catalog.close()
    end try
  }

  test("fencing tokens increase monotonically and stale tokens cannot commit") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable("main", "events", TableOptions.default)

      val l1 = catalog.acquireLease("merge:t1:r1", Some(table.id), "merger-1", ttlMillis = 60000)
      // A second acquirer cannot take a held lease
      intercept[LeaseHeldException] {
        catalog.acquireLease("merge:t1:r1", Some(table.id), "merger-2", ttlMillis = 60000)
      }
      catalog.releaseLease("merge:t1:r1", "merger-1", l1.fencingToken)

      // Take over with a strictly higher token — the zombie's token is now stale forever
      val l2 = catalog.acquireLease("merge:t1:r1", Some(table.id), "merger-2", ttlMillis = 60000)
      (l2.fencingToken > l1.fencingToken) shouldBe true

      // Zombie merger tries to commit with the stale token: rejected inside the transaction
      val zombieIds = catalog.allocateFileIds(table.id, 1)
      val zReg      = catalog.registerIngest(table.id, "zombie", Seq(pending(zombieIds.head)))
      intercept[LeaseLostException] {
        catalog.commitMerge(
          mergeCommit(
            table.id,
            "merge:t1:r1",
            l1.fencingToken,
            sources = Seq.empty,
            mergedEntryId = catalog.allocateFileIds(table.id, 1).head,
            afterSnapshot = zReg.snapshotId.getOrElse(0L)
          )
        )
      }
    finally
      catalog.close()
    end try
  }

  test("conditional retirement asserts row counts so no entry retires twice") {
    val catalog = freshCatalog
    try
      val table             = catalog.createTable("main", "events", TableOptions.default)
      val idA :: idB :: Nil = catalog.allocateFileIds(table.id, 2).toList: @unchecked
      catalog.registerIngest(table.id, "w", Seq(pending(idA), pending(idB)))
      val lease = catalog.acquireLease("merge:t1:all", Some(table.id), "m1", 60000)

      val mergedId = catalog.allocateFileIds(table.id, 1).head
      val commit   = MergeCommit(
        leaseName = "merge:t1:all",
        fencingToken = lease.fencingToken,
        tableId = table.id,
        sourceEntryIds = Seq(idA),
        mergedEntry = pending(mergedId),
        escalatedSchema = None,
        writer = "m1"
      )
      val result = catalog.commitMerge(commit)
      result.retiredEntryIds shouldBe Seq(idA)
      catalog.entry(idA).get.endSnapshot.isDefined shouldBe true

      // Retiring A again within one more merge of [A, B] must abort: A is already retired, so the
      // conditional UPDATE touches fewer rows than asserted
      val secondMergedId = catalog.allocateFileIds(table.id, 1).head
      intercept[RetireConflictException] {
        catalog.commitMerge(
          commit.copy(sourceEntryIds = Seq(idA, idB), mergedEntry = pending(secondMergedId))
        )
      }
      // Nothing from the aborted transaction leaked
      catalog.entry(secondMergedId) shouldBe None
    finally
      catalog.close()
    end try
  }

  test("commit verifies the token even when sources are empty") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable("main", "events", TableOptions.default)
      val lease = catalog.acquireLease("merge:t1:x", Some(table.id), "m1", 60000)
      val ok    = catalog.commitMerge(
        MergeCommit(
          leaseName = "merge:t1:x",
          fencingToken = lease.fencingToken,
          tableId = table.id,
          sourceEntryIds = Seq.empty,
          mergedEntry = pending(catalog.allocateFileIds(table.id, 1).head),
          escalatedSchema = None,
          writer = "m1"
        )
      )
      (ok.snapshotId > 0L) shouldBe true
    finally
      catalog.close()
  }

  test("pins expire and only active pins block retirement of visible files") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable("main", "events", TableOptions.default)
      val idA   = catalog.allocateFileIds(table.id, 1).head
      val s1    = catalog.registerIngest(table.id, "w", Seq(pending(idA))).snapshotId.get

      catalog.pinSnapshot(table.id, s1, "reader-1", ttlMillis = -1) // already expired
      val expiredPinDeletion = catalog.deletableEntries(table.id).filter(_.id == idA)
      // Not retired yet, so still not deletable
      expiredPinDeletion shouldBe empty

      val s2 = retire(catalog, table.id, Seq(idA))
      // With no pin, the retired file is deletable...
      catalog.deletableEntries(table.id).map(_.id) shouldBe Seq(idA)

      // ...but a fresh pin holding s1 (which still sees A) protects it
      catalog.pinSnapshot(table.id, s1, "reader-2", ttlMillis = 60_000)
      catalog.deletableEntries(table.id).map(_.id) shouldBe empty

      // A pin at s2 (where A is invisible) does not protect it
      catalog.releasePin(table.id, "reader-2")
      catalog.pinSnapshot(table.id, s2, "reader-3", ttlMillis = 60_000)
      catalog.deletableEntries(table.id).map(_.id) shouldBe Seq(idA)
    finally
      catalog.close()
  }

  test("schema versions publish through commits") {
    val catalog = freshCatalog
    try
      val table = catalog.createTable(
        "main",
        "events",
        TableOptions.default,
        initialColumns = Seq(ColumnDesc("id", ColumnType.LongType))
      )
      table.schemaVersionHead shouldBe 1
      catalog.schemaVersionsOf(table.id).map(_.version) shouldBe Seq(1)

      val lease    = catalog.acquireLease("merge:t1:s", Some(table.id), "m1", 60000)
      val mergedId = catalog.allocateFileIds(table.id, 1).head
      val widened  = wvlet
        .lang
        .tablestore
        .schema
        .TableSchema(
          2,
          Seq(ColumnDesc("id", ColumnType.LongType), ColumnDesc("extra", ColumnType.StringType))
        )
      val commit = MergeCommit(
        leaseName = "merge:t1:s",
        fencingToken = lease.fencingToken,
        tableId = table.id,
        sourceEntryIds = Seq.empty,
        mergedEntry = pending(mergedId),
        escalatedSchema = Some(widened),
        writer = "m1"
      )
      val result = catalog.commitMerge(commit)
      result.newSchemaVersion shouldBe Some(2)
      catalog.getTable(table.id).schemaVersionHead shouldBe 2
      catalog.schemaVersionsOf(table.id).map(_.version) shouldBe Seq(1, 2)
    finally
      catalog.close()
    end try
  }

  /** Retire `entryIds` under a fresh lease, returning the publishing snapshot id */
  private def retire(catalog: CatalogStore, tableId: Long, entryIds: Seq[Long]): Long =
    val leaseName = s"test-retire:${tableId}:${entryIds.mkString("-")}"
    val lease     = catalog.acquireLease(leaseName, Some(tableId), "retirer", 60000)
    try
      val mergedId = catalog.allocateFileIds(tableId, 1).head
      val latest   = catalog.latestSnapshot(tableId)
      catalog
        .commitMerge(
          mergeCommit(
            tableId,
            leaseName,
            lease.fencingToken,
            entryIds,
            mergedId,
            latest.fold(0L)(_.id)
          )
        )
        .snapshotId
    finally
      catalog.releaseLease(leaseName, "retirer", lease.fencingToken)

  private def mergeCommit(
      tableId: Long,
      leaseName: String,
      token: Long,
      sources: Seq[Long],
      mergedEntryId: Long,
      afterSnapshot: Long
  ): MergeCommit = MergeCommit(
    leaseName = leaseName,
    fencingToken = token,
    tableId = tableId,
    sourceEntryIds = sources,
    mergedEntry = pending(mergedEntryId),
    escalatedSchema = None,
    writer = "test-writer"
  )

end CatalogProtocolSpec

/** Conformance run on SQLite — the dev/test backend of the embedded profile */
class SQLiteCatalogProtocolTest extends CatalogProtocolSpec:
  override protected def newCatalog: CatalogStore = CatalogDrivers.inMemorySqlite()

/** Conformance run on DuckDB — the embedded-profile engine backend */
class DuckDBCatalogProtocolTest extends CatalogProtocolSpec:
  override protected def newCatalog: CatalogStore = CatalogDrivers.inMemoryDuckDB()

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

import wvlet.lang.api.StatusCode
import wvlet.lang.tablestore.schema.ColumnDesc
import wvlet.lang.tablestore.{
  CatalogTable,
  ColumnStat,
  DataFormat,
  EntryId,
  FileEntry,
  Lease,
  SchemaVersion,
  SchemaVersionId,
  Snapshot,
  SnapshotId,
  SnapshotKind,
  SnapshotPin,
  TableOptions,
  TableStoreException
}

/** A database (namespace) of tables */
case class Database(id: Long, name: String, createdAt: Long)

/**
  * A file prepared for registration. `entryId` comes from [[CatalogStore.allocateFileIds]] and
  * names the object in the store before registration, making retries after a crash no-ops.
  */
case class PendingFileEntry(
    entryId: EntryId,
    s3Key: String,
    format: DataFormat,
    checksum: String,
    rowCount: Long,
    byteSize: Long,
    minEventTs: Option[Long],
    maxEventTs: Option[Long],
    observedSchemaJson: String,
    stats: Seq[PendingColumnStat]
)

/** Advisory per-column statistics registered together with their file */
case class PendingColumnStat(
    columnName: String,
    minValue: Option[String],
    maxValue: Option[String],
    nullCount: Long,
    distinctEstimate: Option[Long]
)

case class IngestResult(
    /** The snapshot carrying the new entries; None when everything was already registered */
    snapshotId: Option[SnapshotId],
    registeredEntryIds: Seq[EntryId],
    skippedEntryIds: Seq[EntryId]
)

/**
  * One atomic merge commit: fence-check the lease, retire sources conditionally, register the
  * merged file, escalate the schema if widened, and publish the snapshot — all in one transaction.
  */
case class MergeCommit(
    leaseName: String,
    fencingToken: Long,
    tableId: Long,
    sourceEntryIds: Seq[EntryId],
    mergedEntry: PendingFileEntry,
    /** Some(schema) publishes a new schema version inside the same transaction */
    escalatedSchema: Option[wvlet.lang.tablestore.schema.TableSchema],
    writer: String
)

case class MergeCommitResult(
    snapshotId: SnapshotId,
    mergedEntryId: EntryId,
    newSchemaVersion: Option[SchemaVersionId],
    retiredEntryIds: Seq[EntryId]
)

class LeaseHeldException(leaseName: String, holder: String)
    extends TableStoreException(s"Lease '${leaseName}' is currently held by '${holder}'")

/** Thrown when the commit transaction finds the fencing token stale — the zombie loses */
class LeaseLostException(leaseName: String, fencingToken: Long)
    extends TableStoreException(
      s"Fencing token ${fencingToken} no longer matches lease '${leaseName}'"
    )

class RetireConflictException(expected: Int, actual: Int)
    extends TableStoreException(
      s"Conditional retirement touched ${actual} rows, expected ${expected}; aborting to avoid double-folding"
    )

/**
  * The transactional catalog of the table store: table metadata, the snapshot/file-entry inventory,
  * leases, pins, and stats. Implementations must provide serializable (or equivalent single-writer)
  * isolation for retiring transactions, monotonic per-table sequences, and TEXT-JSON metadata
  * columns only — no arrays, no JSONB (see the portability constraints in the design note).
  */
trait CatalogStore extends AutoCloseable:

  /** Create catalog tables if missing */
  def initialize(): Unit

  // ---- Databases and tables ----
  def createDatabase(name: String): Database
  def findDatabase(name: String): Option[Database]

  /**
    * Register a table and its schema head in one transaction. Declared `initialColumns` publish as
    * schema version 1; an empty declaration leaves version 0 (no published columns) until the first
    * merge escalates the schema from observed data.
    */
  def createTable(
      databaseName: String,
      name: String,
      options: TableOptions = TableOptions.default,
      initialColumns: Seq[ColumnDesc] = Nil
  ): CatalogTable

  def findTable(databaseName: String, name: String): Option[CatalogTable]
  def getTable(tableId: Long): CatalogTable

  // ---- Sequences ----

  /**
    * Pre-issue `count` monotonically increasing file ids for a writer session. Ids survive crashes:
    * registering a previously issued id again is a no-op.
    */
  def allocateFileIds(tableId: Long, count: Int): Seq[EntryId]

  // ---- Ingest ----

  /**
    * Register uploaded files in one transaction: allocate a snapshot id, insert the snapshot row
    * and the file entries with `begin_snapshot` = that id. Data is readable immediately after this
    * returns. Re-registering an already known entry id skips that entry (crash idempotency).
    */
  def registerIngest(tableId: Long, writer: String, entries: Seq[PendingFileEntry]): IngestResult

  // ---- Reads ----

  def latestSnapshot(tableId: Long): Option[Snapshot]
  def snapshotOf(tableId: Long, snapshotId: SnapshotId): Option[Snapshot]
  def snapshotsOf(tableId: Long): Seq[Snapshot]
  def schemaVersionsOf(tableId: Long): Seq[SchemaVersion]

  /** Entries visible at `snapshotId` under the interval predicate */
  def liveEntries(tableId: Long, snapshotId: SnapshotId): Seq[FileEntry]
  def entry(entryId: EntryId): Option[FileEntry]

  /** Stats for the given entries — advisory pruning input */
  def statsFor(fileIds: Seq[EntryId]): Seq[ColumnStat]

  // ---- Leases ----

  /**
    * Acquire the named lease, taking over expired ones. Each acquisition mints a fresh,
    * monotonically increasing fencing token.
    */
  def acquireLease(name: String, tableId: Option[Long], holder: String, ttlMillis: Long): Lease

  /** Extend a held lease; fails with [[LeaseLostException]] if the token is stale */
  def renewLease(name: String, holder: String, fencingToken: Long, ttlMillis: Long): Lease

  def releaseLease(name: String, holder: String, fencingToken: Long): Unit
  def leaseOf(name: String): Option[Lease]

  // ---- Merge commit ----

  /**
    * Execute a [[MergeCommit]] atomically. The fencing token is verified inside the transaction; a
    * stale token throws [[LeaseLostException]]. Source retirement is conditional on
    * `end_snapshot IS NULL` and asserted by row count, so no entry ever retires twice.
    */
  def commitMerge(commit: MergeCommit): MergeCommitResult

  // ---- Snapshot pins ----

  def pinSnapshot(tableId: Long, snapshotId: SnapshotId, holder: String, ttlMillis: Long): Unit
  def renewPin(tableId: Long, holder: String, ttlMillis: Long): Unit
  def releasePin(tableId: Long, holder: String): Unit
  def activePins(tableId: Long): Seq[SnapshotPin]

  // ---- GC helpers ----

  /** All keys referenced by the catalog — orphan detection subtracts them from a store inventory */
  def referencedKeys(tableId: Long): Set[String]

  /**
    * Retired entries whose files no active pin can see anymore — safe to delete from the store
    * (after checking no other table references them).
    */
  def deletableEntries(tableId: Long): Seq[FileEntry]

end CatalogStore

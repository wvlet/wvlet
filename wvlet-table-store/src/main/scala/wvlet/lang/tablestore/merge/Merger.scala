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
package wvlet.lang.tablestore.merge

import wvlet.lang.tablestore.catalog.{MergeCommit, PendingFileEntry}
import wvlet.lang.tablestore.{
  CatalogTable,
  ColumnStats,
  DataRow,
  DataFormat,
  EntryId,
  SnapshotId,
  TableStore,
  TableStoreException
}
import wvlet.lang.tablestore.format.{JsonlFile, ParquetFile}
import wvlet.lang.tablestore.objectstore.Checksum
import wvlet.lang.tablestore.schema.{ObservedSchema, SchemaEscalation}
import wvlet.uni.log.LogSupport

case class MergePolicy(
    /** Merge at most this many live small entries per run */
    maxSourceFiles: Int = 64,
    /** Entries with at most this many rows count as "small" (merge candidates) */
    smallEntryRows: Long = 50_000L,
    /** Skip merging when fewer than this many candidate entries are live */
    minSourceFiles: Int = 2,
    /** Quarantine threshold forwarded to schema escalation */
    outlierThreshold: Double = SchemaEscalation.defaultOutlierThreshold
)

sealed trait MergeOutcome
case object NothingToMerge extends MergeOutcome

case class Merged(
    snapshotId: SnapshotId,
    mergedEntryId: EntryId,
    sourceEntryIds: Seq[EntryId],
    newSchemaVersion: Option[Long],
    quarantinedEntryIds: Seq[EntryId],
    rowCount: Long
) extends MergeOutcome

/**
  * The leased background merger: rewrites many small entries into one read-optimized Parquet file
  * and escalates the table schema from the same observed contents — one pass, two purposes.
  *
  * The merge lease is the only writer that retires entries or publishes schema versions; ingest
  * only adds. The fencing token is verified inside the commit transaction, so a stalled merger
  * whose lease was taken over can never publish stale state.
  */
case class Merger(store: TableStore, writerId: String) extends LogSupport:

  /**
    * Run one merge round over a partition range of the table. `leaseName` scopes exclusivity, e.g.
    * `merge:<tableId>:<time-range>` — leasing per range lets large tables merge in parallel.
    */
  def mergeOnce(
      table: CatalogTable,
      leaseName: String,
      policy: MergePolicy = MergePolicy(),
      leaseTtlMillis: Long = 60_000L
  ): MergeOutcome =
    val catalog = store.catalog

    val lease = catalog.acquireLease(leaseName, Some(table.id), writerId, leaseTtlMillis)
    try
      val latest = catalog
        .latestSnapshot(table.id)
        .getOrElse {
          return NothingToMerge
        }
      val candidates = catalog
        .liveEntries(table.id, latest.id)
        .filter(e => e.format == DataFormat.Jsonl && e.rowCount <= policy.smallEntryRows)
        .sortBy(e => (e.rowCount, e.id))
        .take(policy.maxSourceFiles)
      if candidates.size < policy.minSourceFiles then
        return NothingToMerge

      // Escalate from catalog metadata alone: no data read needed to know the target schema
      val currentSchema = currentPublishedSchema(table)
      val escalation    = SchemaEscalation.escalate(
        currentSchema,
        nextVersion = math.max(currentSchema.version, table.schemaVersionHead) + 1,
        files = candidates.map(e =>
          (e.id, e.rowCount, ObservedSchema.fromJson(e.observedSchemaJson))
        ),
        outlierThreshold = policy.outlierThreshold
      )
      if escalation.quarantinedFiles.nonEmpty then
        warn(
          s"Quarantining ${escalation.quarantinedFiles.size} file(s) of table '${table
              .name}' for review: ${escalation.quarantinedFiles}"
        )

      val sources = candidates.filterNot(e => escalation.quarantinedFiles.contains(e.id))
      if sources.isEmpty then
        warn(s"All merge candidates of '${table.name}' were quarantined; skipping")
        return NothingToMerge

      // Read every surviving source — the only full read of the raw data
      val rows = sources.flatMap { entry =>
        val bytes    = store.objects.get(entry.s3Key)
        val checksum = Checksum.sha256Hex(bytes)
        if checksum != entry.checksum then
          throw TableStoreException(
            s"Checksum mismatch on ${entry.s3Key}: expected ${entry.checksum}, got ${checksum}"
          )
        JsonlFile.decode(bytes)
      }

      val targetSchema = escalation.escalatedSchema.getOrElse(currentSchema)

      // Write merged Parquet content-addressed by SHA-256
      val tmpFile            = java.nio.file.Files.createTempFile("wvlet-merged-", ".parquet")
      val bytes: Array[Byte] =
        try
          ParquetFile.write(rows, targetSchema, tmpFile.toString)
          java.nio.file.Files.readAllBytes(tmpFile)
        finally
          java.nio.file.Files.deleteIfExists(tmpFile)
      val sha256 = Checksum.sha256Hex(bytes)
      val key    = TableStore.mergedKey(table.id, sha256)
      val put    = store.objects.put(key, bytes)
      if put.checksum != sha256 then
        throw TableStoreException(s"Object store corrupted the merged write of ${key}")

      val eventTsBounds      = boundsOf(rows, table.options.eventTimeColumn)
      val stats              = ColumnStats.collect(rows, targetSchema)
      val pendingMergedEntry = PendingFileEntry(
        entryId = reserveOneEntryId(table.id),
        s3Key = key,
        format = DataFormat.Parquet,
        checksum = sha256,
        rowCount = rows.size.toLong,
        byteSize = bytes.length.toLong,
        minEventTs = eventTsBounds._1,
        maxEventTs = eventTsBounds._2,
        observedSchemaJson = targetSchema.schemaJson,
        stats = stats
      )

      val result = catalog.commitMerge(
        MergeCommit(
          leaseName = leaseName,
          fencingToken = lease.fencingToken,
          tableId = table.id,
          sourceEntryIds = sources.map(_.id),
          mergedEntry = pendingMergedEntry,
          escalatedSchema = escalation.escalatedSchema,
          writer = writerId
        )
      )
      debug(
        s"Merged ${sources.size} entries into ${key} at snapshot ${result.snapshotId}" +
          escalation
            .escalatedSchema
            .map(schema => s", published schema v${schema.version}")
            .getOrElse("")
      )
      Merged(
        result.snapshotId,
        result.mergedEntryId,
        result.retiredEntryIds,
        result.newSchemaVersion,
        escalation.quarantinedFiles,
        rows.size.toLong
      )
    finally
      catalog.releaseLease(leaseName, writerId, lease.fencingToken)
    end try
  end mergeOnce

  private def currentPublishedSchema(
      table: CatalogTable
  ): wvlet.lang.tablestore.schema.TableSchema =
    if table.schemaVersionHead == 0 then
      wvlet.lang.tablestore.schema.TableSchema.empty
    else
      store.catalog.schemaVersionsOf(table.id).find(_.version == table.schemaVersionHead) match
        case Some(v) =>
          wvlet.lang.tablestore.schema.TableSchema.fromJson(v.version, v.schemaJson)
        case None =>
          throw TableStoreException(
            s"Schema version ${table.schemaVersionHead} of table '${table.name}' is missing"
          )

  private def reserveOneEntryId(tableId: EntryId): EntryId =
    store.catalog.allocateFileIds(tableId, 1).head

  private def boundsOf(
      rows: Seq[DataRow],
      eventTimeColumn: Option[String]
  ): (Option[Long], Option[Long]) =
    eventTimeColumn match
      case None =>
        (None, None)
      case Some(col) =>
        val ts = rows.flatMap(
          _.get(col)
            .collect { case wvlet.uni.json.JSON.JSONLong(v) =>
              v
            }
        )
        if ts.isEmpty then
          (None, None)
        else
          (Some(ts.min), Some(ts.max))

end Merger

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
package wvlet.lang.tablestore

import wvlet.lang.tablestore.catalog.{CatalogStore, PendingColumnStat, PendingFileEntry}
import wvlet.lang.tablestore.format.JsonlFile
import wvlet.lang.tablestore.objectstore.Checksum
import wvlet.lang.tablestore.schema.{ColumnDesc, ObservedSchema, TableSchema}
import wvlet.uni.json.JSON
import wvlet.uni.log.LogSupport

import scala.collection.mutable.ArrayBuffer

case class IngestConfig(
    /** Rotate the local segment after this many rows */
    maxRowsPerFile: Long = 100_000L,
    /** Rotate the local segment after this many buffered bytes */
    maxBytesPerFile: Long = 32L * 1024 * 1024,
    /** File ids are pre-issued from the catalog in batches of this size */
    fileIdBatchSize: Int = 16
)

/**
  * A high-frequency append session for one table.
  *
  * Rows buffer in a rotating local segment; on rotation the writer uploads `raw/.../<fileId>.jsonl`
  * and registers it in one catalog transaction — the snapshot id is allocated inside that
  * transaction from a catalog counter, so concurrent ingest writers never contend on a CAS, and
  * ingest costs one transaction per file batch, never per row. The data is readable as soon as
  * registration returns: there is no separate live/streaming tier; freshness is bounded by segment
  * rotation cadence.
  *
  * File ids are pre-issued in batches, so a crashed session's retries are no-ops: registering an
  * already-registered id again changes nothing (crash idempotency).
  */
class IngestSession(
    store: TableStore,
    table: CatalogTable,
    writerId: String,
    config: IngestConfig = IngestConfig()
) extends AutoCloseable
    with LogSupport:

  private val buffer        = ArrayBuffer[DataRow]()
  private var bufferedBytes = 0L
  private val idPool        = ArrayBuffer.empty[EntryId]
  private val snapshotIds   = ArrayBuffer.empty[SnapshotId]
  private val entryIds      = ArrayBuffer.empty[EntryId]
  private var totalRows     = 0L
  private var totalBytes    = 0L

  private def catalog: CatalogStore = store.catalog

  /** Append one row (a JSON object). Rotates the segment when it reaches the configured bounds */
  def add(row: DataRow): Unit = synchronized {
    buffer += row
    bufferedBytes += row.toJSON.length.toLong + 1L
    totalRows += 1
    if buffer.size.toLong >= config.maxRowsPerFile || bufferedBytes >= config.maxBytesPerFile then
      rotate()
  }

  def addAll(rows: Seq[DataRow]): Unit = rows.foreach(add)

  /** Upload and register whatever is currently buffered */
  def flush(): Option[SnapshotId] = synchronized {
    rotate()
    snapshotIds.lastOption
  }

  def ingestedEntryIds: Seq[EntryId]      = entryIds.toList
  def flushedSnapshotIds: Seq[SnapshotId] = snapshotIds.toList
  def rowCount: Long                      = totalRows
  def byteSize: Long                      = totalBytes

  /**
    * Flush and stop. Sessions are cheap; open one per writer loop rather than reusing across
    * processes.
    */
  override def close(): Unit = flush()

  private def rotate(): Unit =
    if buffer.isEmpty then
      ()
    else
      val entryId = nextEntryId()
      val format  = DataFormat.Jsonl
      val bytes   = JsonlFile.encode(buffer.toSeq)
      val key     = TableStore.rawKey(table.id, entryId, format)
      val put     = store.objects.put(key, bytes)
      // Catalog-verified checksums: registration records exactly what was written
      if put.checksum != Checksum.sha256Hex(bytes) then
        throw TableStoreException(s"Object store corrupted the ingest write of ${key}")

      val observed = ObservedSchema.fromRows(buffer.toSeq)
      val stats    = ColumnStats.collect(
        buffer.toSeq,
        TableSchema(0L, observed.columns.map((n, t) => ColumnDesc(n, t)))
      )
      val (minTs, maxTs) = eventTimeBounds()

      val result = catalog.registerIngest(
        table.id,
        writerId,
        Seq(
          PendingFileEntry(
            entryId = entryId,
            s3Key = key,
            format = format,
            checksum = Checksum.sha256Hex(bytes),
            rowCount = buffer.size.toLong,
            byteSize = bytes.length.toLong,
            minEventTs = minTs,
            maxEventTs = maxTs,
            observedSchemaJson = observed.schemaJson,
            stats = stats
          )
        )
      )
      result.snapshotId.foreach(snapshotIds += _)
      result.registeredEntryIds.foreach(entryIds += _)
      totalBytes += bytes.length.toLong
      buffer.clear()
      bufferedBytes = 0L
      debug(s"Registered ${key} with ${bytes.length} bytes at snapshot ${result.snapshotId}")
  end rotate

  private def nextEntryId(): EntryId = synchronized {
    if idPool.isEmpty then
      idPool ++= catalog.allocateFileIds(table.id, config.fileIdBatchSize)
    val id = idPool.remove(idPool.size - 1)
    id
  }

  private def eventTimeBounds(): (Option[Long], Option[Long]) =
    table.options.eventTimeColumn match
      case None =>
        (None, None)
      case Some(col) =>
        val ts = buffer.flatMap(
          _.get(col)
            .collect { case JSON.JSONLong(v) =>
              v
            }
        )
        if ts.isEmpty then
          (None, None)
        else
          (Some(ts.min), Some(ts.max))

end IngestSession

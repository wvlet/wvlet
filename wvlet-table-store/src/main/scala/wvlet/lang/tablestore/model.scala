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

import wvlet.uni.json.JSON

type SnapshotId      = Long
type EntryId         = Long
type SchemaVersionId = Long

/** One row of a data file: a JSON object with typed leaf values */
type DataRow = JSON.JSONObject

/** UTC microseconds since epoch — the canonical timestamp representation of the catalog contract */
def nowMicros: Long = java.time.Instant.now().toEpochMilli * 1000L

enum SnapshotKind:
  case Ingest
  case Merge
  case Rewrite
  case Retention

enum EntryKind:
  case File
  case FileSet

enum DataFormat:
  case Jsonl
  case Parquet

/**
  * A table registered in the catalog. `schemaVersionHead` points at the latest published
  * [[SchemaVersion]]; new columns stay pending until merge-time escalation bumps it.
  */
case class CatalogTable(
    id: Long,
    databaseName: String,
    name: String,
    schemaVersionHead: SchemaVersionId,
    /** Optional table options as JSON text (e.g. event-time column name) */
    optionsJson: String,
    createdAt: Long
):
  def options: TableOptions = TableOptions.fromJson(optionsJson)

case class TableOptions(
    /** Column interpreted as the event time for pruning and retention. None = no event time */
    eventTimeColumn: Option[String]
):
  def toJson: String =
    val entries = Seq(eventTimeColumn.map(v => "event_time" -> JSON.JSONString(v))).flatten
    (JSON.JSONObject(entries)).toJSON

object TableOptions:
  val default: TableOptions                = TableOptions(eventTimeColumn = None)
  def fromJson(json: String): TableOptions =
    if json == null || json.isEmpty then
      default
    else
      JSON.parse(json) match
        case obj: JSON.JSONObject =>
          TableOptions(eventTimeColumn =
            obj
              .get("event_time")
              .collect { case JSON.JSONString(v) =>
                v
              }
          )
        case _ =>
          default

/** A published schema version of a table. Publishing happens only through ingest registration or */
/** under the merge lease */
case class SchemaVersion(
    tableId: Long,
    version: SchemaVersionId,
    /** Ordered column definitions encoded as JSON: [{"name":..., "type":"long"}, ...] */
    schemaJson: String,
    publishedAt: Long,
    /** Snapshot that first carried this schema version */
    publishedBySnapshot: SnapshotId
)

case class Snapshot(
    tableId: Long,
    id: SnapshotId,
    kind: SnapshotKind,
    schemaVersion: SchemaVersionId,
    publishedAt: Long,
    publishedBy: String,
    /** Lease fencing token for retiring snapshots (merge/rewrite/retention); None for ingest */
    fencingToken: Option[String]
)

/**
  * One registered data file (or file set manifest). A file is visible at snapshot S iff
  * `beginSnapshot <= S && (endSnapshot.isEmpty || endSnapshot.get > S)` — interval-encoded
  * liveness, so publication costs O(files touched), never O(files live).
  */
case class FileEntry(
    id: EntryId,
    tableId: Long,
    kind: EntryKind,
    s3Key: String,
    format: DataFormat,
    checksum: String,
    rowCount: Long,
    byteSize: Long,
    minEventTs: Option[Long],
    maxEventTs: Option[Long],
    /** Schema observed in this file at registration, as JSON {"columns":[{"name","type"}]} */
    observedSchemaJson: String,
    beginSnapshot: SnapshotId,
    endSnapshot: Option[SnapshotId],
    mergedFrom: List[EntryId],
    writtenBy: String,
    createdAt: Long
):
  def isVisibleAt(snapshotId: SnapshotId): Boolean =
    beginSnapshot <= snapshotId && endSnapshot.forall(_ > snapshotId)

  def isLive: Boolean = endSnapshot.isEmpty

/** Per-column min/max statistics of one file — advisory pruning metadata */
case class ColumnStat(
    fileId: EntryId,
    columnName: String,
    schemaVersion: SchemaVersionId,
    minValue: Option[String],
    maxValue: Option[String],
    nullCount: Long,
    distinctEstimate: Option[Long]
)

case class SnapshotPin(tableId: Long, snapshotId: SnapshotId, holder: String, expiresAt: Long)

/** Named exclusive lease with a monotonically increasing fencing token */
case class Lease(
    name: String,
    tableId: Option[Long],
    holder: String,
    fencingToken: Long,
    acquiredAt: Long,
    expiresAt: Long
):
  def isExpired(nowMicros: Long): Boolean = expiresAt <= nowMicros

case class RetentionPolicy(tableId: Long, kind: String, horizonDays: Int)

class TableStoreException(message: String, cause: Throwable | Null = null)
    extends RuntimeException(message, cause)

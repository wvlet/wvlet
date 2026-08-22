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
package wvlet.lang.tablestore.read

import wvlet.lang.tablestore.{
  CatalogTable,
  DataRow,
  DataFormat,
  FileEntry,
  SnapshotId,
  TableStoreException
}
import wvlet.lang.tablestore.catalog.CatalogStore
import wvlet.lang.tablestore.format.{JsonlFile, ParquetFile}
import wvlet.lang.tablestore.objectstore.ObjectStore
import wvlet.lang.tablestore.schema.{ColumnType, ObservedSchema, TableSchema}
import wvlet.uni.log.LogSupport

/** One planned cast of a file column toward the published schema */
case class CastSpec(
    column: String,
    targetType: ColumnType,
    /** None when the file lacks the column entirely — the scan fills NULL */
    sourceType: Option[ColumnType]
)

/** A file selected for scanning together with its cast plan */
case class PlannedFile(entry: FileEntry, casts: Seq[CastSpec])

/**
  * Everything a reader needs: the pinned snapshot, the published schema in effect there, and the
  * surviving (pruned) files with their cast plans. The scan itself holds no catalog locks.
  */
case class ScanPlan(
    table: CatalogTable,
    snapshotId: SnapshotId,
    snapshotPublishedAtMicros: Long,
    schema: TableSchema,
    files: Seq[PlannedFile],
    /** Entries eliminated by catalog-side pruning (stats said they cannot match) */
    prunedEntryCount: Int
)

/**
  * Resolves scans against the catalog and materializes rows. Production engines can replace
  * [[scan]] with their own readers driven by [[ScanPlan]]: union un-merged JSONL beside merged
  * Parquet and cast every file to the published schema — no engine-side schema inference anywhere.
  */
class TableReader(catalog: CatalogStore, objects: ObjectStore) extends LogSupport:

  /**
    * Pin the latest snapshot (or an explicit AS OF one) and resolve the files worth scanning. The
    * scan itself holds no catalog locks; long scans renew their pin until done so that retention
    * never retires files visible at a pinned snapshot.
    */
  def plan(
      table: CatalogTable,
      asOf: Option[SnapshotId] = None,
      predicate: Option[Predicate] = None,
      /** When set, registers an expiring pin for this read */
      pinHolder: Option[String] = None,
      pinTtlMillis: Long = 60_000L
  ): ScanPlan =
    val snapshot =
      asOf match
        case Some(id) =>
          catalog
            .snapshotOf(table.id, id)
            .getOrElse {
              throw TableStoreException(s"Snapshot ${id} of table '${table.name}' does not exist")
            }
        case None =>
          catalog
            .latestSnapshot(table.id)
            .getOrElse {
              throw TableStoreException(s"Table '${table.name}' has no snapshots yet")
            }

    pinHolder.foreach { holder =>
      catalog.pinSnapshot(table.id, snapshot.id, holder, pinTtlMillis)
    }

    val candidates                   = catalog.liveEntries(table.id, snapshot.id)
    val schema                       = schemaAt(table, snapshot.schemaVersion)
    val typeOf: String => ColumnType =
      col => schema.column(col).map(_.columnType).getOrElse(ColumnType.NullType)

    // Predicate pushdown into the catalog scan: file_entries ⋈ file_column_stats. One catalog
    // round trip returns exactly the files worth scanning — no data I/O for pruned files.
    val statsByEntry = catalog
      .statsFor(candidates.map(_.id))
      .groupBy(_.fileId)
      .map { case (fid, stats) =>
        fid -> stats.map(s => s.columnName -> s).toMap
      }
    val surviving =
      predicate match
        case None =>
          candidates
        case Some(p) =>
          candidates.filter { e =>
            Pruning.canMatch(statsByEntry.getOrElse(e.id, Map.empty), typeOf)(p)
          }
    val prunedCount = candidates.size - surviving.size

    val files = surviving.map { entry =>
      PlannedFile(entry, castPlan(entry, schema))
    }
    ScanPlan(table, snapshot.id, snapshot.publishedAt, schema, files, prunedCount)
  end plan

  /** Materialize the rows of a plan by scanning every surviving file and casting to the schema */
  def scan(plan: ScanPlan): Seq[DataRow] =
    val rows = Seq.newBuilder[DataRow]
    plan
      .files
      .foreach { planned =>
        val bytes  = objects.get(planned.entry.s3Key)
        val actual = wvlet.lang.tablestore.objectstore.Checksum.sha256Hex(bytes)
        if actual != planned.entry.checksum then
          throw TableStoreException(
            s"Checksum mismatch for ${planned.entry.s3Key}: expected ${planned
                .entry
                .checksum}, got ${actual}"
          )
        val decoded: Seq[DataRow] =
          planned.entry.format match
            case DataFormat.Jsonl =>
              JsonlFile.decode(bytes)
            case DataFormat.Parquet =>
              objects.withLocalFile(planned.entry.s3Key) { f =>
                ParquetFile.read(f.getPath)
              }
        decoded.foreach(row => rows += ParquetFile.normalizeRow(row, plan.schema))
      }
    rows.result()
  end scan

  private def castPlan(entry: FileEntry, schema: TableSchema): Seq[CastSpec] =
    if schema.columns.isEmpty then
      Nil
    else
      val observed = ObservedSchema.fromJson(entry.observedSchemaJson)
      schema
        .columns
        .flatMap { col =>
          observed.columnType(col.name) match
            case None =>
              // The file predates this column: fill NULL during the scan
              Some(CastSpec(col.name, col.columnType, None))
            case Some(src) if src != col.columnType =>
              Some(CastSpec(col.name, col.columnType, Some(src)))
            case Some(_) =>
              None
        }

  /** The published schema in effect at a snapshot (version 0 = nothing published yet) */
  def schemaAt(table: CatalogTable, version: Long): TableSchema =
    if version == 0 then
      TableSchema.empty
    else
      catalog.schemaVersionsOf(table.id).find(_.version == version) match
        case Some(v) =>
          TableSchema.fromJson(v.version, v.schemaJson)
        case None =>
          throw TableStoreException(
            s"Schema version ${version} of table '${table.name}' is missing from the catalog"
          )

end TableReader

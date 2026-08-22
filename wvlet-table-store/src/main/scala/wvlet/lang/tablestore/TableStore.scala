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

import wvlet.lang.tablestore.catalog.{CatalogDrivers, CatalogStore}
import wvlet.lang.tablestore.objectstore.ObjectStore
import wvlet.lang.tablestore.schema.ColumnDesc

/**
  * The table store facade: a transactional catalog plus an immutable object store holding the data
  * files. Ingestion registers files directly into the catalog — a registered file is immediately
  * readable; background merge only makes scans faster.
  *
  * Object layout:
  *   - raw ingest: `raw/table=<id>/<fileId>.<ext>` — pre-issued id + catalog checksum
  *   - merged output: `merged/table=<id>/<sha256>.parquet` — content-addressed
  */
case class TableStore(catalog: CatalogStore, objects: ObjectStore):
  def initialize(): Unit = catalog.initialize()

  def createDatabase(name: String): wvlet.lang.tablestore.catalog.Database = catalog.createDatabase(
    name
  )

  def createTable(
      databaseName: String,
      name: String,
      options: TableOptions = TableOptions.default,
      initialColumns: Seq[ColumnDesc] = Nil
  ): CatalogTable = catalog.createTable(databaseName, name, options, initialColumns)

  def findTable(databaseName: String, name: String): Option[CatalogTable] = catalog.findTable(
    databaseName,
    name
  )

  def newIngestSession(table: CatalogTable, writerId: String): IngestSession = IngestSession(
    this,
    table,
    writerId
  )

  def reader: read.TableReader = read.TableReader(catalog, objects)

  def merger(writerId: String): merge.Merger = merge.Merger(store = this, writerId = writerId)

  /** Object keys referenced by the catalog are subtracted from the store inventory */
  def orphanCandidates(tableId: Long): Seq[objectstore.ObjectSummary] =
    val referenced = catalog.referencedKeys(tableId)
    val inventory  =
      objects.list(s"raw/table=${tableId}/") ++ objects.list(s"merged/table=${tableId}/")
    inventory.filterNot(s => referenced.contains(s.key))

end TableStore

object TableStore:
  /** Embedded profile: SQLite catalog + local filesystem store under one root directory */
  def embedded(rootPath: String): TableStore =
    val store = TableStore(
      CatalogDrivers.sqlite(s"${rootPath}/catalog.db"),
      ObjectStore.local(s"${rootPath}/objects")
    )
    store.initialize()
    store

  /** Raw data file key for a pre-issued entry id */
  private[tablestore] def rawKey(tableId: Long, entryId: EntryId, format: DataFormat): String =
    s"raw/table=${tableId}/${entryId}.${
        if format == DataFormat.Jsonl then
          "jsonl"
        else
          "parquet"
      }"

  /** Content-addressed key of a merged Parquet file */
  private[tablestore] def mergedKey(tableId: Long, sha256: String): String =
    s"merged/table=${tableId}/${sha256}.parquet"

end TableStore

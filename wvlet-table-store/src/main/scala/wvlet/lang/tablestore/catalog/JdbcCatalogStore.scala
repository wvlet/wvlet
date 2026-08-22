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

import wvlet.lang.tablestore.{
  CatalogTable,
  ColumnStat,
  DataFormat,
  EntryId,
  EntryKind,
  FileEntry,
  Lease,
  nowMicros,
  SchemaVersion,
  SchemaVersionId,
  Snapshot,
  SnapshotId,
  SnapshotKind,
  SnapshotPin,
  TableOptions,
  TableStoreException
}
import wvlet.lang.tablestore.schema.{ColumnDesc, TableSchema}
import wvlet.uni.json.JSON
import wvlet.uni.log.LogSupport

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
  * A portable JDBC implementation of [[CatalogStore]] shared by SQLite, DuckDB, and Postgres.
  *
  * The catalog contract is honored with plain SQL only: TEXT-JSON metadata, no array columns, and
  * monotonic counters emulated with a CAS-style upsert. Correctness of retiring transactions does
  * not lean on isolation levels — fencing and retirement are enforced by conditional updates with
  * row-count assertions inside one transaction.
  *
  * Concurrency note: this implementation serializes access through a single connection, matching
  * the embedded profile's single-process single-writer semantics. A pooled variant for Postgres can
  * reuse every statement unchanged.
  */
class JdbcCatalogStore(
    url: String,
    user: Option[String] = None,
    password: Option[String] = None,
    /** TTL applied when a successful merge commit renews its lease */
    val leaseTtlMillis: Long = 60_000L
) extends CatalogStore
    with LogSupport:

  private val conn: Connection =
    (user, password) match
      case (Some(u), Some(p)) =>
        DriverManager.getConnection(url, u, p)
      case _ =>
        DriverManager.getConnection(url)

  private val lock = new Object

  override def close(): Unit = lock.synchronized(conn.close())

  // ---- Low-level helpers ----

  private def update(sql: String, bind: PreparedStatement => Unit): Int = lock.synchronized {
    val st = conn.prepareStatement(sql)
    try
      bind(st)
      st.executeUpdate()
    finally
      st.close()
  }

  private def query[A](sql: String, bind: PreparedStatement => Unit)(mapRs: ResultSet => A): A =
    lock.synchronized {
      val st = conn.prepareStatement(sql)
      try
        bind(st)
        val rs = st.executeQuery()
        try mapRs(rs)
        finally rs.close()
      finally
        st.close()
    }

  private def queryOne[A](sql: String, bind: PreparedStatement => Unit)(
      mapRs: ResultSet => A
  ): Option[A] =
    query(sql, bind) { rs =>
      if rs.next() then
        Some(mapRs(rs))
      else
        None
    }

  private def queryList[A](sql: String, bind: PreparedStatement => Unit)(
      mapRs: ResultSet => A
  ): Seq[A] =
    query(sql, bind) { rs =>
      val b = Seq.newBuilder[A]
      while rs.next() do
        b += mapRs(rs)
      b.result()
    }

  private def tx[A](body: => A): A = lock.synchronized {
    val prevAutoCommit = conn.getAutoCommit
    conn.setAutoCommit(false)
    try
      val result = body
      conn.commit()
      result
    catch
      case e: Throwable =>
        conn.rollback()
        throw e
    finally
      conn.setAutoCommit(prevAutoCommit)
  }

  private def exec(sql: String): Unit = lock.synchronized {
    val st = conn.createStatement()
    try st.execute(sql)
    finally st.close()
  }

  // ---- DDL ----

  override def initialize(): Unit = lock.synchronized {
    val ddl = Seq(
      """CREATE TABLE IF NOT EXISTS databases(
        |  id BIGINT PRIMARY KEY,
        |  name VARCHAR NOT NULL UNIQUE,
        |  created_at BIGINT NOT NULL
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS tables(
        |  id BIGINT PRIMARY KEY,
        |  db_id BIGINT NOT NULL,
        |  name VARCHAR NOT NULL,
        |  schema_version_head BIGINT NOT NULL DEFAULT 0,
        |  partition_spec_json TEXT NOT NULL DEFAULT '{}',
        |  options_json TEXT NOT NULL DEFAULT '{}',
        |  created_at BIGINT NOT NULL,
        |  UNIQUE(db_id, name)
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS schema_versions(
        |  table_id BIGINT NOT NULL,
        |  version BIGINT NOT NULL,
        |  schema_json TEXT NOT NULL,
        |  published_at BIGINT NOT NULL,
        |  published_by_snapshot BIGINT NOT NULL,
        |  PRIMARY KEY (table_id, version)
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS snapshots(
        |  table_id BIGINT NOT NULL,
        |  id BIGINT NOT NULL,
        |  kind VARCHAR NOT NULL,
        |  schema_version BIGINT NOT NULL,
        |  published_at BIGINT NOT NULL,
        |  published_by VARCHAR NOT NULL,
        |  fencing_token VARCHAR,
        |  PRIMARY KEY (table_id, id)
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS file_entries(
        |  id BIGINT PRIMARY KEY,
        |  table_id BIGINT NOT NULL,
        |  kind VARCHAR NOT NULL,
        |  s3_key VARCHAR NOT NULL,
        |  format VARCHAR NOT NULL,
        |  checksum VARCHAR NOT NULL,
        |  row_count BIGINT NOT NULL,
        |  byte_size BIGINT NOT NULL,
        |  min_event_ts BIGINT,
        |  max_event_ts BIGINT,
        |  observed_schema_json TEXT NOT NULL,
        |  begin_snapshot BIGINT NOT NULL,
        |  end_snapshot BIGINT,
        |  merged_from_json TEXT,
        |  written_by VARCHAR NOT NULL,
        |  created_at BIGINT NOT NULL
        |)""".stripMargin,
      """CREATE INDEX IF NOT EXISTS idx_file_entries_liveness
        |  ON file_entries(table_id, begin_snapshot, end_snapshot)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS file_column_stats(
        |  file_id BIGINT NOT NULL,
        |  table_id BIGINT NOT NULL,
        |  column_name VARCHAR NOT NULL,
        |  schema_version BIGINT NOT NULL,
        |  min_value VARCHAR,
        |  max_value VARCHAR,
        |  null_count BIGINT NOT NULL,
        |  distinct_estimate BIGINT,
        |  PRIMARY KEY (file_id, column_name)
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS snapshot_pins(
        |  table_id BIGINT NOT NULL,
        |  snapshot_id BIGINT NOT NULL,
        |  holder VARCHAR NOT NULL,
        |  expires_at BIGINT NOT NULL,
        |  PRIMARY KEY (table_id, holder)
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS leases(
        |  name VARCHAR PRIMARY KEY,
        |  table_id BIGINT,
        |  holder VARCHAR NOT NULL,
        |  fencing_token BIGINT NOT NULL,
        |  acquired_at BIGINT NOT NULL,
        |  expires_at BIGINT NOT NULL
        |)""".stripMargin,
      """CREATE TABLE IF NOT EXISTS retention_policies(
        |  table_id BIGINT NOT NULL,
        |  kind VARCHAR NOT NULL,
        |  horizon_days INT NOT NULL,
        |  PRIMARY KEY (table_id, kind)
        |)""".stripMargin,
      // Monotonic sequence emulation: each scope holds the next unallocated value
      """CREATE TABLE IF NOT EXISTS counters(
        |  scope VARCHAR PRIMARY KEY,
        |  next_value BIGINT NOT NULL
        |)""".stripMargin
    )
    ddl.foreach(exec)
  }

  // ---- Counters (monotonic sequences) ----

  /**
    * Reserve `amount` monotonic values from the scope's counter: ids are 1-based and never repeat.
    * The CAS-style upsert allocates inside the caller's transaction, so concurrent writers
    * serialize on the counter row without a separate round trip.
    */
  private def reserve(scope: String, amount: Long): (Long, Long) =
    update(
      """INSERT INTO counters(scope, next_value) VALUES(?, ?)
        |ON CONFLICT(scope) DO UPDATE SET next_value = next_value + ?""".stripMargin,
      st =>
        st.setString(1, scope)
        // Fresh counters start just past the first allocation so ids begin at 1
        st.setLong(2, amount + 1)
        st.setLong(3, amount)
    )
    val next =
      queryOne("SELECT next_value FROM counters WHERE scope = ?", st => st.setString(1, scope))(
        rs => rs.getLong(1)
      ).get
    (next - amount, next)

  private def tableSnapshotScope(tableId: Long) = s"table:${tableId}:snapshot"
  private def tableFileScope(tableId: Long)     = s"table:${tableId}:file"

  override def allocateFileIds(tableId: Long, count: Int): Seq[EntryId] = tx {
    val (start, end) = reserve(tableFileScope(tableId), count)
    (start until end)
  }

  // ---- Databases and tables ----

  override def createDatabase(name: String): Database = tx {
    findDatabase(name) match
      case Some(db) =>
        db
      case None =>
        val id = reserve("database-id", 1)._1
        update(
          "INSERT INTO databases(id, name, created_at) VALUES (?, ?, ?)",
          st =>
            st.setLong(1, id)
            st.setString(2, name)
            st.setLong(3, nowMicros)
        )
        Database(id, name, nowMicros)
  }

  override def findDatabase(name: String): Option[Database] =
    queryOne(
      "SELECT id, name, created_at FROM databases WHERE name = ?",
      st => st.setString(1, name)
    )(rs => Database(rs.getLong(1), rs.getString(2), rs.getLong(3)))

  override def createTable(
      databaseName: String,
      name: String,
      options: TableOptions = TableOptions.default,
      initialColumns: Seq[ColumnDesc] = Nil
  ): CatalogTable = tx {
    val db = findDatabase(databaseName).getOrElse {
      throw wvlet
        .lang
        .api
        .StatusCode
        .CATALOG_NOT_FOUND
        .newException(s"Database '${databaseName}' does not exist")
    }
    findTable(databaseName, name).foreach { t =>
      throw wvlet
        .lang
        .api
        .StatusCode
        .TABLE_ALREADY_EXISTS
        .newException(s"Table '${databaseName}.${name}' already exists as id ${t.id}")
    }
    val id = reserve("table-id", 1)._1
    update(
      """INSERT INTO tables(id, db_id, name, schema_version_head, partition_spec_json, options_json, created_at)
        |VALUES (?, ?, ?, 0, '{}', ?, ?)""".stripMargin,
      st =>
        st.setLong(1, id)
        st.setLong(2, db.id)
        st.setString(3, name)
        st.setString(4, options.toJson)
        st.setLong(5, nowMicros)
    )
    if initialColumns.nonEmpty then
      // The declared schema head: version 1, published by table creation (snapshot 0)
      publishSchemaVersion(id, TableSchema(1, initialColumns), bySnapshot = 0L)
    CatalogTable(
      id,
      databaseName,
      name,
      if initialColumns.nonEmpty then
        1
      else
        0
      ,
      options.toJson,
      nowMicros
    )
  }

  override def findTable(databaseName: String, name: String): Option[CatalogTable] =
    queryOne(
      """SELECT t.id, d.name, t.name, t.schema_version_head, t.options_json, t.created_at
        |FROM tables t JOIN databases d ON t.db_id = d.id
        |WHERE d.name = ? AND t.name = ?""".stripMargin,
      st =>
        st.setString(1, databaseName)
        st.setString(2, name)
    ) { rs =>
      CatalogTable(
        rs.getLong(1),
        rs.getString(2),
        rs.getString(3),
        rs.getLong(4),
        rs.getString(5),
        rs.getLong(6)
      )
    }

  override def getTable(tableId: Long): CatalogTable = queryOne(
    """SELECT t.id, d.name, t.name, t.schema_version_head, t.options_json, t.created_at
        |FROM tables t JOIN databases d ON t.db_id = d.id
        |WHERE t.id = ?""".stripMargin,
    st => st.setLong(1, tableId)
  ) { rs =>
    CatalogTable(
      rs.getLong(1),
      rs.getString(2),
      rs.getString(3),
      rs.getLong(4),
      rs.getString(5),
      rs.getLong(6)
    )
  }.getOrElse {
    throw wvlet.lang.api.StatusCode.TABLE_NOT_FOUND.newException(s"Table id ${tableId}")
  }

  private def schemaHead(tableId: Long): SchemaVersionId = queryOne(
    "SELECT schema_version_head FROM tables WHERE id = ?",
    st => st.setLong(1, tableId)
  )(rs => rs.getLong(1)).getOrElse(0L)

  // ---- Ingest registration ----

  override def registerIngest(
      tableId: Long,
      writer: String,
      entries: Seq[PendingFileEntry]
  ): IngestResult = tx {
    require(entries.forall(e => e.entryId >= 0), "entry ids must be pre-issued")
    val knownIds = entries.map(_.entryId).filter(entryExists)
    val fresh    = entries.filterNot(e => knownIds.contains(e.entryId))

    if fresh.isEmpty then
      IngestResult(None, Nil, knownIds)
    else
      val snapId = reserve(tableSnapshotScope(tableId), 1)._1
      val head   = schemaHead(tableId)
      insertSnapshot(Snapshot(tableId, snapId, SnapshotKind.Ingest, head, nowMicros, writer, None))
      fresh.foreach { e =>
        insertFileEntry(
          FileEntry(
            id = e.entryId,
            tableId = tableId,
            kind = EntryKind.File,
            s3Key = e.s3Key,
            format = e.format,
            checksum = e.checksum,
            rowCount = e.rowCount,
            byteSize = e.byteSize,
            minEventTs = e.minEventTs,
            maxEventTs = e.maxEventTs,
            observedSchemaJson = e.observedSchemaJson,
            beginSnapshot = snapId,
            endSnapshot = None,
            mergedFrom = Nil,
            writtenBy = writer,
            createdAt = nowMicros
          ),
          e.stats,
          head
        )
      }
      IngestResult(Some(snapId), fresh.map(_.entryId), knownIds)
    end if
  }

  private def entryExists(entryId: EntryId): Boolean =
    queryOne("SELECT id FROM file_entries WHERE id = ?", st => st.setLong(1, entryId))(rs =>
      rs.getLong(1)
    ).isDefined

  private def insertSnapshot(snapshot: Snapshot): Unit = update(
    """INSERT INTO snapshots(table_id, id, kind, schema_version, published_at, published_by, fencing_token)
        |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin,
    st =>
      st.setLong(1, snapshot.tableId)
      st.setLong(2, snapshot.id)
      st.setString(3, snapshot.kind.toString.toLowerCase)
      st.setLong(4, snapshot.schemaVersion)
      st.setLong(5, snapshot.publishedAt)
      st.setString(6, snapshot.publishedBy)
      snapshot.fencingToken.foreach(t => st.setString(7, t))
      if snapshot.fencingToken.isEmpty then
        st.setNull(7, java.sql.Types.VARCHAR)
  )

  private def insertFileEntry(
      entry: FileEntry,
      stats: Seq[PendingColumnStat],
      statsSchemaVersion: SchemaVersionId
  ): Unit =
    update(
      """INSERT INTO file_entries(
        |  id, table_id, kind, s3_key, format, checksum, row_count, byte_size,
        |  min_event_ts, max_event_ts, observed_schema_json, begin_snapshot, end_snapshot,
        |  merged_from_json, written_by, created_at)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |ON CONFLICT (id) DO NOTHING""".stripMargin,
      st =>
        var i = 1
        st.setLong(i, entry.id);
        i += 1
        st.setLong(i, entry.tableId);
        i += 1
        st.setString(i, entry.kind.toString.toLowerCase);
        i += 1
        st.setString(i, entry.s3Key);
        i += 1
        st.setString(i, entry.format.toString.toLowerCase);
        i += 1
        st.setString(i, entry.checksum);
        i += 1
        st.setLong(i, entry.rowCount);
        i += 1
        st.setLong(i, entry.byteSize);
        i += 1
        setOptionalLong(st, i, entry.minEventTs);
        i += 1
        setOptionalLong(st, i, entry.maxEventTs);
        i += 1
        st.setString(i, entry.observedSchemaJson);
        i += 1
        st.setLong(i, entry.beginSnapshot);
        i += 1
        entry.endSnapshot.foreach(x => st.setLong(i, x))
        if entry.endSnapshot.isEmpty then
          st.setNull(i, java.sql.Types.BIGINT)
        i += 1
        st.setString(i, encodeMergedFrom(entry.mergedFrom));
        i += 1
        st.setString(i, entry.writtenBy);
        i += 1
        st.setLong(i, entry.createdAt)
    )
    stats.foreach { stat =>
      update(
        """INSERT INTO file_column_stats(file_id, table_id, column_name, schema_version, min_value, max_value, null_count, distinct_estimate)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin,
        st =>
          var i = 1
          st.setLong(i, entry.id);
          i += 1
          st.setLong(i, entry.tableId);
          i += 1
          st.setString(i, stat.columnName);
          i += 1
          st.setLong(i, statsSchemaVersion);
          i += 1
          setOptionalString(st, i, stat.minValue);
          i += 1
          setOptionalString(st, i, stat.maxValue);
          i += 1
          st.setLong(i, stat.nullCount);
          i += 1
          setOptionalLong(st, i, stat.distinctEstimate)
      )
    }
  end insertFileEntry

  private def encodeMergedFrom(ids: List[EntryId]): String = JSON.format(
    JSON.JSONArray(ids.map(id => JSON.JSONLong(id)).toIndexedSeq)
  )

  private def decodeMergedFrom(json: String): List[EntryId] =
    if json == null then
      Nil
    else
      JSON.parse(json) match
        case JSON.JSONArray(items) =>
          items
            .collect { case JSON.JSONLong(v) =>
              v
            }
            .toList
        case _ =>
          Nil

  private def setOptionalLong(st: PreparedStatement, index: Int, v: Option[Long]): Unit =
    v match
      case Some(x) =>
        st.setLong(index, x)
      case None =>
        st.setNull(index, java.sql.Types.BIGINT)

  private def setOptionalString(st: PreparedStatement, index: Int, v: Option[String]): Unit =
    v match
      case Some(x) =>
        st.setString(index, x)
      case None =>
        st.setNull(index, java.sql.Types.VARCHAR)

  // ---- Row mapping ----

  private def mapEntry(rs: ResultSet): FileEntry = FileEntry(
    id = rs.getLong("id"),
    tableId = rs.getLong("table_id"),
    kind = enumFromName(EntryKind.values, rs.getString("kind"), EntryKind.File),
    s3Key = rs.getString("s3_key"),
    format = enumFromName(DataFormat.values, rs.getString("format"), DataFormat.Jsonl),
    checksum = rs.getString("checksum"),
    rowCount = rs.getLong("row_count"),
    byteSize = rs.getLong("byte_size"),
    minEventTs = optLong(rs, "min_event_ts"),
    maxEventTs = optLong(rs, "max_event_ts"),
    observedSchemaJson = rs.getString("observed_schema_json"),
    beginSnapshot = rs.getLong("begin_snapshot"),
    endSnapshot = optLong(rs, "end_snapshot"),
    mergedFrom = decodeMergedFrom(rs.getString("merged_from_json")),
    writtenBy = rs.getString("written_by"),
    createdAt = rs.getLong("created_at")
  )

  private def mapSnapshot(rs: ResultSet): Snapshot = Snapshot(
    tableId = rs.getLong("table_id"),
    id = rs.getLong("id"),
    kind = enumFromName(SnapshotKind.values, rs.getString("kind"), SnapshotKind.Ingest),
    schemaVersion = rs.getLong("schema_version"),
    publishedAt = rs.getLong("published_at"),
    publishedBy = rs.getString("published_by"),
    fencingToken =
      val t = rs.getString("fencing_token")
      if rs.wasNull then
        None
      else
        Some(t)
  )

  private def enumFromName[E](values: Array[E], raw: String, default: E): E = values
    .find(_.toString.toLowerCase == raw)
    .getOrElse(default)

  private def optLong(rs: ResultSet, col: String): Option[Long] =
    val v = rs.getLong(col)
    if rs.wasNull then
      None
    else
      Some(v)

  private def optLongAt(rs: ResultSet, col: Int): Option[Long] =
    val v = rs.getLong(col)
    if rs.wasNull then
      None
    else
      Some(v)

  // ---- Reads ----

  override def latestSnapshot(tableId: Long): Option[Snapshot] =
    queryOne(
      "SELECT * FROM snapshots WHERE table_id = ? ORDER BY id DESC LIMIT 1",
      st => st.setLong(1, tableId)
    )(mapSnapshot)

  override def snapshotOf(tableId: Long, snapshotId: SnapshotId): Option[Snapshot] =
    queryOne(
      "SELECT * FROM snapshots WHERE table_id = ? AND id = ?",
      st =>
        st.setLong(1, tableId)
        st.setLong(2, snapshotId)
    )(mapSnapshot)

  override def snapshotsOf(tableId: Long): Seq[Snapshot] =
    queryList(
      "SELECT * FROM snapshots WHERE table_id = ? ORDER BY id",
      st => st.setLong(1, tableId)
    )(mapSnapshot)

  override def schemaVersionsOf(tableId: Long): Seq[SchemaVersion] =
    queryList(
      """SELECT version, schema_json, published_at, published_by_snapshot
        |FROM schema_versions WHERE table_id = ? ORDER BY version""".stripMargin,
      st => st.setLong(1, tableId)
    ) { rs =>
      SchemaVersion(tableId, rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4))
    }

  /**
    * The interval predicate `begin_snapshot <= S AND (end_snapshot IS NULL OR end_snapshot > S)` —
    * the single definition of file liveness in the system.
    */
  private val livePredicateSql =
    "begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)"

  override def liveEntries(tableId: Long, snapshotId: SnapshotId): Seq[FileEntry] =
    queryList(
      s"""SELECT * FROM file_entries WHERE table_id = ? AND $livePredicateSql ORDER BY id"""
        .stripMargin,
      st =>
        st.setLong(1, tableId)
        st.setLong(2, snapshotId)
        st.setLong(3, snapshotId)
    )(mapEntry)

  override def entry(entryId: EntryId): Option[FileEntry] =
    queryOne("SELECT * FROM file_entries WHERE id = ?", st => st.setLong(1, entryId))(mapEntry)

  override def statsFor(fileIds: Seq[EntryId]): Seq[ColumnStat] =
    if fileIds.isEmpty then
      Nil
    else
      val placeholders = fileIds.map(_ => "?").mkString(", ")
      queryList(
        s"""SELECT file_id, column_name, schema_version, min_value, max_value, null_count, distinct_estimate
           |FROM file_column_stats WHERE file_id IN ($placeholders) ORDER BY file_id, column_name"""
          .stripMargin,
        st =>
          var i = 1
          fileIds.foreach { id =>
            st.setLong(i, id)
            i += 1
          }
      ) { rs =>
        ColumnStat(
          fileId = rs.getLong(1),
          columnName = rs.getString(2),
          schemaVersion = rs.getLong(3),
          minValue = optString(rs, 4),
          maxValue = optString(rs, 5),
          nullCount = rs.getLong(6),
          distinctEstimate = optLongAt(rs, 7)
        )
      }

  private def optString(rs: ResultSet, col: Int): Option[String] =
    val v = rs.getString(col)
    if rs.wasNull then
      None
    else
      Some(v)

  // ---- Leases ----

  private def leaseTokenScope(name: String) = s"lease-token:${name}"

  override def acquireLease(
      name: String,
      tableId: Option[Long],
      holder: String,
      ttlMillis: Long
  ): Lease = tx {
    val now             = nowMicros
    val expiresAtMicros = now + ttlMillis * 1000L
    // Mint a fresh fencing token for every acquisition attempt. Tokens only ever increase, so a
    // zombie holding an old token can never pass an in-transaction check against the new one.
    val token = reserve(leaseTokenScope(name), 1)._1
    update(
      """INSERT INTO leases(name, table_id, holder, fencing_token, acquired_at, expires_at)
        |VALUES (?, ?, ?, ?, ?, ?)
        |ON CONFLICT(name) DO UPDATE SET
        |  holder = excluded.holder,
        |  fencing_token = excluded.fencing_token,
        |  acquired_at = excluded.acquired_at,
        |  expires_at = excluded.expires_at
        |WHERE leases.expires_at <= ?
        |   OR leases.holder = excluded.holder""".stripMargin,
      st =>
        st.setString(1, name)
        setOptionalLong(st, 2, tableId)
        st.setString(3, holder)
        st.setLong(4, token)
        st.setLong(5, now)
        st.setLong(6, expiresAtMicros)
        st.setLong(7, now)
    )
    leaseOf(name) match
      case Some(l) if l.holder == holder && l.fencingToken == token =>
        l
      case Some(other) =>
        throw LeaseHeldException(name, other.holder)
      case None =>
        throw TableStoreException(s"Lease row '${name}' disappeared during acquisition")
  }

  override def renewLease(
      name: String,
      holder: String,
      fencingToken: Long,
      ttlMillis: Long
  ): Lease = tx {
    val updated = update(
      """UPDATE leases SET expires_at = ? WHERE name = ? AND holder = ? AND fencing_token = ? AND expires_at > ?"""
        .stripMargin,
      st =>
        st.setLong(1, nowMicros + ttlMillis * 1000L)
        st.setString(2, name)
        st.setString(3, holder)
        st.setLong(4, fencingToken)
        st.setLong(5, nowMicros)
    )
    if updated != 1 then
      throw LeaseLostException(name, fencingToken)
    leaseOf(name).get
  }

  override def releaseLease(name: String, holder: String, fencingToken: Long): Unit = tx {
    update(
      "DELETE FROM leases WHERE name = ? AND holder = ? AND fencing_token = ?",
      st =>
        st.setString(1, name)
        st.setString(2, holder)
        st.setLong(3, fencingToken)
    )
    ()
  }

  override def leaseOf(name: String): Option[Lease] =
    queryOne(
      "SELECT name, table_id, holder, fencing_token, acquired_at, expires_at FROM leases WHERE name = ?",
      st => st.setString(1, name)
    ) { rs =>
      Lease(
        rs.getString(1),
        optLongAt(rs, 2),
        rs.getString(3),
        rs.getLong(4),
        rs.getLong(5),
        rs.getLong(6)
      )
    }

  // ---- Merge commit ----

  override def commitMerge(commit: MergeCommit): MergeCommitResult = tx {
    // 1. Fencing: re-read the lease inside this transaction and abort unless our token matches.
    //    Stamping is not checking — this conditional update is what excludes a zombie merger.
    val fenced = update(
      """UPDATE leases SET expires_at = ? WHERE name = ? AND fencing_token = ? AND expires_at > ?"""
        .stripMargin,
      st =>
        st.setLong(1, nowMicros + leaseTtlMillis * 1000L)
        st.setString(2, commit.leaseName)
        st.setLong(3, commit.fencingToken)
        st.setLong(4, nowMicros)
    )
    if fenced != 1 then
      throw LeaseLostException(commit.leaseName, commit.fencingToken)

    val snapId = reserve(tableSnapshotScope(commit.tableId), 1)._1

    // 2. Retire sources conditionally and assert the affected-row count so two mergers can never
    //    double-fold the same rows. (An empty source list retires nothing.)
    val retired =
      if commit.sourceEntryIds.isEmpty then
        0
      else
        val placeholders = commit.sourceEntryIds.map(_ => "?").mkString(", ")
        update(
          s"""UPDATE file_entries SET end_snapshot = ?
             |WHERE table_id = ? AND id IN ($placeholders) AND end_snapshot IS NULL""".stripMargin,
          st =>
            st.setLong(1, snapId)
            st.setLong(2, commit.tableId)
            var i = 3
            commit
              .sourceEntryIds
              .foreach { id =>
                st.setLong(i, id)
                i += 1
              }
        )
    if retired != commit.sourceEntryIds.size then
      throw RetireConflictException(commit.sourceEntryIds.size, retired)

    // 3. Schema escalation publishes a new version under the same transaction
    val headBefore                          = schemaHead(commit.tableId)
    val newVersion: Option[SchemaVersionId] = commit
      .escalatedSchema
      .map { schema =>
        publishSchemaVersion(commit.tableId, schema, snapId)
        schema.version
      }
    val effectiveHead = newVersion.getOrElse(headBefore)

    // 4. Register the merged file; it is immediately visible at the new snapshot
    insertFileEntry(
      FileEntry(
        id = commit.mergedEntry.entryId,
        tableId = commit.tableId,
        kind = EntryKind.File,
        s3Key = commit.mergedEntry.s3Key,
        format = commit.mergedEntry.format,
        checksum = commit.mergedEntry.checksum,
        rowCount = commit.mergedEntry.rowCount,
        byteSize = commit.mergedEntry.byteSize,
        minEventTs = commit.mergedEntry.minEventTs,
        maxEventTs = commit.mergedEntry.maxEventTs,
        observedSchemaJson = commit.mergedEntry.observedSchemaJson,
        beginSnapshot = snapId,
        endSnapshot = None,
        mergedFrom = commit.sourceEntryIds.toList,
        writtenBy = commit.writer,
        createdAt = nowMicros
      ),
      commit.mergedEntry.stats,
      effectiveHead
    )

    // 5. Publish the retiring snapshot with the verified token stamped on it
    insertSnapshot(
      Snapshot(
        commit.tableId,
        snapId,
        SnapshotKind.Merge,
        effectiveHead,
        nowMicros,
        commit.writer,
        Some(commit.fencingToken.toString)
      )
    )

    MergeCommitResult(snapId, commit.mergedEntry.entryId, newVersion, commit.sourceEntryIds)
  }

  private def publishSchemaVersion(
      tableId: Long,
      schema: TableSchema,
      bySnapshot: SnapshotId
  ): Unit =
    update(
      """INSERT INTO schema_versions(table_id, version, schema_json, published_at, published_by_snapshot)
        |VALUES (?, ?, ?, ?, ?)""".stripMargin,
      st =>
        st.setLong(1, tableId)
        st.setLong(2, schema.version)
        st.setString(3, schema.schemaJson)
        st.setLong(4, nowMicros)
        st.setLong(5, bySnapshot)
    )
    update(
      "UPDATE tables SET schema_version_head = ? WHERE id = ?",
      st =>
        st.setLong(1, schema.version)
        st.setLong(2, tableId)
    )

  // ---- Snapshot pins ----

  override def pinSnapshot(
      tableId: Long,
      snapshotId: SnapshotId,
      holder: String,
      ttlMillis: Long
  ): Unit = tx {
    update(
      """INSERT INTO snapshot_pins(table_id, snapshot_id, holder, expires_at)
        |VALUES (?, ?, ?, ?)
        |ON CONFLICT(table_id, holder) DO UPDATE SET
        |  snapshot_id = excluded.snapshot_id,
        |  expires_at = excluded.expires_at""".stripMargin,
      st =>
        st.setLong(1, tableId)
        st.setLong(2, snapshotId)
        st.setString(3, holder)
        st.setLong(4, nowMicros + ttlMillis * 1000L)
    )
    ()
  }

  override def renewPin(tableId: Long, holder: String, ttlMillis: Long): Unit = tx {
    val updated = update(
      "UPDATE snapshot_pins SET expires_at = ? WHERE table_id = ? AND holder = ?",
      st =>
        st.setLong(1, nowMicros + ttlMillis * 1000L)
        st.setLong(2, tableId)
        st.setString(3, holder)
    )
    if updated != 1 then
      throw TableStoreException(s"No active pin for holder '${holder}' on table ${tableId}")
  }

  override def releasePin(tableId: Long, holder: String): Unit = tx {
    update(
      "DELETE FROM snapshot_pins WHERE table_id = ? AND holder = ?",
      st =>
        st.setLong(1, tableId)
        st.setString(2, holder)
    )
    ()
  }

  override def activePins(tableId: Long): Seq[SnapshotPin] =
    queryList(
      "SELECT table_id, snapshot_id, holder, expires_at FROM snapshot_pins WHERE table_id = ? AND expires_at > ?",
      st =>
        st.setLong(1, tableId)
        st.setLong(2, nowMicros)
    ) { rs =>
      SnapshotPin(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getLong(4))
    }

  // ---- GC helpers ----

  override def referencedKeys(tableId: Long): Set[String] =
    queryList(
      "SELECT DISTINCT s3_key FROM file_entries WHERE table_id = ?",
      st => st.setLong(1, tableId)
    )(rs => rs.getString(1)).toSet

  override def deletableEntries(tableId: Long): Seq[FileEntry] =
    // A retired entry's file may be deleted once no active pin holds any snapshot that still
    // sees it: NOT EXISTS pin p where begin <= p.snapshot < end
    queryList(
      """SELECT e.* FROM file_entries e
        |WHERE e.table_id = ? AND e.end_snapshot IS NOT NULL
        |AND NOT EXISTS (
        |  SELECT 1 FROM snapshot_pins p
        |  WHERE p.table_id = e.table_id AND p.expires_at > ?
        |    AND p.snapshot_id >= e.begin_snapshot AND p.snapshot_id < e.end_snapshot
        |)
        |ORDER BY e.id""".stripMargin,
      st =>
        st.setLong(1, tableId)
        st.setLong(2, nowMicros)
    )(mapEntry)

end JdbcCatalogStore

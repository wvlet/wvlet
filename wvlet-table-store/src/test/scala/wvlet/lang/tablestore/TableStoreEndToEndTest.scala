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

import wvlet.lang.tablestore.catalog.CatalogDrivers
import wvlet.lang.tablestore.merge.Merged
import wvlet.lang.tablestore.objectstore.ObjectStore
import wvlet.lang.tablestore.read.{Predicate, Scalar}
import wvlet.lang.tablestore.schema.ColumnDesc
import wvlet.lang.tablestore.schema.ColumnType
import wvlet.uni.json.JSON
import wvlet.uni.test.UniTest
import wvlet.uni.test.empty

import java.nio.file.Files

class TableStoreEndToEndTest extends UniTest:

  private def tempStore(): TableStore =
    val dir   = Files.createTempDirectory("wvlet-table-store-test")
    val store = TableStore(
      CatalogDrivers.sqlite(s"${dir}/catalog.db"),
      ObjectStore.local(s"${dir}/objects")
    )
    store.initialize()
    store

  private def row(pairs: (String, JSON.JSONValue)*): DataRow = JSON.JSONObject(pairs)

  test("ingested data is readable immediately; merge keeps results identical") {
    val store = tempStore()
    try
      store.createDatabase("main")
      val table = store.createTable("main", "usage")

      // Two ingest sessions append rows in separate small files
      val session1 = store.newIngestSession(table, "writer-1")
      session1.addAll(
        Seq(
          row(
            "user_id" -> JSON.JSONLong(1),
            "model"   -> JSON.JSONString("gpt"),
            "tokens"  -> JSON.JSONLong(120)
          ),
          row(
            "user_id" -> JSON.JSONLong(2),
            "model"   -> JSON.JSONString("claude"),
            "tokens"  -> JSON.JSONLong(80)
          )
        )
      )
      session1.close()

      val session2 = store.newIngestSession(table, "writer-2")
      session2.addAll(
        Seq(
          row(
            "user_id" -> JSON.JSONLong(3),
            "model"   -> JSON.JSONString("gpt"),
            "tokens"  -> JSON.JSONLong(20)
          )
        )
      )
      session2.close()
      val rawEntryIds = session1.ingestedEntryIds ++ session2.ingestedEntryIds

      val beforeMerge = store.reader.plan(table)
      beforeMerge.files.size shouldBe 2
      val rowsBefore = store.reader.scan(beforeMerge)
      rowsBefore.size shouldBe 3

      // Merge: both raw entries become one Parquet entry; same rows come back
      val merged =
        store.merger("merger-1").mergeOnce(table, leaseName = s"merge:${table.id}:all") match
          case m: Merged =>
            m
          case other =>
            sys.error(s"Expected a merge to happen, got ${other}")
      merged.sourceEntryIds.toSet shouldBe rawEntryIds.toSet
      merged.rowCount shouldBe 3L
      merged.quarantinedEntryIds shouldBe empty

      val plan = store.reader.plan(table)
      // Only the merged entry is live at the new snapshot
      plan.files.size shouldBe 1
      plan.files.head.entry.format shouldBe DataFormat.Parquet
      val rowsAfter = store.reader.scan(plan)
      rowsAfter.size shouldBe 3
      rowsAfter.map(_.get("user_id")).toSet shouldBe
        Set(Some(JSON.JSONLong(1)), Some(JSON.JSONLong(2)), Some(JSON.JSONLong(3)))
      // AS OF the first ingest snapshot still returns only the original raw file
      val asOfPlan = store.reader.plan(table, asOf = session1.flushedSnapshotIds.headOption)
      asOfPlan.files.map(f => (f.entry.format, f.entry.id)) shouldBe
        Seq((DataFormat.Jsonl, session1.ingestedEntryIds.head))
    finally
      store.catalog.close()
    end try
  }

  test("declared schema heads make fresh tables readable and casts fill nulls") {
    val store = tempStore()
    try
      store.createDatabase("main")
      val table = store.createTable(
        "main",
        "events",
        initialColumns = Seq(ColumnDesc("id", ColumnType.LongType))
      )
      val session = store.newIngestSession(table, "writer-1")
      session.add(row("id" -> JSON.JSONLong(7)))
      session.close()

      val rows = store.reader.scan(store.reader.plan(table))
      rows.size shouldBe 1
      rows.head.get("id") shouldBe Some(JSON.JSONLong(7))

      // A file carrying a pending column stays castable to the published one-column schema
      val session2 = store.newIngestSession(table, "writer-2")
      session2.add(row("id" -> JSON.JSONLong(8), "pending_col" -> JSON.JSONString("later")))
      session2.close()
      val rows2 = store.reader.scan(store.reader.plan(table))
      rows2.size shouldBe 2
      rows2.foreach(_.get("pending_col") shouldBe None)
    finally
      store.catalog.close()
  }

  test("catalog-side pruning skips files that cannot match the predicate") {
    val store = tempStore()
    try
      store.createDatabase("main")
      val table = store.createTable(
        "main",
        "metering",
        options = TableOptions(eventTimeColumn = Some("ts")),
        initialColumns = Seq(
          ColumnDesc("user", ColumnType.StringType),
          ColumnDesc("ts", ColumnType.LongType)
        )
      )
      val s = store.newIngestSession(table, "writer-1")
      // File A: user alice only; File B: user bob only — stats allow exact pruning on user
      s.addAll(Seq(row("user" -> JSON.JSONString("alice"), "ts" -> JSON.JSONLong(100))))
      s.flush()
      s.addAll(Seq(row("user" -> JSON.JSONString("bob"), "ts" -> JSON.JSONLong(200))))
      s.close()

      val predicate = Predicate.And(List(Predicate.Eq("user", Scalar.SString("alice"))))
      val plan      = store.reader.plan(table, predicate = Some(predicate))
      plan.prunedEntryCount shouldBe 1
      plan.files.size shouldBe 1
      val scanned = store.reader.scan(plan)
      scanned.size shouldBe 1
      scanned.head.get("user") shouldBe Some(JSON.JSONString("alice"))
    finally
      store.catalog.close()
  }

  test("merge escalates the schema lazily from observed data") {
    val store = tempStore()
    try
      store.createDatabase("main")
      val table = store.createTable("main", "logs")
      val s     = store.newIngestSession(table, "writer-1")
      s.addAll(Seq(row("msg" -> JSON.JSONString("a")), row("msg" -> JSON.JSONString("b"))))
      s.flush()
      // New column appears in later data — invisible until escalation
      s.addAll(Seq(row("msg" -> JSON.JSONString("c"), "level" -> JSON.JSONString("warn"))))
      s.close()

      // Before merge: only msg is visible (no published columns yet, so scans yield empty rows)
      val preSchema = store.reader.plan(table).schema
      preSchema.columns shouldBe empty

      val outcome =
        store.merger("merger-1").mergeOnce(table, s"merge:${table.id}:all") match
          case m: Merged =>
            m
          case other =>
            sys.error(s"Expected a merge, got ${other}")
      outcome.newSchemaVersion shouldBe Some(1)

      val postPlan = store.reader.plan(table)
      postPlan.schema.column("msg").get.columnType shouldBe ColumnType.StringType
      postPlan.schema.column("level").get.columnType shouldBe ColumnType.StringType
      val rows = store.reader.scan(postPlan)
      rows.size shouldBe 3
      // The escalated column is visible; rows predating it scan as explicit NULLs
      rows.map(_.get("level")).toSet shouldBe
        Set(Some(JSON.JSONString("warn")), Some(JSON.JSONNull()))
    finally
      store.catalog.close()
    end try
  }

  test("orphan detection subtracts catalog references from the store inventory") {
    val store = tempStore()
    try
      store.createDatabase("main")
      val table   = store.createTable("main", "t")
      val session = store.newIngestSession(table, "writer-1")
      session.add(row("x" -> JSON.JSONLong(1)))
      session.close()

      // An upload that never got registered (e.g. crash between upload and registration)
      store.objects.put(TableStore.rawKey(table.id, 9999, DataFormat.Jsonl), Array[Byte]())

      val orphans = store.orphanCandidates(table.id)
      orphans.map(_.key) shouldBe Seq(TableStore.rawKey(table.id, 9999, DataFormat.Jsonl))
    finally
      store.catalog.close()
  }

end TableStoreEndToEndTest

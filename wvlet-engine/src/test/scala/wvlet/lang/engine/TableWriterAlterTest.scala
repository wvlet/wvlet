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
package wvlet.lang.engine

import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.TableWriter.AlterOp
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.catalog.Warehouse
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** `alter table` on in-memory and warehouse tables, through [[TableWriter.alterTable]] */
class TableWriterAlterTest extends UniTest:
  private val schema = Schema(IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int32)))

  // a = 1, 2; b = 5, NULL
  private def batch: Batch = Batch(
    Array(Vec.fromValues(PType.Int64, Seq(1L, 2L)), Vec.fromValues(PType.Int32, Seq(5, null))),
    2
  )

  private def rows(t: TableSource): Seq[Seq[Any]] = t
    .scan(ScanSpec())
    .toSeq
    .flatMap(b => (0 until b.size).map(i => b.row(i).toSeq))
    .sortBy(r => String.valueOf(r.head))

  private def setups: Seq[(String, () => (TableWriter, WvletEngine))] = Seq(
    "in memory" -> { () =>
      val engine = WvletEngine()
      engine.catalog.register(MemTable("t", schema, Seq(batch)))
      (TableWriter(engine), engine)
    },
    "in a warehouse" -> { () =>
      val engine = WvletEngine()
      val w      = Warehouse(s"target/engine-alter/${System.nanoTime()}")
      engine.catalog.register(w.replace("t", schema, Seq(batch)))
      (TableWriter(engine, Some(w)), engine)
    }
  )

  setups.foreach { (label, setup) =>
    test(s"a failed set not null leaves the table's constraints as they were, ${label}") {
      val (writer, _) = setup()
      // b holds a NULL, so the check fails each time
      (1 to 2).foreach { _ =>
        intercept[WvletLangException] {
          writer.alterTable("t", Seq(AlterOp.SetNotNull("b")))
        }.getMessage shouldContain "NOT NULL"
      }
      writer.alterTable("t", Seq(AlterOp.SetNotNull("a")))
    }

    test(s"find columns ignoring case and cast defaults with the column, ${label}") {
      val (writer, engine) = setup()
      val t                = writer.alterTable(
        "t",
        Seq(
          AlterOp.SetDefault("b", 7L, PType.Int64),
          AlterOp.SetType("B", PType.Int64),
          AlterOp.DropColumn("A")
        )
      )
      t.schema.fields.map(f => (f.name, f.ptype)) shouldBe Seq(("b", PType.Int64))
      rows(engine.catalog.find("t").get) shouldBe Seq(Seq(5L), Seq(null))
    }
  }

end TableWriterAlterTest

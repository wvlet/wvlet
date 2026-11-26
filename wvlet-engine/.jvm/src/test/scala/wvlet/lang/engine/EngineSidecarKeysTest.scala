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

import wvlet.lang.engine.catalog.ForeignKey
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.TableKeys
import wvlet.uni.json.JSON
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/** Key declarations of Parquet data in sidecar files (`<name>.keys.json`, `_keys.json`) */
class EngineSidecarKeysTest extends UniTest:
  private val root = Paths.get("target/engine-test/sidecar-keys").toAbsolutePath

  /** A fresh folder holding copies of spec/tpch/data tables */
  private def folder(name: String, tables: String*): Path =
    val dir = root.resolve(name)
    if Files.exists(dir) then
      Files.list(dir).forEach(Files.delete)
    Files.createDirectories(dir)
    tables.foreach(t =>
      Files.copy(
        Paths.get(s"spec/tpch/data/${t}.parquet"),
        dir.resolve(s"${t}.parquet"),
        StandardCopyOption.REPLACE_EXISTING
      )
    )
    dir

  private def write(p: Path, json: String): Unit = Files.writeString(p, json)

  test("parse primary, unique and foreign keys") {
    val keys = TableKeys.parse(
      JSON.parse(
        """{"primaryKey": ["a", "b"], "uniqueKeys": [["c"], ["a", "b"]],
          | "foreignKeys": [{"columns": ["d"], "references": "t", "referencedColumns": ["k"]}]}"""
          .stripMargin
      )
    )
    keys.uniqueKeys shouldBe Seq(Seq("a", "b"), Seq("c"))
    keys.foreignKeys shouldBe Seq(ForeignKey(Seq("d"), "t", Seq("k")))
    // Malformed parts are dropped
    TableKeys
      .parse(JSON.parse("""{"primaryKey": "a", "uniqueKeys": [["b"], 1]}"""))
      .uniqueKeys shouldBe Seq(Seq("b"))
    TableKeys.parse(JSON.parse("[1]")).isEmpty shouldBe true
  }

  test("read a Parquet file's keys from <name>.keys.json, dropping unknown columns") {
    val dir = folder("file", "partsupp")
    write(
      dir.resolve("partsupp.keys.json"),
      """{"primaryKey": ["ps_partkey", "ps_suppkey"], "uniqueKeys": [["no_such_column"]],
        | "foreignKeys": [{"columns": ["ps_partkey"], "references": "part"}]}""".stripMargin
    )
    val s = ParquetSource("partsupp", dir.resolve("partsupp.parquet").toString)
    s.uniqueKeys shouldBe Seq(Seq("ps_partkey", "ps_suppkey"))
    s.foreignKeys shouldBe Seq(ForeignKey(Seq("ps_partkey"), "part", Nil))
    // Keys given at registration win over the sidecar
    ParquetSource(
      "partsupp",
      dir.resolve("partsupp.parquet").toString,
      declaredKeys = Seq(Seq("ps_partkey"))
    ).uniqueKeys shouldBe Seq(Seq("ps_partkey"))
  }

  test("read the keys of a folder's tables from its _keys.json") {
    val dir = folder("tables", "nation", "region")
    write(dir.resolve("_keys.json"), """{"tables": {"nation": {"primaryKey": ["n_nationkey"]}}}""")
    ParquetSource("nation", dir.resolve("nation.parquet").toString).uniqueKeys shouldBe
      Seq(Seq("n_nationkey"))
    ParquetSource("region", dir.resolve("region.parquet").toString).uniqueKeys shouldBe Nil
  }

  test("read a folder table's keys from its _keys.json") {
    val dir = folder("folder-table", "region")
    write(dir.resolve("_keys.json"), """{"primaryKey": ["r_regionkey"]}""")
    ParquetSource("region", dir.toString).uniqueKeys shouldBe Seq(Seq("r_regionkey"))
  }

  test("declare no keys for a missing or unreadable sidecar") {
    val dir = folder("broken", "region", "nation")
    write(dir.resolve("region.keys.json"), "{not json")
    ParquetSource("region", dir.resolve("region.parquet").toString).uniqueKeys shouldBe Nil
    ParquetSource("nation", dir.resolve("nation.parquet").toString).uniqueKeys shouldBe Nil
  }

  test("register Parquet tables with the keys of their sidecar files") {
    val dir = folder("query", "lineitem", "partsupp")
    write(dir.resolve("partsupp.keys.json"), """{"primaryKey": ["ps_partkey", "ps_suppkey"]}""")
    write(dir.resolve("lineitem.keys.json"), """{"primaryKey": ["l_orderkey", "l_linenumber"]}""")
    val engine = WvletEngine()
    engine.catalog.registerParquet("lineitem", dir.resolve("lineitem.parquet").toString)
    engine.catalog.registerParquet("partsupp", dir.resolve("partsupp.parquet").toString)
    engine.catalog.find("partsupp").get.uniqueKeys shouldBe Seq(Seq("ps_partkey", "ps_suppkey"))
  }

end EngineSidecarKeysTest

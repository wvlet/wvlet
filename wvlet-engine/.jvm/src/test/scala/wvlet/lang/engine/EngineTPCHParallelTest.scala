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

import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * TPC-H queries on the engine with the tables rewritten in small row groups, so that scans, join
  * probes, and their filters run as parallel morsels, and with grouped aggregations, join builds,
  * sorts, and windows parallel at any size, must return the same rows as DuckDB
  */
class EngineTPCHParallelTest extends UniTest:
  // Partition grouped aggregations, join builds, sorts, and windows at any size
  private val minRowsKeys = Seq(
    "wvlet.engine.partitionedAggMinRows",
    "wvlet.engine.partitionedJoinMinRows",
    "wvlet.engine.parallelSortMinRows",
    "wvlet.engine.parallelWindowMinRows"
  )

  minRowsKeys.foreach(k => sys.props(k) = "1")

  private val tables = EngineTPCHBench.tables

  private val dataDir: String =
    val dir = Paths.get("target/engine-test/tpch-small-row-groups").toAbsolutePath
    Files.createDirectories(dir)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try tables.foreach { t =>
        EngineTPCHBench.execute(
          conn,
          s"copy (select * from 'spec/tpch/data/${t}.parquet') to '${dir}/${t}.parquet' (format parquet, row_group_size 2048)"
        )
      }
    finally conn.close()
    dir.toString

  private val conformance = EngineConformance(
    "spec/tpch",
    tables.map(t => s"create view ${t} as select * from '${dataDir}/${t}.parquet'"),
    engine => tables.foreach(t => engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet"))
  )

  override def afterAll: Unit =
    minRowsKeys.foreach(sys.props.remove)
    conformance.close()

  test("should split the scans into parts") {
    conformance
      .engine
      .catalog
      .get("lineitem")
      .scanSplits(wvlet.lang.engine.catalog.ScanSpec()) match
      case Some(s) =>
        try (s.parts.size > 10) shouldBe true
        finally s.close()
      case None =>
        fail("lineitem should have several row groups")
  }

  for unit <- conformance.units do
    test(unit.sourceFile.fileName) {
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }

end EngineTPCHParallelTest

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
package wvlet.lang.runner.connector

import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.connector.duckdb.DuckDBConnector
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.vector.PType
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

/**
  * Writing a JDBC engine's result set to local files with the wvlet engine's writers. DuckDB stands
  * in for a JDBC engine (e.g. Snowflake) here; DuckDB itself writes files with COPY TO in practice
  */
class LocalFileExportTest extends UniTest:
  given QueryProgressMonitor = QueryProgressMonitor.noOp

  private val source = DuckDBConnector(WorkEnv())
  private val dir = Files.createTempDirectory(Files.createDirectories(Path.of("target")), "export")

  override def afterAll: Unit = source.close()

  private val query =
    """select * from (values
      |  (1, 'a', date '2026-01-02', 1.50::decimal(10, 2)),
      |  (2, null, null, null)
      |) as t(id, name, d, amt)""".stripMargin

  test("should write a JDBC result to Parquet with the source column types") {
    val path = dir.resolve("out.parquet").toString
    LocalFileExport.write(source, query, path, Map("format" -> "parquet")) shouldBe 2L
    val t = ParquetSource("t", path)
    t.schema.types shouldBe IndexedSeq(PType.Int32, PType.Str, PType.Date, PType.Decimal(10, 2))
    val b = t.scan(ScanSpec()).toSeq.head
    b.column(1).get(0) shouldBe "a"
    b.column(2).isNull(1) shouldBe true
  }

  test("should write a JDBC result to CSV with a header") {
    val path = dir.resolve("out.csv").toString
    LocalFileExport.write(source, query, path, Map("format" -> "csv")) shouldBe 2L
    val lines = Files.readAllLines(Path.of(path))
    lines.get(0) shouldBe "id,name,d,amt"
    lines.get(1) shouldBe "1,a,2026-01-02,1.50"
  }

  test("should write the schema of an empty result") {
    val path = dir.resolve("empty.csv").toString
    LocalFileExport.write(source, s"${query} where id > 100", path, Map("format" -> "csv")) shouldBe
      0L
    Files.readString(Path.of(path)).trim shouldBe "id,name,d,amt"
  }

end LocalFileExportTest

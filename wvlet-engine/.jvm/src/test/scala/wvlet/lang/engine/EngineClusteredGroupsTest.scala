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

import wvlet.lang.engine.catalog.ParquetSource
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/** The sampled run lengths of Parquet columns that `clustered_group_estimates` reads */
class EngineClusteredGroupsTest extends UniTest:

  /**
    * 20480 rows in 10 row groups of 2048: orders of 8 lines sharing the customer and a string
    * channel, amounts varying per line, and NULL customers on whole orders
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("clustered_groups.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    case when (range // 8) % 5 = 0 then null else ((range // 8) % 997)::bigint end as cust,
             |    'c' || ((range // 8) % 3) as channel,
             |    (range % 13)::integer as amt,
             |    1.5 as price
             |  from range(20480)
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  test("measure the mean run of equal consecutive values in sampled row groups") {
    val src = ParquetSource("t", file)
    src.keyRunLength("cust") shouldBe Some(8.0)
    src.keyRunLength("channel") shouldBe Some(8.0)
    src.keyRunLength("id") shouldBe Some(1.0)
    // amt repeats every 13 rows, never on consecutive rows
    src.keyRunLength("amt") shouldBe Some(1.0)
    // Not an integer, date or string column
    src.keyRunLength("price") shouldBe None
    src.keyRunLength("missing") shouldBe None
  }

end EngineClusteredGroupsTest

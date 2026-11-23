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

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ColumnVector
import wvlet.uni.cloud.parquet.Parquet
import wvlet.uni.cloud.parquet.ParquetReadOptions
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * Parquet scans hand uni's decoded arrays to engine vectors by reference: a copy here would undo
  * the allocation savings of uni's flat-column reader
  */
class EngineParquetVecSharingTest extends UniTest:

  private val numRows = 1000

  /** Flat columns of every fixed-size type, each with a nullable and a non-null variant */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("vec_sharing.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(
          s"""copy (
             |  select
             |    range::integer as i,
             |    case when range % 3 = 0 then null else range::integer end as ni,
             |    range::bigint as l,
             |    case when range % 3 = 0 then null else range::bigint end as nl,
             |    range::double as d,
             |    case when range % 3 = 0 then null else range::double end as nd,
             |    case when range % 3 = 0 then null else range % 2 = 0 end as nb,
             |    case when range % 3 = 0 then null else 'v' || (range % 7) end as ns
             |  from range(${numRows})
             |) to '${path}' (format parquet)""".stripMargin
        )
      finally st.close()
    finally
      conn.close()
    path.toString

  private val cols  = Seq("i", "ni", "l", "nl", "d", "nd", "nb", "ns")
  private val types = Seq(
    PType.Int32,
    PType.Int32,
    PType.Int64,
    PType.Int64,
    PType.Float64,
    PType.Float64,
    PType.Bool,
    PType.Str
  )

  /** Each uni batch of the file paired with the engine batch made from it */
  private def batches: Seq[(wvlet.uni.cloud.parquet.ColumnBatch, Batch)] =
    val f = Parquet.open(file)
    try
      val options = ParquetReadOptions.default.withColumns(cols*).withBinaryAsString(true)
      f.readBatches(options).map(b => (b, ParquetSource.toBatch(b, cols, types))).toList
    finally
      f.close()

  test("should share uni's value and null arrays in fixed-size vectors") {
    val bs = batches
    bs.map(_._2.size).sum shouldBe numRows
    bs.foreach { (u, e) =>
      cols
        .zip(e.columns)
        .foreach { (name, vec) =>
          val c = u.column(name)
          vec.size shouldBe u.numRows
          (c, vec) match
            case (x: ColumnVector.IntColumn, v: IntVec) =>
              v.values shouldBeTheSameInstanceAs x.values
              v.nulls shouldBeTheSameInstanceAs x.nullFlags
            case (x: ColumnVector.LongColumn, v: LongVec) =>
              v.values shouldBeTheSameInstanceAs x.values
              v.nulls shouldBeTheSameInstanceAs x.nullFlags
            case (x: ColumnVector.DoubleColumn, v: DoubleVec) =>
              v.values shouldBeTheSameInstanceAs x.values
              v.nulls shouldBeTheSameInstanceAs x.nullFlags
            case (x: ColumnVector.BooleanColumn, v: BoolVec) =>
              v.values shouldBeTheSameInstanceAs x.values
              v.nulls shouldBeTheSameInstanceAs x.nullFlags
            case (x: ColumnVector.BinaryColumn, v: StrVec) =>
              // Strings are decoded into Array[String], so only the null mask is shared
              v.nulls shouldBeTheSameInstanceAs x.nullFlags
            case other =>
              fail(s"Unexpected vector pair for ${name}: ${other}")
        }
    }
  }

  test("should keep null rows and skip a null mask for columns without nulls") {
    val all = batches.map(_._2)
    def column(name: String): Seq[Any] =
      val k = cols.indexOf(name)
      all.flatMap(b => (0 until b.size).map(b.columns(k).get))

    all.foreach { b =>
      b.columns(cols.indexOf("i")).asInstanceOf[IntVec].nulls shouldBe null
      b.columns(cols.indexOf("l")).asInstanceOf[LongVec].nulls shouldBe null
    }
    val ni = column("ni")
    val ns = column("ns")
    (0 until numRows).foreach { r =>
      if r % 3 == 0 then
        ni(r) shouldBe null
        ns(r) shouldBe null
      else
        ni(r) shouldBe r
        ns(r) shouldBe s"v${r % 7}"
    }
  }

end EngineParquetVecSharingTest

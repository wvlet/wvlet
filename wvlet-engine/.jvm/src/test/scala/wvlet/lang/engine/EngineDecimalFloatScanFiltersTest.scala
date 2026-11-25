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

import wvlet.lang.compiler.*
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.cloud.parquet.Parquet
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.cloud.parquet.ParquetFilter.col
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * Row groups skipped by decimal and floating-point statistics (`decimal_float_scan_filters`):
  * DECIMAL as INT32, INT64 and FIXED_LEN_BYTE_ARRAY, and a DOUBLE column holding -0.0 and NaN. Also
  * the exact row filters of `decimal_float_row_filters` over the same file
  */
class EngineDecimalFloatScanFiltersTest extends UniTest:
  private val on  = WvletEngine(optimizer = OptimizerOptions().withValidatePlans(true))
  private val off = WvletEngine(optimizer =
    OptimizerOptions().withValidatePlans(true).withDisabled(Optimization.DecimalFloatScanFilters)
  )

  private val noRowFilters = WvletEngine(optimizer =
    OptimizerOptions().withValidatePlans(true).withDisabled(Optimization.DecimalFloatRowFilters)
  )

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /**
    * 20480 rows in 10 row groups of 2048, sorted by every column: decimals (i - 10240) / 100
    * (-102.40 .. 102.39, 20.48 per row group) and a double i - 10240, except NaN in row 2048 (a row
    * group of negative values) and -0.0 in row 16384 (a row group of positive values)
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("decimal_row_groups.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
           |  select
           |    range::bigint as id,
           |    ((range - 10240)::decimal(9, 0) / 100)::decimal(9, 2) as d32,
           |    ((range - 10240)::decimal(18, 0) / 100)::decimal(18, 2) as d64,
           |    ((range - 10240)::decimal(30, 0) / 100)::decimal(30, 2) as d128,
           |    case when range = 2048 then 'nan'::double when range = 16384 then '-0.0'::double
           |      else (range - 10240)::double end as x
           |  from range(20480)
           |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  private def query[A](q: String)(body: Context ?=> Relation => A): A =
    val unit           = CompilationUnit.fromWvletString(q)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    EngineTPCHBench.queryOf(unit) match
      case Some(rel) =>
        body(rel)
      case None =>
        fail(s"No query in ${q}")

  private def rows(e: WvletEngine, q: String): Seq[IndexedSeq[Any]] =
    query(q)(rel => e.execute(rel).rows.toSeq)

  private def explain(q: String): String = query(q)(rel => on.explain(rel))

  /** How many row groups a scan of `file` with `filter` reads */
  private def groupsRead(filter: ParquetFilter): Int =
    val f = Parquet.open(file)
    try
      val spec            = ScanSpec(filter = Some(filter))
      val (_, _, options) = ParquetSource.readOptions(ParquetSource(file, file).schema, spec)
      ParquetSource.selectRowGroups(f, spec, options).size
    finally
      f.close()

  test("should skip row groups by INT32, INT64 and FIXED_LEN_BYTE_ARRAY decimal statistics") {
    for c <- Seq("d32", "d64", "d128") do
      groupsRead(col(c) >= JBigDecimal("81.92")) shouldBe 1
      groupsRead(col(c) > JBigDecimal("102.39")) shouldBe 0
      groupsRead(col(c) < JBigDecimal("-81.92")) shouldBe 1
      groupsRead(col(c) <= JBigDecimal("-81.92")) shouldBe 2
      groupsRead(col(c) === JBigDecimal("0.05")) shouldBe 1
      // Another scale compares by value
      groupsRead(col(c) === JBigDecimal("0.050")) shouldBe 1
      groupsRead(col(c) < JBigDecimal("-81.92") || col(c) >= JBigDecimal("81.92")) shouldBe 2
      // A non-decimal part of the filter is left to uni
      groupsRead(col(c) >= JBigDecimal("81.92") && col("id") < 2048L) shouldBe 0
  }

  test("should attach decimal and floating-point comparisons to the scan filter") {
    val plan = explain(s"from '${file}' where d64 >= 81.9 and x < 10.0 select id")
    debug(plan)
    plan shouldContain "Compare(d64,GtEq,81.9"
    plan shouldContain "Compare(x,Lt,10.0)"
  }

  test("should return the same rows with and without decimal and float scan filters") {
    val queries = Seq(
      "d32 >= 81.9",
      "d64 > 102.38",
      "d128 < -102.3",
      "d64 = 0.05",
      "d32 between -0.5 and 0.5",
      "d64 < -102.3 or d128 >= 102.35",
      // -0.0 (row 16384) is the only value below 0.0 in its row group
      "x < 0.0",
      "x <= -0.0",
      "x = 0.0",
      "x < -10239.0",
      // NaN (row 2048) is above every number, but its row group's statistics hold negatives only
      "x > 10237.0",
      "x >= 10238.0",
      "x != 5.0"
    )
    for q <- queries do
      val query    = s"from '${file}' where ${q} select id order by id"
      val expected = rows(off, query)
      debug(s"${q}: ${expected.size} rows")
      rows(on, query) shouldBe expected
    rows(on, s"from '${file}' where x < 0.0 select id").map(_.head) shouldContain 16384L
    rows(on, s"from '${file}' where x > 10237.0 select id order by id").map(_.head) shouldBe
      Seq(2048L, 20478L, 20479L)
  }

  test("should move exact decimal and floating-point conjuncts into the row filter") {
    val plan = explain(
      s"from '${file}' where id < 1024 and d64 >= 81.9 and d32 < 0.5 and x < 10.0 and d128 > 0.0" +
        " and x > 1.0 select id"
    )
    debug(plan)
    plan shouldContain "row_filter="
    val rowFilter = plan.split("row_filter=")(1).split(" -> ")(0)
    // Decimal and floating-point conjuncts follow the estimated one
    rowFilter.indexOf("Compare(id,") < rowFilter.indexOf("Compare(d64,") shouldBe true
    rowFilter shouldContain "Compare(d64,GtEq,81.9"
    rowFilter shouldContain "Compare(d32,Lt,0.5"
    rowFilter shouldContain "Compare(x,Lt,10.0)"
    // A FIXED_LEN_BYTE_ARRAY decimal, and x > 1.0 which NaN satisfies in the engine, stay out
    rowFilter.contains("d128") shouldBe false
    rowFilter.contains("Compare(x,Gt") shouldBe false
  }

  test("should return the same rows with and without decimal and float row filters") {
    val queries = Seq(
      "d32 >= 81.9",
      "d64 > 102.38",
      "d64 = 0.05",
      "d64 in (0.05, 0.07, 1.00)",
      "d32 between -0.5 and 0.5 and d64 < 0.3",
      "d64 < -102.3 or d32 >= 102.35",
      "d64 >= 1.0 and d128 < 2.0",
      "x < 0.0",
      "x <= -0.0",
      "x = 0.0",
      "x < -10239.0",
      "x <= 10.0 and x = 5.0",
      "x > 10237.0",
      "x != 5.0 and d64 < 0.0"
    )
    // Selective id windows (10% each), so that the row filter runs: around the NaN row (2048), the
    // zero crossing, and the -0.0 row (16384)
    val windows = Seq("id between 1024 and 3071", "id between 9216 and 11263", "id between 15360 and 17407")
    for
      w <- windows
      q <- queries
    do
      val query    = s"from '${file}' where ${w} and (${q}) select id order by id"
      explain(query) shouldContain "row_filter="
      val expected = rows(noRowFilters, query)
      debug(s"${w} and ${q}: ${expected.size} rows")
      rows(on, query) shouldBe expected
  }

end EngineDecimalFloatScanFiltersTest

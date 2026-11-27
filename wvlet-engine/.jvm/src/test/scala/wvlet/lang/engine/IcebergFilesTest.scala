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
import wvlet.lang.engine.catalog.IcebergWriter
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `dataset_file_scans`: an Iceberg table is read file by file through uni's dataset reader, split
  * into its data files and with its manifests' statistics, returning the rows uni's dataset reader
  * returns for the whole table
  */
class IcebergFilesTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.DatasetFileScans))

  private val root = Paths.get("target/engine-test/iceberg_files").toAbsolutePath

  private def clean(p: Path): Unit =
    if Files.isDirectory(p) then
      Files.list(p).forEach(clean)
    Files.deleteIfExists(p)

  /** Write ids [from, from + rows) with a few more columns, in row groups of 2048 */
  private def write(path: Path, from: Int, rows: Int): Unit =
    Files.createDirectories(path.getParent)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    (range % 7)::integer as k,
             |    case when range % 11 = 0 then null else 'm' || (range % 5) end as mode
             |  from range(${from}, ${from + rows})
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()

  /** An Iceberg table of 4 snapshots, each appending a data file of 4096 ids */
  private lazy val table: String =
    val dir = root.resolve("table")
    clean(dir)
    clean(root.resolve("input"))
    (0 until 4).foreach { i =>
      val file = root.resolve("input").resolve(s"part-${i}.parquet")
      write(file, i * 4096, 4096)
      val src     = ParquetSource("input", file.toString)
      val batches = src.scan(ScanSpec()).toSeq
      if i == 0 then
        IcebergWriter.create(dir.toString, src.schema, batches, Map.empty)
      else
        IcebergWriter.append(dir.toString, src.schema, batches)
    }
    dir.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def rows(e: WvletEngine, query: String): Seq[String] =
    val unit           = CompilationUnit.fromWvletString(query)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    e.execute(q).rows.map(_.mkString(",")).toSeq.sorted

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def ids(src: ParquetSource, spec: ScanSpec): Seq[Long] =
    src
      .scan(spec)
      .flatMap(b => (0 until b.size).map(i => b.column(0).get(i).asInstanceOf[Long]))
      .toSeq

  private def splitIds(src: ParquetSource, spec: ScanSpec): Seq[Seq[Long]] =
    val splits = src.scanSplits(spec).getOrElse(fail("no splits"))
    try splits
        .parts
        .map(p => p().flatMap(b => (0 until b.size).map(i => b.column(0).get(i))).toSeq)
        .map(_.map(_.asInstanceOf[Long]))
    finally splits.close()

  test("split an Iceberg table into its data files, with the manifests' statistics") {
    val src = ParquetSource("t", table)
    src.canSplit shouldBe true
    src.supportsRowFilter shouldBe true
    src.estimatedRows shouldBe Some(16384L)
    src.columnRange("id") shouldBe Some((0L, 16383L))
    src.columnRange("k") shouldBe Some((0L, 6L))
    src.partRanges("id").map(_.size) shouldBe Some(4)
    src.estimatedDistinct("id") shouldBe Some(16384.0)
    src.valueCounts("mode").isDefined shouldBe true
    val spec  = ScanSpec(Some(Seq("id")))
    val parts = splitIds(src, spec)
    parts.size shouldBe 4
    parts.flatten.sorted shouldBe (0L until 16384L)
    // The dataset reader returns the same rows
    ids(src, spec).sorted shouldBe ids(src.withoutDatasetFiles, spec).sorted
    src.withoutDatasetFiles.canSplit shouldBe false
    src.withoutDatasetFiles.estimatedRows shouldBe None
  }

  test("skip the data files the manifests' statistics exclude, and filter rows exactly") {
    val src = ParquetSource("t", table)
    val ge  = ScanSpec(Some(Seq("id")), filter = Some(ParquetFilter.col("id") >= 8192L))
    splitIds(src, ge).map(_.min).sorted shouldBe Seq(8192L, 12288L)
    val exact = ScanSpec(
      Some(Seq("id")),
      rowFilter = Some(ParquetFilter.col("id") >= 4090L && ParquetFilter.col("id") < 4100L)
    )
    splitIds(src, exact).flatten.sorted shouldBe (4090L until 4100L)
    ids(src, exact).sorted shouldBe (4090L until 4100L)
  }

  test("return the same rows with dataset_file_scans on and off") {
    val queries = Seq(
      s"from '${table}' where id < 100 or id >= 16300 select count(*), sum(k)",
      s"from '${table}' where mode = 'm3' group by k select k, count(*)",
      s"""from '${table}' as a
         |join { from '${table}' where id % 997 = 0 select id as bid } on a.id = bid
         |select a.id, a.mode""".stripMargin
    )
    queries.foreach { q =>
      val r = rows(on, q)
      r.nonEmpty shouldBe true
      r shouldBe rows(off, q)
    }
  }

end IcebergFilesTest

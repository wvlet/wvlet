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
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `folder_file_scans`: a folder of plain Parquet files is read as its files, split into their row
  * groups and with their statistics, returning the rows uni's dataset reader returns
  */
class ParquetFolderTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.FolderFileScans))

  private val root = Paths.get("target/engine-test/parquet_folder").toAbsolutePath

  private def clean(p: Path): Unit =
    if Files.isDirectory(p) then
      Files.list(p).forEach(clean)
    Files.deleteIfExists(p)

  /** Write ids [from, from + rows) with a few more columns, in row groups of 2048 */
  private def write(path: Path, from: Int, rows: Int, extra: String = ""): Unit =
    Files.createDirectories(path.getParent)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    (range % 7)::integer as k,
             |    case when range % 11 = 0 then null else 'm' || (range % 5) end as mode${extra}
             |  from range(${from}, ${from + rows})
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()

  /** 3 files of 8192, 8192 and 4096 ids: 4, 4 and 2 row groups */
  private lazy val folder: String =
    val dir = root.resolve("files")
    clean(dir)
    write(dir.resolve("part-0.parquet"), 0, 8192)
    write(dir.resolve("part-1.parquet"), 8192, 8192)
    write(dir.resolve("part-2.parquet"), 16384, 4096)
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

  test("split a folder's scan into every file's row groups, with the files' statistics") {
    val src = ParquetSource("t", folder)
    src.canSplit shouldBe true
    src.supportsRowFilter shouldBe true
    src.estimatedRows shouldBe Some(20480L)
    src.columnRange("id") shouldBe Some((0L, 20479L))
    src.partRanges("id").map(_.size) shouldBe Some(10)
    src.estimatedDistinct("id") shouldBe Some(20480.0)
    src.estimatedDistinct("mode") shouldBe Some(5.0)
    src.keyRunLength("k") shouldBe Some(1.0)
    src.valueCounts("mode").isDefined shouldBe true
    val spec   = ScanSpec(Some(Seq("id")))
    val splits = src.scanSplits(spec).getOrElse(fail("no splits"))
    try
      splits.parts.size shouldBe 10
      splits.rows shouldBe Seq.fill(10)(2048L)
      splits.parts.flatMap(_()).map(_.size).sum shouldBe 20480
    finally
      splits.close()
    // The dataset reader returns the same rows, in the same order
    ids(src, spec) shouldBe ids(src.withoutFolderFiles, spec)
    src.withoutFolderFiles.canSplit shouldBe false
    src.withoutFolderFiles.estimatedRows shouldBe None
  }

  test("read partitioned folders and folders of differing files through the dataset reader") {
    val partitioned = root.resolve("partitioned")
    clean(partitioned)
    write(partitioned.resolve("p=0").resolve("data.parquet"), 0, 4096)
    write(partitioned.resolve("p=1").resolve("data.parquet"), 4096, 4096)
    val differing = root.resolve("differing")
    clean(differing)
    write(differing.resolve("a.parquet"), 0, 4096)
    write(differing.resolve("b.parquet"), 4096, 4096, extra = ", range * 2 as more")
    Seq(partitioned, differing).foreach { dir =>
      val src = ParquetSource("t", dir.toString)
      src.canSplit shouldBe false
      src.supportsRowFilter shouldBe false
      src.estimatedRows shouldBe None
      rows(on, s"from '${dir}' select count(*), sum(id)") shouldBe Seq("8192,33550336")
    }
  }

  test("list a folder again once it changes") {
    val dir = root.resolve("growing")
    clean(dir)
    write(dir.resolve("part-0.parquet"), 0, 4096)
    val src = ParquetSource("t", dir.toString)
    src.estimatedRows shouldBe Some(4096L)
    write(dir.resolve("part-1.parquet"), 4096, 4096)
    // Listed again at most every second
    Thread.sleep(1100)
    src.estimatedRows shouldBe Some(8192L)
    ids(src, ScanSpec(Some(Seq("id")))).size shouldBe 8192
  }

  test("return the same rows with folder_file_scans on and off") {
    val queries = Seq(
      s"from '${folder}' where id < 100 or id >= 20400 select count(*), sum(k)",
      s"from '${folder}' where mode = 'm3' group by k select k, count(*)",
      s"""from '${folder}' as a
         |join { from '${folder}' where id % 997 = 0 select id as bid } on a.id = bid
         |select a.id, a.mode""".stripMargin
    )
    queries.foreach { q =>
      val r = rows(on, q)
      r.nonEmpty shouldBe true
      r shouldBe rows(off, q)
    }
  }

end ParquetFolderTest

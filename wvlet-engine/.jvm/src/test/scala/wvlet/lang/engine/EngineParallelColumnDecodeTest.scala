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
import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.Batch
import wvlet.lang.model.plan.*
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

/**
  * `parallel_column_decode`: a scan reading a single row group decodes its columns as tasks of the
  * runner, with the same rows as one decode
  */
class EngineParallelColumnDecodeTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.ParallelColumnDecode))

  /** A runner over the default one that counts the tasks submitted to it */
  private class CountingRunner extends TaskRunner:
    val submitted                                      = AtomicInteger(0)
    private val base                                   = TaskRunner.default
    def parallelism: Int                               = math.max(2, base.parallelism)
    def submit[A](task: () => A): TaskRunner.Handle[A] =
      submitted.incrementAndGet()
      base.submit(task)

  private def writeParquet(name: String, rows: Int, rowGroupSize: Int): String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve(name)
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    'name#' || range as name,
             |    case when range % 7 = 0 then null else 'tag' || (range % 5) end as tag,
             |    (range % 25)::integer as k,
             |    (range % 1000)::decimal(15, 2) as amount,
             |    date '2020-01-01' + (range % 365)::integer as day
             |  from range(${rows})
             |) to '${path}' (format parquet, row_group_size ${rowGroupSize})""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  // One row group of 300000 rows: names ~4.4 MB uncompressed, ids ~2.4 MB, and the other columns
  // small dictionaries (under 0.4 MB each)
  private lazy val single: String = writeParquet("parallel_column_decode.parquet", 300000, 1000000)
  private lazy val small: String  = writeParquet(
    "parallel_column_decode_small.parquet",
    1000,
    100000
  )

  private def values(it: Iterator[Batch]): Seq[Seq[Any]] =
    val out = Seq.newBuilder[Seq[Any]]
    it.foreach { b =>
      (0 until b.size).foreach(i => out += b.columns.map(_.get(i)).toSeq)
    }
    out.result()

  test("decode the columns of a single row group as tasks, with the same rows") {
    val src    = ParquetSource("t", single)
    val runner = CountingRunner()
    Seq(ScanSpec(), ScanSpec(dictionaryCodes = true, lazyStrings = true)).foreach { spec =>
      runner.submitted.set(0)
      val parallel = values(src.scan(spec.copy(decodeRunner = Some(runner))))
      // Tasks of at least 2 MB: name, and id with the small columns. The calling thread takes the
      // first
      runner.submitted.get() shouldBe 1
      parallel.size shouldBe 300000
      parallel shouldBe values(src.scan(spec))
    }
    // Only the requested columns, in their order: name, and id + tag
    runner.submitted.set(0)
    val spec = ScanSpec(columns = Some(Seq("tag", "name", "id")), decodeRunner = Some(runner))
    values(src.scan(spec)) shouldBe values(src.scan(spec.copy(decodeRunner = None)))
    runner.submitted.get() shouldBe 1
  }

  test("decode on one thread with a row filter, one column, or few bytes") {
    val runner = CountingRunner()
    val src    = ParquetSource("t", single)
    val filter = ParquetFilter.col("k").isIn(Seq(3L, 4L))
    val rf     = ScanSpec(rowFilter = Some(filter), decodeRunner = Some(runner))
    values(src.scan(rf)).size shouldBe 24000
    // id and tag together are one task of ~2.5 MB
    values(src.scan(ScanSpec(columns = Some(Seq("id", "tag")), decodeRunner = Some(runner))))
      .size shouldBe 300000
    values(src.scan(ScanSpec(columns = Some(Seq("name")), decodeRunner = Some(runner))))
      .size shouldBe 300000
    values(ParquetSource("s", small).scan(ScanSpec(decodeRunner = Some(runner)))).size shouldBe 1000
    runner.submitted.get() shouldBe 0
  }

  // The probe side: 20480 rows in row groups of 2048, so that its scan splits into parts
  private lazy val fact: String = writeParquet("parallel_column_decode_fact.parquet", 20480, 2048)

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def rows(e: WvletEngine, query: String): Seq[String] =
    val unit = CompilationUnit.fromWvletString(
      query.replace("$d", s"'${single}'").replace("$f", s"'${fact}'")
    )
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

  test("return the same rows with the toggle on and off") {
    // The single row group of $d is the build side of the join
    val query =
      """from $f as f
        |join $d as d on f.k = d.id
        |where f.id < 1000
        |select d.name, d.amount, d.day, f.id""".stripMargin
    val r = rows(on, query)
    // Every fact row's k (0-24) is a dim id. The filter is on the fact side, so the dim scan has no
    // row filter and reads its single row group with parallel decode
    r.size shouldBe 1000
    r shouldBe rows(off, query)
  }

end EngineParallelColumnDecodeTest

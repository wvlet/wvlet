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
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

/**
  * Opt-in benchmark of the columnar engine against DuckDB on the TPC-H queries in spec/tpch, both
  * reading the same Parquet files. No assertion on timings; the numbers are logged and written to
  * `target/engine-bench/tpch-sf<sf>.tsv` for before/after comparison on one machine.
  *
  * {{{
  *   WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineTPCHBench"
  * }}}
  *
  * Environment variables (sbt forks the test JVM, so -D properties would not reach it):
  *   - `WVLET_ENGINE_BENCH_SF`: scale factor (required; the probe is ignored without it, so it
  *     never runs in CI). The data is generated once with DuckDB's tpch extension into
  *     `target/tpch-sf<sf>/<table>.parquet` and reused afterwards
  *   - `WVLET_ENGINE_BENCH_RUNS`: timed executions per query after one warm-up (default 3)
  *   - `WVLET_ENGINE_BENCH_QUERIES`: comma-separated query names to run, e.g. `q1,q6` (default all)
  *   - `WVLET_ENGINE_BENCH_DUCKDB_THREADS`: DuckDB worker threads (default: DuckDB's own default,
  *     all cores). Set 1 for a single-threaded comparison
  */
class EngineTPCHBench extends UniTest:
  import EngineTPCHBench.*

  test("time spec/tpch queries on the engine and DuckDB") {
    sys.env.get("WVLET_ENGINE_BENCH_SF").map(_.toDouble) match
      case None =>
        ignore("Set WVLET_ENGINE_BENCH_SF=<scale factor> (e.g. 1) to run the engine benchmark")
      case Some(sf) =>
        val runs = sys.env.get("WVLET_ENGINE_BENCH_RUNS").map(_.toInt).getOrElse(3)
        require(runs >= 1, s"WVLET_ENGINE_BENCH_RUNS must be at least 1, but got ${runs}")
        val queries = sys
          .env
          .get("WVLET_ENGINE_BENCH_QUERIES")
          .map(_.split(",").map(_.trim.stripSuffix(".wv")).filter(_.nonEmpty).toSet)
        val threads = sys.env.get("WVLET_ENGINE_BENCH_DUCKDB_THREADS").map(_.toInt)
        runBench(sf, runs, queries, threads)
  }

  private def runBench(
      sf: Double,
      runs: Int,
      queries: Option[Set[String]],
      duckdbThreads: Option[Int]
  ): Unit =
    val specPath = "spec/tpch"
    val dataDir  = prepareData(sf)

    Class.forName("org.duckdb.DuckDBDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:duckdb:")
    try
      duckdbThreads.foreach(t => execute(conn, s"set threads = ${t}"))
      tables.foreach { t =>
        execute(conn, s"create view ${t} as select * from '${dataDir}/${t}.parquet'")
      }
      val engine = WvletEngine(baseDir = specPath)
      tables.foreach(t => engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet"))

      val compiler = Compiler(
        CompilerOptions(sourceFolders = List(specPath), workEnv = WorkEnv(specPath))
      )
      val units = compiler
        .localCompilationUnits
        .filter(_.sourceFile.fileName.matches("q\\d+\\.wv"))
        .filter(u => queries.forall(_.contains(u.sourceFile.fileName.stripSuffix(".wv"))))
        .sortBy(_.sourceFile.fileName.stripPrefix("q").stripSuffix(".wv").toInt)

      val results = units.map { unit =>
        val name          = unit.sourceFile.fileName.stripSuffix(".wv")
        val compileResult = compiler.compileSingleUnit(unit)
        given Context     = compileResult.context
        queryOf(unit) match
          case None =>
            QueryTiming(name, Nil, Nil, -1, -1, Some("no query"))
          case Some(q) =>
            val sql = GenSQL.generateSQLFromRelation(q, addHeader = false).sql
            def duckdbOnce(): (Double, Long) = timed(countRows(conn, sql))
            def engineOnce(): (Double, Long) = timed(engine.execute(q).numRows.toLong)
            try
              val (_, duckRows)   = duckdbOnce() // warm-up
              val duck            = (1 to runs).map(_ => duckdbOnce()._1)
              val (_, engineRows) = engineOnce()
              val eng             = (1 to runs).map(_ => engineOnce()._1)
              val r = QueryTiming(name, duck.sorted, eng.sorted, duckRows, engineRows, None)
              info(r.logLine)
              r
            catch
              case e: Throwable =>
                warn(s"${name}: ${e}", e)
                QueryTiming(name, Nil, Nil, -1, -1, Some(e.toString))
      }
      report(sf, runs, duckdbThreads, results)
    finally
      conn.close()
    end try

  end runBench

  private def report(
      sf: Double,
      runs: Int,
      duckdbThreads: Option[Int],
      results: Seq[QueryTiming]
  ): Unit =
    val ok        = results.filter(_.error.isEmpty)
    val duckTotal = ok.map(_.duckdbMedian).sum
    val engTotal  = ok.map(_.engineMedian).sum
    val header    =
      s"TPC-H sf=${sf}, ${runs} runs, DuckDB threads=${duckdbThreads
          .map(_.toString)
          .getOrElse("default")}"
    val table = Seq.newBuilder[String]
    table += "| query | DuckDB median (ms) | engine median (ms) | engine / DuckDB | rows |"
    table += "|---|---:|---:|---:|---|"
    results.foreach { r =>
      r.error match
        case Some(e) =>
          table += s"| ${r.name} | | | | error: ${e.take(80)} |"
        case None =>
          val rows =
            if r.rowsMatch then
              s"${r.engineRows}"
            else
              s"MISMATCH duckdb=${r.duckdbRows} engine=${r.engineRows}"
          table +=
            f"| ${r.name} | ${r.duckdbMedian}%.1f | ${r.engineMedian}%.1f | ${r
                .ratio}%.1fx | ${rows} |"
    }
    table += f"| total | ${duckTotal}%.1f | ${engTotal}%.1f | ${engTotal / duckTotal}%.1fx | |"
    info(s"${header}\n${table.result().mkString("\n")}")

    val outDir = File("target/engine-bench")
    outDir.mkdirs()
    val tsv   = File(outDir, s"tpch-sf${sfLabel(sf)}.tsv")
    val lines =
      "query\tduckdb_median_ms\tengine_median_ms\tratio\tduckdb_rows\tengine_rows\terror" +:
        results.map { r =>
          f"${r.name}\t${r.duckdbMedian}%.1f\t${r.engineMedian}%.1f\t${r.ratio}%.2f\t${r
              .duckdbRows}\t${r.engineRows}\t${r.error.getOrElse("")}"
        }
    Files.writeString(tsv.toPath, lines.mkString("", "\n", "\n"))
    info(s"Wrote ${tsv.getPath}")

  end report

end EngineTPCHBench

object EngineTPCHBench:
  val tables: Seq[String] = Seq(
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier"
  )

  case class QueryTiming(
      name: String,
      duckdbMs: Seq[Double],
      engineMs: Seq[Double],
      duckdbRows: Long,
      engineRows: Long,
      error: Option[String]
  ):
    def duckdbMedian: Double = median(duckdbMs)
    def engineMedian: Double = median(engineMs)
    def ratio: Double        = engineMedian / math.max(duckdbMedian, 0.001)
    def rowsMatch: Boolean   = duckdbRows == engineRows
    def logLine: String      =
      f"${name}%-4s DuckDB median ${duckdbMedian}%9.1f ms  engine median ${engineMedian}%9.1f ms  (${ratio}%6.1fx)  engine min ${engineMs
          .headOption
          .getOrElse(0.0)}%9.1f ms${
          if rowsMatch then
            ""
          else
            s"  ROW COUNT MISMATCH duckdb=${duckdbRows} engine=${engineRows}"
        }"

  /** The scale factor as written in paths: 1 rather than 1.0, 0.1 as is */
  def sfLabel(sf: Double): String =
    if sf == sf.floor then
      sf.toLong.toString
    else
      sf.toString

  def median(xs: Seq[Double]): Double =
    if xs.isEmpty then
      0.0
    else if xs.size % 2 == 1 then
      xs(xs.size / 2)
    else
      (xs(xs.size / 2 - 1) + xs(xs.size / 2)) / 2

  def timed(body: => Long): (Double, Long) =
    val start = System.nanoTime()
    val rows  = body
    ((System.nanoTime() - start) / 1000000.0, rows)

  def execute(conn: Connection, sql: String): Unit =
    val st = conn.createStatement()
    try st.execute(sql)
    finally st.close()

  /** Run a query and consume every row, so that DuckDB pays for producing the full result */
  def countRows(conn: Connection, sql: String): Long =
    val st = conn.createStatement()
    try
      val rs   = st.executeQuery(sql)
      val cols = rs.getMetaData.getColumnCount
      var n    = 0L
      while rs.next() do
        var i = 1
        while i <= cols do
          rs.getObject(i)
          i += 1
        n += 1
      n
    finally
      st.close()

  /** The first query of a compiled unit */
  def queryOf(unit: CompilationUnit): Option[Relation] =
    def find(e: ExecutionPlan): Option[Relation] =
      e match
        case ExecuteQuery(q: Relation) =>
          Some(q)
        case ExecuteTasks(ts) =>
          ts.iterator.flatMap(find).nextOption()
        case ExecuteSave(_, q) =>
          find(q)
        case _ =>
          None
    find(unit.executionPlan)

  /** Generate TPC-H Parquet files with DuckDB's tpch extension, once per scale factor */
  def prepareData(sf: Double): String =
    val dir = Paths.get(s"target/tpch-sf${sfLabel(sf)}").toAbsolutePath
    if tables.forall(t => Files.exists(dir.resolve(s"${t}.parquet"))) then
      dir.toString
    else
      Files.createDirectories(dir)
      Class.forName("org.duckdb.DuckDBDriver")
      val conn = DriverManager.getConnection("jdbc:duckdb:")
      try
        execute(conn, "install tpch")
        execute(conn, "load tpch")
        execute(conn, s"call dbgen(sf = ${sf})")
        tables.foreach { t =>
          val tmp = dir.resolve(s"${t}.parquet.tmp")
          execute(conn, s"copy ${t} to '${tmp}' (format parquet)")
          Files.move(tmp, dir.resolve(s"${t}.parquet"), StandardCopyOption.REPLACE_EXISTING)
        }
      finally
        conn.close()
      dir.toString

end EngineTPCHBench

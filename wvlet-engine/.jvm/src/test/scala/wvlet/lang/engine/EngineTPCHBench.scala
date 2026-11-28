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
import wvlet.lang.engine.catalog.BenchmarkKeys
import wvlet.lang.engine.exec.MemoryContext
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.Batch
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport
import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
  * Opt-in benchmark of the columnar engine against DuckDB on the TPC-H queries in spec/tpch, both
  * reading the same Parquet files. No assertion on timings; the numbers are logged and written to
  * `target/engine-bench/tpch-sf<sf>.tsv` for before/after comparison on one machine. Engine runs
  * also report the JVM's GC time, the bytes all threads allocated during the run, and the peak
  * memory the query reserved in its `MemoryContext` (`engine_peak_mb`) and the bytes it wrote to
  * spill files (`engine_spilled_mb`; medians).
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
  *     all cores, the engine's default too). Set 1 for a single-threaded comparison
  *   - `WVLET_ENGINE_BENCH_DUCKDB_DEFAULTS`: set 1 to run DuckDB with all its default settings.
  *     Otherwise DuckDB caches Parquet footers across queries (`parquet_metadata_cache`), as the
  *     engine does, and doesn't keep the insertion order of unordered results
  *     (`preserve_insertion_order = false`), which the engine doesn't either
  *   - `WVLET_ENGINE_BENCH_KEYS`: the unique keys the engine is told about: `declared` (default:
  *     the specifications' primary keys, plus TPC-DS `c_customer_id`, see [[BenchmarkKeys]]), `pk`
  *     (primary keys only), or `none`. DuckDB reads Parquet views and knows no keys
  *   - `WVLET_ENGINE_BENCH_COLD`: set 1 to time each run without what earlier runs cached about the
  *     files: before every engine run, the Parquet footers and file statistics (distinct counts,
  *     value samples) are dropped and the tables registered again, and DuckDB's footer cache is
  *     off. The OS page cache stays warm for both
  *   - `WVLET_ENGINE_TEST_HEAP`: the test JVM's `-Xmx`, e.g. `32g` (default: the JVM's, a quarter
  *     of RAM). The TSV's `engine_live_heap_mb` is the largest heap used after a GC during the
  *     timed engine runs, an approximate live set (see [[LiveHeapProbe]])
  *   - `WVLET_ENGINE_BENCH_TIMEOUT`: seconds to wait for all engine runs of one query, warm-up
  *     included (default 300); the remaining queries are skipped after a timeout
  *   - `WVLET_ENGINE_BENCH_ENGINE_ONLY`: set 1 to skip the DuckDB runs (DuckDB columns are 0), so
  *     that the process's memory is the engine's alone, e.g. to measure its peak RSS at a spill
  *     threshold (`JAVA_TOOL_OPTIONS=-Dwvlet.engine.spillThreshold=<bytes>`)
  *   - `WVLET_ENGINE_BENCH_DUCKDB_PROFILE`: set 1 to write DuckDB's JSON profile (per-operator
  *     timings) of one extra run of each query to `target/engine-bench/<label>-<query>.duckdb.json`
  *   - `WVLET_ENGINE_BENCH_EXPLAIN`: set 1 to write each engine plan to
  *     `target/engine-bench/<suite>-<query>.plan`; with `WVLET_ENGINE_BENCH_AB`, also the plan with
  *     the A/B optimizations disabled to `<suite>-<query>.off.plan`, to diff a suite's plans
  *   - `WVLET_ENGINE_BENCH_AB`: comma-separated optimization names (see `Optimization`). Instead of
  *     comparing with DuckDB, each query runs on the engine with every optimization ("on") and with
  *     these disabled ("off"), alternating on/off and off/on run by run in the same JVM, so that
  *     load drift on a noisy machine hits both alike. Logs the on / off ratio of the fastest run of
  *     each per query and for the suite, and writes `target/engine-bench/<suite>-sf<sf>-ab.tsv`.
  *     Both suites at SF1 take about a minute; trust the suite totals first, since single short
  *     queries still move by 10-20% between runs. For example
  *     `WVLET_ENGINE_BENCH_SF=1 WVLET_ENGINE_BENCH_RUNS=5 WVLET_ENGINE_BENCH_AB=join_direct_index`
  *
  * [[EngineTPCDSBench]] runs spec/tpcds the same way.
  */
class EngineTPCHBench extends UniTest:
  import EngineTPCHBench.*

  test("time spec/tpch queries on the engine and DuckDB") {
    BenchOptions.fromEnv() match
      case None =>
        ignore("Set WVLET_ENGINE_BENCH_SF=<scale factor> (e.g. 1) to run the engine benchmark")
      case Some(opts) =>
        val dataDir = prepareData(opts.sf)
        runBench("tpch", "spec/tpch", dataDir, tables, opts)
  }

end EngineTPCHBench

object EngineTPCHBench extends LogSupport:
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

  /**
    * Benchmark settings read from environment variables (sbt forks the test JVM, so -D properties
    * would not reach it). None when `WVLET_ENGINE_BENCH_SF` is unset, so that benchmarks never run
    * in CI
    */
  case class BenchOptions(
      sf: Double,
      runs: Int,
      queries: Option[Set[String]],
      duckdbThreads: Option[Int],
      timeoutSeconds: Int,
      explain: Boolean,
      duckdbProfile: Boolean = false,
      abOff: Option[OptimizerOptions] = None,
      duckdbDefaults: Boolean = false,
      keys: BenchKeys = BenchKeys.Declared,
      cold: Boolean = false,
      engineOnly: Boolean = false
  )

  /** Which unique keys the engine is told about (`WVLET_ENGINE_BENCH_KEYS`) */
  enum BenchKeys:
    case Declared,
      PrimaryOnly,
      NoKeys

  object BenchKeys:
    def parse(s: String): BenchKeys =
      s.trim.toLowerCase match
        case "" | "declared" =>
          Declared
        case "pk" =>
          PrimaryOnly
        case "none" =>
          NoKeys
        case other =>
          throw IllegalArgumentException(
            s"WVLET_ENGINE_BENCH_KEYS must be declared, pk, or none, but got ${other}"
          )

  private def flag(name: String): Boolean = sys.env.get(name).exists(v => v == "1" || v == "true")

  object BenchOptions:
    def fromEnv(): Option[BenchOptions] = sys
      .env
      .get("WVLET_ENGINE_BENCH_SF")
      .map { sf =>
        val runs = sys.env.get("WVLET_ENGINE_BENCH_RUNS").map(_.toInt).getOrElse(3)
        require(runs >= 1, s"WVLET_ENGINE_BENCH_RUNS must be at least 1, but got ${runs}")
        BenchOptions(
          sf = sf.toDouble,
          runs = runs,
          queries = sys
            .env
            .get("WVLET_ENGINE_BENCH_QUERIES")
            .map(_.split(",").map(_.trim.stripSuffix(".wv")).filter(_.nonEmpty).toSet),
          duckdbThreads = sys.env.get("WVLET_ENGINE_BENCH_DUCKDB_THREADS").map(_.toInt),
          timeoutSeconds = sys.env.get("WVLET_ENGINE_BENCH_TIMEOUT").map(_.toInt).getOrElse(300),
          explain = flag("WVLET_ENGINE_BENCH_EXPLAIN"),
          duckdbProfile = flag("WVLET_ENGINE_BENCH_DUCKDB_PROFILE"),
          duckdbDefaults = flag("WVLET_ENGINE_BENCH_DUCKDB_DEFAULTS"),
          keys = sys
            .env
            .get("WVLET_ENGINE_BENCH_KEYS")
            .map(BenchKeys.parse)
            .getOrElse(BenchKeys.Declared),
          cold = flag("WVLET_ENGINE_BENCH_COLD"),
          engineOnly = flag("WVLET_ENGINE_BENCH_ENGINE_ONLY"),
          abOff = sys
            .env
            .get("WVLET_ENGINE_BENCH_AB")
            .filter(_.trim.nonEmpty)
            .map(names =>
              OptimizerOptions
                .fromSystem()
                .withDisabled(OptimizerOptions.parse(names).disabled.toSeq*)
            )
        )
      }

  end BenchOptions

  /** The number of a query file name, e.g. 24 for q24.wv */
  def queryNumber(fileName: String): Int = fileName.filter(_.isDigit).toInt

  /**
    * Time the q<N>.wv queries in specPath on DuckDB and the engine, both reading
    * `<dataDir>/<table>.parquet` (or the files of a `<dataDir>/<table>/` folder), and write
    * `target/engine-bench/<label>-sf<sf>.tsv`
    */
  def runBench(
      label: String,
      specPath: String,
      dataDir: String,
      tables: Seq[String],
      opts: BenchOptions,
      accelerator: Option[runtime.PlanAccelerator] = None,
      offAccelerator: Option[runtime.PlanAccelerator] = None
  ): Seq[QueryTiming] =
    Class.forName("org.duckdb.DuckDBDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:duckdb:")
    // Engine runs go through a worker thread so that a slow query can time out
    val worker = Executors.newSingleThreadExecutor()
    try
      opts.duckdbThreads.foreach(t => execute(conn, s"set threads = ${t}"))
      if !opts.duckdbDefaults then
        // The engine caches footers across queries (parquet_footer_cache) and keeps no
        // insertion order, so DuckDB is set up the same way
        execute(conn, s"set parquet_metadata_cache = ${!opts.cold}")
        execute(conn, "set preserve_insertion_order = false")
      // A `<table>/` folder of Parquet files, when there is one, is read instead of the single
      // file, to time multi-file scans
      def folder(t: String): Option[String] = Option(s"${dataDir}/${t}").filter(d =>
        Files.isDirectory(Paths.get(d))
      )
      tables.foreach { t =>
        // Nested files too, with `key=value` folders as columns, as the engine reads a folder
        val from = folder(t)
          .map(d => s"read_parquet('${d}/**/*.parquet', hive_partitioning = true)")
          .getOrElse(s"'${dataDir}/${t}.parquet'")
        execute(conn, s"create view ${t} as select * from ${from}")
      }
      def newEngine(): WvletEngine =
        val e = WvletEngine(baseDir = specPath, accelerator = accelerator)
        tables.foreach { t =>
          val path = folder(t).getOrElse(s"${dataDir}/${t}.parquet")
          val keys = BenchmarkKeys.of(label, t)
          opts.keys match
            case BenchKeys.Declared =>
              e.catalog.registerParquet(t, path, keys)
            case BenchKeys.PrimaryOnly =>
              e.catalog.registerParquet(t, path, keys.take(1))
            case BenchKeys.NoKeys if folder(t).isEmpty =>
              // Not even the keys of a sidecar file next to the data
              e.catalog.register(UnkeyedParquetSource(t, path))
            case BenchKeys.NoKeys =>
              throw IllegalArgumentException(
                s"WVLET_ENGINE_BENCH_KEYS=none reads single files only, but ${t} is a folder"
              )
        }
        e
      val engine = newEngine()
      // In cold mode, each run plans over freshly registered tables, with no cached footers or
      // file statistics left from earlier runs
      def engineForRun(): WvletEngine =
        if opts.cold then
          catalog.ParquetFooterCache.clear()
          catalog.ParquetSource.clearCachedStatistics()
          newEngine()
        else
          engine
      logEnvironment(conn, label, opts)
      // The engine with the A/B optimizations disabled, over the same tables
      // offAccelerator, when given, replaces the accelerator there (to A/B accelerator settings)
      val offEngine = opts
        .abOff
        .map(off => WvletEngine(engine.catalog, specPath, off, offAccelerator.orElse(accelerator)))
      val abResults = Seq.newBuilder[ABTiming]
      val abFailed  = Seq.newBuilder[String]

      val compiler = Compiler(
        CompilerOptions(sourceFolders = List(specPath), workEnv = WorkEnv(specPath))
      )
      val units = compiler
        .localCompilationUnits
        .filter(_.sourceFile.fileName.matches("q\\d+\\.wv"))
        .filter(u =>
          opts.queries.forall(qs => qs.contains(u.sourceFile.fileName.stripSuffix(".wv")))
        )
        .sortBy(u => queryNumber(u.sourceFile.fileName))

      val outDir = File("target/engine-bench")
      outDir.mkdirs()
      var timedOut = false
      val results  = units.map { unit =>
        val name          = unit.sourceFile.fileName.stripSuffix(".wv")
        val compileResult = compiler.compileSingleUnit(unit)
        given Context     = compileResult.context
        queryOf(unit) match
          case None =>
            QueryTiming(name, Nil, Nil, -1, -1, Some("no query"))
          case Some(_) if timedOut =>
            // A timed-out engine run may still occupy the worker thread
            QueryTiming(name, Nil, Nil, -1, -1, Some("skipped after a timeout"))
          case Some(q) if offEngine.isDefined =>
            if opts.explain then
              Files.writeString(File(outDir, s"${label}-${name}.plan").toPath, engine.explain(q))
              Files.writeString(
                File(outDir, s"${label}-${name}.off.plan").toPath,
                offEngine.get.explain(q)
              )
            val task = worker.submit(() => abTiming(name, q, engine, offEngine.get, opts.runs))
            try
              val r = task.get(opts.timeoutSeconds.toLong, TimeUnit.SECONDS)
              info(r.logLine)
              abResults += r
            catch
              case e: ExecutionException =>
                warn(s"${name}: ${e.getCause}", e.getCause)
                abFailed += name
              case e: TimeoutException =>
                task.cancel(true)
                timedOut = true
                warn(s"${name}: timed out after ${opts.timeoutSeconds}s")
                abFailed += name
            QueryTiming(name, Nil, Nil, -1, -1, Some("A/B run"))
          case Some(q) =>
            val sql = GenSQL.generateSQLFromRelation(q, addHeader = false).sql
            // Without DuckDB runs, 0 ms and -1 rows (taken as matching the engine's)
            def duckdbOnce(): (Double, Long) =
              if opts.engineOnly then
                (0.0, -1L)
              else
                timed(countRows(conn, sql))
            // Wall time and busy cores of one DuckDB run (DuckDB runs in this process)
            // (0 ms and 0 cores without DuckDB runs)
            def duckdbTimed(): (Double, Double) =
              if opts.engineOnly then
                (0.0, 0.0)
              else
                val cpu0    = processCpuNanos()
                val (ms, _) = duckdbOnce()
                val cpuMs   = (processCpuNanos() - cpu0) / 1000000.0
                (ms, cpuMs / math.max(ms, 0.001))
            def engineOnce(e: WvletEngine): PlannedRun = plannedRun(e, q)
            // Wall time, busy cores (process CPU time / wall time), GC time, allocated MB,
            // planning time, live heap, and peak tracked memory of one engine run
            def engineTimed(): EngineRun =
              // A cold run's fresh engine is set up before the measurements start
              val e      = engineForRun()
              val cpu0   = processCpuNanos()
              val gc0    = gcMillis()
              val alloc0 = allocatedBytes()
              val heap   = LiveHeapProbe.start()
              val run    =
                try engineOnce(e)
                finally heap.stop()
              val cpuMs = (processCpuNanos() - cpu0) / 1000000.0
              EngineRun(
                run.ms,
                cpuMs / math.max(run.ms, 0.001),
                (gcMillis() - gc0).toDouble,
                (allocatedBytes() - alloc0) / 1048576.0,
                run.planMs,
                heap.maxMb,
                run.peakMb,
                run.spilledMb
              )
            try
              if opts.explain then
                Files.writeString(File(outDir, s"${label}-${name}.plan").toPath, engine.explain(q))
              // Engine runs share one time budget, warm-up included; DuckDB's runs don't count
              var budgetNanos                = opts.timeoutSeconds * 1000000000L
              def onWorker[A](body: => A): A =
                val start = System.nanoTime()
                val task  = worker.submit(() => body)
                try
                  task.get(math.max(0L, budgetNanos), TimeUnit.NANOSECONDS)
                catch
                  case e: ExecutionException =>
                    throw e.getCause
                  case e: TimeoutException =>
                    task.cancel(true)
                    timedOut = true
                    throw e
                finally
                  budgetNanos -= System.nanoTime() - start
              // Warm-ups: the engine's first run also computes the file statistics that later
              // runs (and queries) reuse, so its time is reported as engine_first_ms
              val (duckFirst, duckRows0)    = duckdbOnce()
              val first                     = onWorker(engineOnce(engineForRun()))
              val (engineFirst, engineRows) = (first.ms, first.rows)
              val duckRows                  =
                if opts.engineOnly then
                  engineRows
                else
                  duckRows0
              // DuckDB and engine runs alternate, each going first every other run, so that a
              // load change on a shared machine hits both alike
              val duckRuns = Seq.newBuilder[(Double, Double)]
              val engRuns  = Seq.newBuilder[EngineRun]
              (1 to opts.runs).foreach { r =>
                if r % 2 == 1 then
                  duckRuns += duckdbTimed()
                  engRuns += onWorker(engineTimed())
                else
                  engRuns += onWorker(engineTimed())
                  duckRuns += duckdbTimed()
              }
              val duck = duckRuns.result()
              val eng  = engRuns.result()
              if opts.duckdbProfile && !opts.engineOnly then
                val out = File(outDir, s"${label}-${name}.duckdb.json").getAbsolutePath
                execute(conn, "set enable_profiling = 'json'")
                execute(conn, s"set profiling_output = '${out}'")
                try countRows(conn, sql)
                finally execute(conn, "pragma disable_profiling")
              val r = QueryTiming(
                name,
                duck.map(_._1).sorted,
                eng.map(_.ms).sorted,
                duckRows,
                engineRows,
                None,
                eng.map(_.cores).sorted,
                duck.map(_._2).sorted,
                eng.map(_.gcMs).sorted,
                eng.map(_.allocMb).sorted,
                eng.map(_.planMs).sorted,
                duckFirst,
                engineFirst,
                eng.map(_.liveHeapMb),
                eng.map(_.peakMb).sorted,
                eng.map(_.spilledMb).sorted
              )
              info(r.logLine)
              r
            catch
              case e: TimeoutException =>
                warn(s"${name}: timed out after ${opts.timeoutSeconds}s")
                QueryTiming(name, Nil, Nil, -1, -1, Some(s"timeout ${opts.timeoutSeconds}s"))
              case e: Throwable =>
                warn(s"${name}: ${e}", e)
                QueryTiming(name, Nil, Nil, -1, -1, Some(e.toString))
            end try
        end match
      }
      if offEngine.isDefined then
        reportAB(label, opts, abResults.result(), abFailed.result())
      else
        report(label, opts, results)
      results
    finally
      worker.shutdownNow()
      conn.close()
    end try

  end runBench

  /**
    * Times one query on `on` and `off` after a warm-up of each, alternating which runs first, and
    * checks that both return the same number of rows
    */
  private def abTiming(name: String, q: Relation, on: WvletEngine, off: WvletEngine, runs: Int)(
      using Context
  ): ABTiming =
    // Wall and process CPU milliseconds, and rows, of one run
    def once(e: WvletEngine): (Double, Double, Long) =
      val cpu0       = processCpuNanos()
      val (ms, rows) = timed(e.execute(q).numRows.toLong)
      (ms, (processCpuNanos() - cpu0) / 1000000.0, rows)
    val onRows  = once(on)._3
    val offRows = once(off)._3
    val onRuns  = Seq.newBuilder[(Double, Double)]
    val offRuns = Seq.newBuilder[(Double, Double)]
    (1 to runs).foreach { r =>
      def runOn(): Unit =
        val (ms, cpu, _) = once(on)
        onRuns += ((ms, cpu))
      def runOff(): Unit =
        val (ms, cpu, _) = once(off)
        offRuns += ((ms, cpu))
      if r % 2 == 1 then
        runOn()
        runOff()
      else
        runOff()
        runOn()
    }
    val onR  = onRuns.result()
    val offR = offRuns.result()
    ABTiming(name, onR.map(_._1), offR.map(_._1), onR.map(_._2), offR.map(_._2), onRows == offRows)

  end abTiming

  /** Logs the A/B timings and writes `target/engine-bench/<label>-sf<sf>-ab.tsv` */
  private def reportAB(
      label: String,
      opts: BenchOptions,
      results: Seq[ABTiming],
      failed: Seq[String]
  ): Unit =
    // Only the A/B optimizations: ones disabled system-wide are off in both engines
    val off                              = sys.env.getOrElse("WVLET_ENGINE_BENCH_AB", "")
    val onMs                             = results.map(_.onMs).sum
    val offMs                            = results.map(_.offMs).sum
    val onCpu                            = results.map(_.onCpuMs).sum
    val offCpu                           = results.map(_.offCpuMs).sum
    def geomean(xs: Seq[Double]): Double =
      if xs.isEmpty then
        1.0
      else
        math.exp(xs.map(math.log).sum / xs.size)
    val moved = results.sortBy(_.ratio)
    val lines = Seq.newBuilder[String]
    lines +=
      f"${label} sf=${opts.sf}, ${opts.runs} runs, off=${off}: on / off wall ${onMs /
          offMs}%.3f (${onMs}%.0f / ${offMs}%.0f ms), CPU ${onCpu /
          offCpu}%.3f, wall geomean ${geomean(results.map(_.ratio))}%.3f over ${results
          .size} queries"
    lines +=
      s"  most improved: ${moved.take(5).map(r => f"${r.name} ${r.ratio}%.2f").mkString(", ")}"
    lines +=
      s"  most regressed: ${moved
          .reverse
          .take(5)
          .map(r => f"${r.name} ${r.ratio}%.2f")
          .mkString(", ")}"
    if failed.nonEmpty then
      lines += s"  FAILED or timed out (not in the totals): ${failed.mkString(", ")}"
    val mismatched = results.filterNot(_.rowsMatch).map(_.name)
    if mismatched.nonEmpty then
      lines += s"  ROW COUNT MISMATCH: ${mismatched.mkString(", ")}"
    info(lines.result().mkString("\n"))
    val tsv  = File("target/engine-bench", s"${label}-sf${sfLabel(opts.sf)}-ab.tsv")
    val rows =
      "query\ton_min_ms\toff_min_ms\tratio\ton_cpu_ms\toff_cpu_ms\tcpu_ratio\trows_match" +:
        results.map { r =>
          f"${r.name}\t${r.onMs}%.1f\t${r.offMs}%.1f\t${r.ratio}%.3f\t${r.onCpuMs}%.1f\t${r
              .offCpuMs}%.1f\t${r.cpuRatio}%.3f\t${r.rowsMatch}"
        }
    Files.writeString(tsv.toPath, rows.mkString("", "\n", "\n"))
    info(s"Wrote ${tsv.getPath}")

  end reportAB

  /**
    * One query's engine runs with every optimization (on) and with the A/B optimizations disabled
    * (off), as wall and process CPU milliseconds per run
    */
  case class ABTiming(
      name: String,
      onMsPerRun: Seq[Double],
      offMsPerRun: Seq[Double],
      onCpuMsPerRun: Seq[Double],
      offCpuMsPerRun: Seq[Double],
      rowsMatch: Boolean
  ):
    // The fastest run of each: load spikes on a shared machine only ever slow a run down
    def onMs: Double     = onMsPerRun.min
    def offMs: Double    = offMsPerRun.min
    def onCpuMs: Double  = onCpuMsPerRun.min
    def offCpuMs: Double = offCpuMsPerRun.min
    def ratio: Double    = onMs / math.max(offMs, 0.001)
    def cpuRatio: Double = onCpuMs / math.max(offCpuMs, 0.001)
    def logLine: String  =
      f"${name}%-4s on ${onMs}%7.1f ms  off ${offMs}%7.1f ms  (${ratio}%.2f)  CPU on ${onCpuMs}%7.1f ms  off ${offCpuMs}%7.1f ms  (${cpuRatio}%.2f)${
          if rowsMatch then
            ""
          else
            "  ROW COUNT MISMATCH"
        }"

  private def report(label: String, opts: BenchOptions, results: Seq[QueryTiming]): Unit =
    val ok        = results.filter(_.error.isEmpty)
    val duckTotal = ok.map(_.duckdbMedian).sum
    val engTotal  = ok.map(_.engineMedian).sum
    val header    =
      s"${label} sf=${opts.sf}, ${opts.runs} runs, DuckDB threads=${opts
          .duckdbThreads
          .map(_.toString)
          .getOrElse("default")}"
    val table = Seq.newBuilder[String]
    table +=
      "| query | DuckDB median (ms) | engine median (ms) | engine / DuckDB | DuckDB busy cores | engine busy cores | engine / DuckDB CPU | engine GC (ms) | engine alloc (MB) | engine plan (ms) | engine peak (MB) | engine spilled (MB) | rows |"
    table += "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    results.foreach { r =>
      r.error match
        case Some(e) =>
          table += s"| ${r.name} | | | | | | | | | | | | error: ${e.take(80)} |"
        case None =>
          val rows =
            if r.rowsMatch then
              s"${r.engineRows}"
            else
              s"MISMATCH duckdb=${r.duckdbRows} engine=${r.engineRows}"
          table +=
            f"| ${r.name} | ${r.duckdbMedian}%.1f | ${r.engineMedian}%.1f | ${r.ratio}%.1fx | ${r
                .duckdbCores}%.1f | ${r.engineCores}%.1f | ${r.cpuRatio}%.1fx | ${r
                .engineGcMs}%.0f | ${r.engineAllocMb}%.0f | ${r.enginePlanMs}%.1f | ${r
                .enginePeakMb}%.0f | ${r.engineSpilledMb}%.0f | ${rows} |"
    }
    val duckCpu = ok.map(r => r.duckdbMedian * r.duckdbCores).sum
    val engCpu  = ok.map(r => r.engineMedian * r.engineCores).sum
    table +=
      f"| total | ${duckTotal}%.1f | ${engTotal}%.1f | ${engTotal / duckTotal}%.1fx | ${duckCpu /
          duckTotal}%.1f | ${engCpu / engTotal}%.1f | ${engCpu / duckCpu}%.1fx | ${ok
          .map(_.engineGcMs)
          .sum}%.0f | ${ok.map(_.engineAllocMb).sum}%.0f | ${ok.map(_.enginePlanMs).sum}%.1f | ${ok
          .map(_.enginePeakMb)
          .maxOption
          .getOrElse(0.0)}%.0f | ${ok.map(_.engineSpilledMb).sum}%.0f | |"
    info(s"${header}\n${table.result().mkString("\n")}")

    val tsv   = File("target/engine-bench", s"${label}-sf${sfLabel(opts.sf)}.tsv")
    val lines =
      "query\tduckdb_median_ms\tengine_median_ms\tratio\tduckdb_rows\tengine_rows\terror\tengine_cores\tduckdb_cores\tcpu_ratio\tengine_gc_ms\tengine_alloc_mb\tengine_plan_ms\tduckdb_first_ms\tengine_first_ms\tengine_live_heap_mb\tengine_peak_mb\tengine_spilled_mb" +:
        results.map { r =>
          f"${r.name}\t${r.duckdbMedian}%.1f\t${r.engineMedian}%.1f\t${r.ratio}%.2f\t${r
              .duckdbRows}\t${r.engineRows}\t${r.error.getOrElse("")}\t${r.engineCores}%.2f\t${r
              .duckdbCores}%.2f\t${r.cpuRatio}%.2f\t${r.engineGcMs}%.1f\t${r.engineAllocMb}%.1f\t${r
              .enginePlanMs}%.1f\t${r.duckdbFirstMs}%.1f\t${r.engineFirstMs}%.1f\t${r
              .engineLiveHeapMb}%.1f\t${r.enginePeakMb}%.1f\t${r.engineSpilledMb}%.1f"
        }
    Files.writeString(tsv.toPath, lines.mkString("", "\n", "\n"))
    info(s"Wrote ${tsv.getPath}")

  end report

  case class QueryTiming(
      name: String,
      duckdbMs: Seq[Double],
      engineMs: Seq[Double],
      duckdbRows: Long,
      engineRows: Long,
      error: Option[String],
      engineCoresPerRun: Seq[Double] = Nil,
      duckdbCoresPerRun: Seq[Double] = Nil,
      engineGcMsPerRun: Seq[Double] = Nil,
      engineAllocMbPerRun: Seq[Double] = Nil,
      enginePlanMsPerRun: Seq[Double] = Nil,
      duckdbFirstMs: Double = -1,
      engineFirstMs: Double = -1,
      engineLiveHeapMbPerRun: Seq[Double] = Nil,
      enginePeakMbPerRun: Seq[Double] = Nil,
      engineSpilledMbPerRun: Seq[Double] = Nil
  ):
    /** The median peak memory the engine runs reserved (MemoryContext.peakBytes) */
    def enginePeakMb: Double = median(enginePeakMbPerRun)

    /** The median MB the engine runs wrote to spill files (MemoryContext.spilledBytes) */
    def engineSpilledMb: Double = median(engineSpilledMbPerRun)

    /**
      * The largest live heap (heap used after a GC, see [[LiveHeapProbe]]) of the engine runs in
      * MB, or -1 when no GC ended during them
      */
    def engineLiveHeapMb: Double = engineLiveHeapMbPerRun.maxOption.getOrElse(-1.0)

    /**
      * The median planning time of the engine runs: model expansion and building the physical
      * operators, including plan-time subqueries and statistics reads, before any operator runs
      */
    def enginePlanMs: Double = median(enginePlanMsPerRun)

    /** The median GC time (collector time summed over the JVM's collectors) of the engine runs */
    def engineGcMs: Double = median(engineGcMsPerRun)

    /** The median MB allocated by all threads during an engine run */
    def engineAllocMb: Double = median(engineAllocMbPerRun)

    /** The median busy cores of the engine runs: process CPU time / wall time */
    def engineCores: Double = median(engineCoresPerRun)

    /** The median busy cores of the DuckDB runs */
    def duckdbCores: Double = median(duckdbCoresPerRun)

    /**
      * Engine CPU time / DuckDB CPU time (median wall time × median busy cores). With the wall-time
      * ratio, this splits the gap into work done per row and parallelism.
      */
    def cpuRatio: Double =
      (engineMedian * engineCores) / math.max(duckdbMedian * duckdbCores, 0.001)

    def duckdbMedian: Double = median(duckdbMs)
    def engineMedian: Double = median(engineMs)
    def ratio: Double        = engineMedian / math.max(duckdbMedian, 0.001)
    def rowsMatch: Boolean   = duckdbRows == engineRows
    def logLine: String      =
      f"${name}%-4s DuckDB median ${duckdbMedian}%9.1f ms  engine median ${engineMedian}%9.1f ms  (${ratio}%6.1fx)  engine min ${engineMs
          .headOption
          .getOrElse(0.0)}%9.1f ms  plan ${enginePlanMs}%7.1f ms${
          if rowsMatch then
            ""
          else
            s"  ROW COUNT MISMATCH duckdb=${duckdbRows} engine=${engineRows}"
        }"

  end QueryTiming

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

  /**
    * Wall time, busy cores, GC time, allocated MB, planning time, live heap MB (-1 without a GC),
    * peak tracked memory MB, and MB written to spill files of one engine run
    */
  case class EngineRun(
      ms: Double,
      cores: Double,
      gcMs: Double,
      allocMb: Double,
      planMs: Double,
      liveHeapMb: Double,
      peakMb: Double,
      spilledMb: Double
  )

  /**
    * Plans and runs a query on an engine through [[WvletEngine.runQuery]], as
    * [[WvletEngine.execute]] does (so an OutOfMemoryError fails it with RESOURCE_EXHAUSTED and
    * stops its tasks), returning the wall time, the result rows, and the milliseconds spent
    * planning (before any operator ran)
    */
  def plannedRun(engine: WvletEngine, q: Relation)(using ctx: Context): PlannedRun =
    val start = System.nanoTime()
    engine.runQuery(q) { (op, memory) =>
      val planned = System.nanoTime()
      val rows    = readAll(memory.drain(op))
      PlannedRun(
        (System.nanoTime() - start) / 1000000.0,
        rows,
        (planned - start) / 1000000.0,
        memory.peakBytes / 1048576.0,
        memory.spilledBytes / 1048576.0
      )
    }

  /**
    * Wall time, result rows, planning time, peak tracked memory, and MB written to spill files of
    * one engine run
    */
  case class PlannedRun(ms: Double, rows: Long, planMs: Double, peakMb: Double, spilledMb: Double)

  /**
    * Read every value of every row as a boxed value, as [[countRows]] makes DuckDB's JDBC driver
    * do, so that both engines pay for handing out the whole result. Returns the row count
    */
  def readAll(batches: Seq[Batch]): Long =
    var rows = 0L
    var sink = 0L
    batches.foreach { b =>
      var c = 0
      while c < b.numColumns do
        val v = b.column(c)
        var i = 0
        while i < b.size do
          val x = v.get(i)
          if x != null then
            sink += 1
          i += 1
        c += 1
      rows += b.size
    }
    // Keep the reads from being optimized away
    if sink < 0 then
      rows - 1
    else
      rows

  /**
    * Logs and writes `target/engine-bench/<label>-sf<sf>.env.txt`: the machine, the JVM, the
    * engine's parallelism, and DuckDB's version and settings for this run
    */
  private def logEnvironment(conn: Connection, label: String, opts: BenchOptions): Unit =
    val duck = Seq.newBuilder[String]
    val st   = conn.createStatement()
    try
      val rs = st.executeQuery(
        "select 'version', version() union all select name, value from duckdb_settings() " +
          "where name in ('threads', 'memory_limit', 'preserve_insertion_order', " +
          "'parquet_metadata_cache', 'enable_external_file_cache', 'temp_directory')"
      )
      while rs.next() do
        duck += s"${rs.getString(1)}=${rs.getString(2)}"
    finally
      st.close()
    val rt    = Runtime.getRuntime
    val lines = Seq(
      s"machine: ${sys.props.getOrElse("os.name", "")} ${sys.props.getOrElse("os.arch", "")}, ${rt
          .availableProcessors()} processors",
      s"jvm: ${sys.props.getOrElse("java.vm.name", "")} ${sys
          .props
          .getOrElse("java.version", "")}, max heap ${rt.maxMemory() / 1048576} MB",
      s"engine: parallelism ${exec
          .TaskRunner
          .configuredParallelism(rt.availableProcessors())}, keys ${opts.keys}, cold ${opts.cold}",
      s"duckdb: ${duck.result().mkString(", ")}${
          if opts.duckdbDefaults then
            " (defaults)"
          else
            ""
        }"
    )
    info(lines.mkString("Benchmark environment\n  ", "\n  ", ""))
    val out = File("target/engine-bench", s"${label}-sf${sfLabel(opts.sf)}.env.txt")
    out.getParentFile.mkdirs()
    Files.writeString(out.toPath, lines.mkString("", "\n", "\n"))

  end logEnvironment

  /**
    * A Parquet table with no unique keys, not even a sidecar file's. Only for single files: the
    * copies a folder's source makes for other scan paths read its sidecar keys again
    */
  private class UnkeyedParquetSource(name: String, path: String)
      extends catalog.ParquetSource(name, path):
    override def uniqueKeys: Seq[Seq[String]] = Nil

  /** Accumulated collection time of the JVM's garbage collectors */
  def gcMillis(): Long =
    var total = 0L
    java
      .lang
      .management
      .ManagementFactory
      .getGarbageCollectorMXBeans
      .forEach { b =>
        total += math.max(0L, b.getCollectionTime)
      }
    total

  /** Bytes allocated so far by all live threads, or 0 when the JVM can't tell */
  def allocatedBytes(): Long =
    java.lang.management.ManagementFactory.getThreadMXBean match
      case t: com.sun.management.ThreadMXBean if t.isThreadAllocatedMemoryEnabled =>
        t.getTotalThreadAllocatedBytes
      case _ =>
        0L

  /** CPU time of this process (every thread, GC and JIT included) */
  def processCpuNanos(): Long =
    java.lang.management.ManagementFactory.getOperatingSystemMXBean match
      case os: com.sun.management.OperatingSystemMXBean =>
        os.getProcessCpuTime
      case _ =>
        0L

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

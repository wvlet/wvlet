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
import wvlet.lang.engine.exec.TaskGroup
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.log.LogSupport
import wvlet.uni.test.UniTest

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
  * Opt-in benchmark of short-query latency next to a long query, with and without
  * `fair_task_scheduling`. A stream of short TPC-H queries runs one after another, first alone,
  * then while another thread runs the long query in a loop; rounds alternate between the toggle on
  * and off in one JVM. Logs the short query's median and 90th-percentile latency in each case and
  * how many long runs finished
  *
  * {{{
  *   WVLET_ENGINE_BENCH_SF=10 WVLET_ENGINE_MIXED_BENCH=1 ./sbt "engineJVM/testOnly *EngineMixedWorkloadBench"
  * }}}
  *
  * Environment variables:
  *   - `WVLET_ENGINE_BENCH_SF`: TPC-H scale factor, as for [[EngineTPCHBench]]
  *   - `WVLET_ENGINE_MIXED_BENCH`: set 1 to run (the probe is ignored otherwise)
  *   - `WVLET_ENGINE_MIXED_LONG`, `WVLET_ENGINE_MIXED_SHORT`: a spec/tpch query name (`q6`) or SQL
  *     text. The long query defaults to a group-by of lineitem on (l_partkey, l_suppkey), about 1 s
  *     at SF10, since no TPC-H query runs long enough there to leave the first levels; the short
  *     one to q6
  *   - `WVLET_ENGINE_MIXED_RUNS`: short queries timed per case (default 20)
  *   - `WVLET_ENGINE_MIXED_ROUNDS`: on/off rounds (default 2)
  */
class EngineMixedWorkloadBench extends UniTest with LogSupport:
  import EngineTPCHBench.*

  test("time short queries next to a long one, with and without fair task scheduling") {
    val sf = sys.env.get("WVLET_ENGINE_BENCH_SF")
    if sf.isEmpty || !sys.env.get("WVLET_ENGINE_MIXED_BENCH").contains("1") then
      ignore("Set WVLET_ENGINE_BENCH_SF=<sf> and WVLET_ENGINE_MIXED_BENCH=1 to run")
    val dataDir  = prepareData(sf.get.toDouble)
    val longName = sys
      .env
      .getOrElse(
        "WVLET_ENGINE_MIXED_LONG",
        "select l_partkey, l_suppkey, count(*) as n, sum(l_extendedprice) as s from lineitem " +
          "group by l_partkey, l_suppkey order by n desc, s desc limit 10"
      )
    val shortName = sys.env.getOrElse("WVLET_ENGINE_MIXED_SHORT", "q6")
    val runs      = sys.env.get("WVLET_ENGINE_MIXED_RUNS").map(_.toInt).getOrElse(20)
    val rounds    = sys.env.get("WVLET_ENGINE_MIXED_ROUNDS").map(_.toInt).getOrElse(2)

    val on = WvletEngine(baseDir = "spec/tpch")
    tables.foreach(t => on.catalog.registerParquet(t, s"${dataDir}/${t}.parquet"))
    val off = on.withOptimizer(OptimizerOptions().withDisabled(Optimization.FairTaskScheduling))

    val compiler = Compiler(
      CompilerOptions(sourceFolders = List("spec/tpch"), workEnv = WorkEnv("spec/tpch"))
    )
    def compiled(name: String): (Relation, Context) =
      val unit =
        if name.contains(" ") then
          CompilationUnit.fromSqlString(name)
        else
          compiler
            .localCompilationUnits
            .find(_.sourceFile.fileName == s"${name}.wv")
            .getOrElse(throw IllegalArgumentException(s"No spec/tpch/${name}.wv"))
      val ctx = compiler.compileSingleUnit(unit).context
      (queryOf(unit).get, ctx)
    val (longQ, longCtx)   = compiled(longName)
    val (shortQ, shortCtx) = compiled(shortName)

    def runMs(e: WvletEngine, q: Relation, ctx: Context): Double = plannedRun(e, q)(using ctx).ms

    def shortStream(e: WvletEngine): Seq[Double] = (1 to runs).map(_ => runMs(e, shortQ, shortCtx))

    // Short-query latencies, and the long query's runs that finished, with the long one in a loop
    def mixed(e: WvletEngine): (Seq[Double], Seq[Double]) =
      val stop    = AtomicBoolean(false)
      val started = AtomicInteger(0)
      val longMs  = Seq.newBuilder[Double]
      val looper  = Thread(() =>
        while !stop.get() do
          started.incrementAndGet()
          longMs += runMs(e, longQ, longCtx)
      )
      looper.start()
      // Let the long query queue its morsels first
      Thread.sleep(200)
      val shorts =
        try shortStream(e)
        finally stop.set(true)
      looper.join()
      (shorts, longMs.result())

    def p90(xs: Seq[Double]): Double = xs.sorted.apply(((xs.size - 1) * 0.9).toInt)
    def fmt(xs: Seq[Double]): String = f"median ${median(xs)}%.1f ms, p90 ${p90(xs)}%.1f ms"

    // Warm-up of both queries and both engines
    Seq(on, off).foreach { e =>
      runMs(e, longQ, longCtx)
      shortStream(e)
    }
    // How much of each query's CPU time runs as pool tasks, which the scheduling can order
    def poolShare(label: String, q: Relation, ctx: Context): Unit =
      val g    = TaskGroup.newGroup()
      val cpu0 = processCpuNanos()
      val ms   = TaskGroup.withGroup(g)(runMs(on, q, ctx))
      val cpu  = (processCpuNanos() - cpu0) / 1e6
      val pool = g.runNanos / 1e6
      info(
        f"${label}: ${ms}%.1f ms, process CPU ${cpu}%.1f ms, pool tasks ${pool}%.1f ms in ${g
            .finishedTasks} tasks (mean ${pool / math.max(1L, g.finishedTasks)}%.2f ms)"
      )
    val longAlone = (1 to 3).map(_ => runMs(on, longQ, longCtx))
    val longLabel =
      if longName.contains(" ") then
        "long"
      else
        longName
    info(f"${longLabel} alone: median ${median(longAlone)}%.1f ms")
    poolShare(longLabel, longQ, longCtx)
    poolShare(shortName, shortQ, shortCtx)
    (1 to rounds).foreach { r =>
      val order =
        if r % 2 == 1 then
          Seq("on" -> on, "off" -> off)
        else
          Seq("off" -> off, "on" -> on)
      order.foreach { (label, e) =>
        val alone          = shortStream(e)
        val gc0            = gcMillis()
        val (shorts, long) = mixed(e)
        val gc             = gcMillis() - gc0
        info(
          f"round ${r} ${label}: ${shortName} alone ${fmt(alone)}; next to ${longLabel} ${fmt(
              shorts
            )} (${median(shorts) / median(alone)}%.2fx); ${long
              .size} ${longLabel} runs, median ${median(long)}%.1f ms; GC ${gc} ms"
        )
      }
    }
  }

end EngineMixedWorkloadBench

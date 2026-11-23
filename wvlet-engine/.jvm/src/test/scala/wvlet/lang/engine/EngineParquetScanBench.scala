/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine

import wvlet.lang.engine.catalog.*
import wvlet.uni.test.UniTest

import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Paths

/**
  * Opt-in probe of the engine's Parquet scan cost: reads every column of the TPC-DS fact tables
  * through [[ParquetSource]] one row group at a time on this thread, and logs the bytes allocated
  * and the thread CPU time per table. Allocation does not depend on machine load, so it compares
  * two builds even on a busy machine; CPU time is the best of the runs.
  *
  * {{{
  *   WVLET_ENGINE_BENCH_SF=1 ./sbt "engineJVM/testOnly *EngineParquetScanBench"
  * }}}
  *
  * `WVLET_ENGINE_BENCH_RUNS` sets the timed runs per table after one warm-up (default 5).
  */
class EngineParquetScanBench extends UniTest:
  private val tables = Seq(
    "store_sales",
    "catalog_sales",
    "web_sales",
    "inventory",
    "store_returns",
    "catalog_returns",
    "customer"
  )

  test("time full scans of the TPC-DS fact tables") {
    sys.env.get("WVLET_ENGINE_BENCH_SF") match
      case None =>
        ignore("Set WVLET_ENGINE_BENCH_SF=<scale factor> (e.g. 1) to run the scan probe")
      case Some(sf) =>
        val dir   = Paths.get(s"target/tpcds-sf${sf}")
        val runs  = sys.env.get("WVLET_ENGINE_BENCH_RUNS").map(_.toInt).getOrElse(5)
        val mx    = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
        val tid   = Thread.currentThread().threadId()
        val lines = Seq.newBuilder[String]
        lines += "| table | rows | allocated (MB) | CPU best (ms) |"
        lines += "|---|---|---|---|"
        var totalBytes = 0L
        var totalCpu   = 0.0
        tables
          .map(t => t -> dir.resolve(s"${t}.parquet"))
          .filter((_, p) => Files.exists(p))
          .foreach { (t, p) =>
            val src              = ParquetSource(t, p.toString)
            def scanOnce(): Long =
              val parts =
                src.scanSplits(ScanSpec()) match
                  case Some(s) =>
                    try s.parts.map(part => part().map(_.size.toLong).sum).sum
                    finally s.close()
                  case None =>
                    src.scan(ScanSpec()).map(_.size.toLong).sum
              parts
            val rows  = scanOnce() // warm-up
            var bytes = Long.MaxValue
            var cpu   = Long.MaxValue
            (1 to runs).foreach { _ =>
              val a0 = mx.getThreadAllocatedBytes(tid)
              val c0 = mx.getThreadCpuTime(tid)
              scanOnce()
              cpu = math.min(cpu, mx.getThreadCpuTime(tid) - c0)
              bytes = math.min(bytes, mx.getThreadAllocatedBytes(tid) - a0)
            }
            totalBytes += bytes
            totalCpu += cpu / 1e6
            lines += f"| ${t} | ${rows} | ${bytes / 1e6}%.1f | ${cpu / 1e6}%.1f |"
          }
        lines += f"| total | | ${totalBytes / 1e6}%.1f | ${totalCpu}%.1f |"
        info(lines.result().mkString("\n", "\n", ""))
  }

end EngineParquetScanBench

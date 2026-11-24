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
package wvlet.lang.engine.gpu

import wvlet.lang.engine.EngineConformance
import wvlet.lang.engine.EngineTPCHBench
import wvlet.uni.test.UniTest

/**
  * Runs spec/tpch with streamed scans over the small spec data: every table over 1,000 rows
  * streams, in ranges of 2,500 rows, and string columns over 4 KB are cached in pieces of 16 KB, so
  * the per-range execution, the merge of partial aggregates, and the piecewise cache run as they do
  * at SF100. Rows are checked against DuckDB. Skipped without a CUDA device. `WVLET_GPU_TEST_DATA`
  * names another folder of TPC-H Parquet files, and `WVLET_GPU_STREAM_ROWS`,
  * `WVLET_GPU_CHUNK_ROWS`, and `WVLET_GPU_PIECE_BYTES` override the sizes (e.g. 10M, 4M, and 64M
  * for SF10).
  */
class GpuStreamTest extends UniTest:
  private val tables  = EngineTPCHBench.tables
  private val dataDir = sys.env.getOrElse("WVLET_GPU_TEST_DATA", "spec/tpch/data")

  private val config =
    val c                   = GpuConfig.fromEnv()
    def unset(name: String) = !sys.env.contains(name)
    Seq[(String, GpuConfig => GpuConfig)](
      "WVLET_GPU_STREAM_ROWS"     -> (_.withStreamRows(1000)),
      "WVLET_GPU_STREAM_FRACTION" -> (_.withStreamFraction(0)),
      "WVLET_GPU_CHUNK_ROWS"      -> (_.withChunkRows(2500)),
      "WVLET_GPU_PIECE_BYTES"     -> (_.withPieceBytes(16 * 1024).withStringLimit(4 * 1024))
    ).foldLeft(c)((acc, kv) =>
      if unset(kv._1) then
        kv._2(acc)
      else
        acc
    )

  private val conformance = EngineConformance(
    "spec/tpch",
    tables.map(t => s"create view ${t} as select * from '${dataDir}/${t}.parquet'"),
    engine => tables.foreach(t => engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet")),
    accelerator = Some(GpuAccelerator(config))
  )

  override def afterAll: Unit = conformance.close()

  private def requireGpu(): Unit =
    if !GpuRuntime.isAvailable then
      ignore("No CUDA device")

  test("keep streamed queries on the GPU") {
    requireGpu()
    // q12 aggregates its streamed l_shipmode per range, but its build side concatenates orders'
    // o_orderpriority, over the 4 KB string limit here. q13 filters o_comment per range
    // (stream_residual) and doesn't concatenate it
    val onGpu =
      conformance
        .units
        .flatMap { unit =>
          val name = unit.sourceFile.fileName.stripSuffix(".wv")
          conformance.explain(unit).map(plan => (name, plan.contains("Gpu[")))
        }
        .toMap
    Seq("q1", "q6", "q13").foreach { q =>
      (q, onGpu.get(q)) shouldBe (q, Some(true))
    }
    onGpu.get("q12") shouldBe Some(false)
  }

  for unit <- conformance.units do
    test(s"${unit.sourceFile.fileName} with streamed scans") {
      requireGpu()
      val before = GpuStats.streamedRanges
      conformance
        .run(unit)
        .foreach { r =>
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
      debug(s"${unit.sourceFile.fileName}: ${GpuStats.streamedRanges - before} range(s)")
    }

  test("merge global aggregates over ranges without rows") {
    requireGpu()
    // Every range of lineitem is empty after the filter: count is 0, the others NULL
    val r = conformance.compareQuery("""from lineitem
        |where l_quantity < 0
        |select cnt = count(*), total = sum(l_extendedprice), mean = avg(l_discount),
        |  latest = max(l_shipdate)""".stripMargin)
    if !r.matched then
      fail(r.message)
  }

  test("stream scans over several ranges") {
    requireGpu()
    // Set by the queries above, which run first in this class
    (GpuStats.streamedRanges > 0) shouldBe true
  }

end GpuStreamTest

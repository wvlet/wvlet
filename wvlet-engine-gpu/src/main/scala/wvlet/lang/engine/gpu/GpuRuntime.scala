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

import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.Cuda
import ai.rapids.cudf.ParquetOptions
import ai.rapids.cudf.Rmm
import ai.rapids.cudf.RmmAllocationMode
import ai.rapids.cudf.Table
import wvlet.uni.log.LogSupport

import java.io.File
import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Settings of the GPU executor.
  *
  * @param poolFraction
  *   the share of free device memory the RMM pool takes at start-up
  * @param cacheFraction
  *   the share of the pool that cached table columns may use; the least recently used columns are
  *   evicted beyond it
  */
case class GpuConfig(poolFraction: Double = 0.8, cacheFraction: Double = 0.5):
  def withPoolFraction(f: Double): GpuConfig  = copy(poolFraction = f)
  def withCacheFraction(f: Double): GpuConfig = copy(cacheFraction = f)

object GpuConfig:
  /** Read `WVLET_GPU_POOL_FRACTION` and `WVLET_GPU_CACHE_FRACTION`, with defaults otherwise */
  def fromEnv(): GpuConfig =
    def frac(name: String, default: Double): Double = sys
      .env
      .get(name)
      .flatMap(_.toDoubleOption)
      .filter(f => f > 0 && f <= 1)
      .getOrElse(default)
    GpuConfig(frac("WVLET_GPU_POOL_FRACTION", 0.8), frac("WVLET_GPU_CACHE_FRACTION", 0.5))

/** The CUDA device and its memory pool, initialized once per JVM */
object GpuRuntime extends LogSupport:

  /** Whether a CUDA device and the cudf native libraries are usable */
  lazy val isAvailable: Boolean =
    try
      Cuda.getDeviceCount > 0
    catch
      case e: Throwable if NonFatal(e) || e.isInstanceOf[LinkageError] =>
        debug(s"No usable CUDA device: ${e}")
        false

  private var poolBytes: Long = 0L

  /** Initialize the RMM pool (once), and return its size in bytes */
  def init(config: GpuConfig): Long = synchronized {
    if !Rmm.isInitialized then
      val free = Cuda.memGetInfo().free
      // RMM pools are allocated in 256-byte units
      poolBytes = (free * config.poolFraction).toLong / 256 * 256
      Rmm.initialize(RmmAllocationMode.POOL, null, poolBytes)
      info(
        f"GPU memory pool: ${poolBytes / (1L << 30).toDouble}%.1f GiB of ${free /
            (1L << 30).toDouble}%.1f GiB free"
      )
    poolBytes
  }

end GpuRuntime

/**
  * Table columns kept on the device across queries, so that repeated scans skip reading and
  * decoding Parquet (as Sirius's `pin_table` does). Columns are read one at a time on first use and
  * evicted least-recently-used beyond `budgetBytes`. A changed file (size or modification time) is
  * read again.
  */
class GpuColumnCache(budgetBytes: Long) extends LogSupport:
  private case class Key(path: String, column: String, length: Long, modified: Long)

  // Access order: the first entry is the least recently used
  private val entries = mutable.LinkedHashMap.empty[Key, ColumnVector]
  private var bytes   = 0L

  /** The column of a Parquet file, which the caller must close */
  def column(path: String, name: String): ColumnVector =
    val f   = File(path)
    val key = Key(f.getCanonicalPath, name, f.length(), f.lastModified())
    synchronized {
      entries.remove(key) match
        case Some(cv) =>
          entries(key) = cv
          return cv.incRefCount()
        case None =>
    }
    val cv = read(f, name)
    synchronized {
      entries.get(key) match
        case Some(other) =>
          // Read concurrently by another query
          cv.close()
          other.incRefCount()
        case None =>
          entries(key) = cv
          bytes += cv.getDeviceMemorySize
          evict()
          cv.incRefCount()
    }

  private def read(f: File, name: String): ColumnVector =
    val nano = System.nanoTime()
    val opts = ParquetOptions.builder().includeColumn(name).build()
    val t    = Table.readParquet(opts, f)
    try
      val cv = t.getColumn(0).incRefCount()
      debug(
        f"Cached ${f.getName}.${name} on the GPU: ${cv.getDeviceMemorySize / 1e6}%.0f MB in ${(
            System.nanoTime() - nano
          ) / 1e6}%.0f ms"
      )
      cv
    finally
      t.close()

  private def evict(): Unit =
    while bytes > budgetBytes && entries.size > 1 do
      val (k, cv) = entries.head
      entries.remove(k)
      bytes -= cv.getDeviceMemorySize
      debug(s"Evicted ${k.column} of ${k.path} from the GPU cache")
      cv.close()

  /** Device memory held by cached columns */
  def sizeBytes: Long = synchronized(bytes)

  def clear(): Unit = synchronized {
    entries.values.foreach(_.close())
    entries.clear()
    bytes = 0
  }

end GpuColumnCache

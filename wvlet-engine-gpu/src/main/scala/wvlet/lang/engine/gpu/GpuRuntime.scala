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
import ai.rapids.cudf.DType
import ai.rapids.cudf.Scalar
import ai.rapids.cudf.Table
import wvlet.uni.log.LogSupport

import java.io.File
import java.math.BigDecimal as JBigDecimal
import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Optional parts of the GPU executor, each on by default. A feature can be turned off with
  * `WVLET_GPU_DISABLE=<name>,...`, and `WVLET_GPU_BENCH_AB=<name>,...` compares on and off in the
  * GPU benchmark.
  */
enum GpuFeature(val featureName: String):
  /** Sum decimals as two INT64 halves, so that the group-by stays on cudf's hash path */
  case DecimalSumSplit extends GpuFeature("decimal_sum_split")

  /**
    * Compute decimal arithmetic in DECIMAL64 when the value bounds of the cached columns allow it,
    * even when the declared precision needs DECIMAL128
    */
  case DecimalNarrow extends GpuFeature("decimal_narrow")

  /** Read integer constants behind casts (`substring(c, cast(1 as long), ...)` in TPC-H q22) */
  case ConstantCasts extends GpuFeature("constant_casts")

  /** Upload constant relations (`Values`, e.g. a subquery computed at plan time; TPC-H q16) */
  case UploadValues extends GpuFeature("upload_values")

  /** Left joins with a residual condition (TPC-H q13) */
  case OuterResidual extends GpuFeature("outer_residual")

  /**
    * Filter join inputs early by the single-input predicates that OR conditions above the joins
    * imply (TPC-H q7, q19)
    */
  case ImpliedFilters extends GpuFeature("implied_filters")

  /** count(distinct x) as distinct rows, then a count: hash-based instead of cudf's `nunique` */
  case DistinctCount extends GpuFeature("distinct_count")

  /**
    * Filter a join's right input by the left input's keys before the right input aggregates or
    * joins further (TPC-H q17, q9)
    */
  case JoinKeyFilter extends GpuFeature("join_key_filter")

  /** Copy DECIMAL128 and string results to the host as whole buffers, not one value per row */
  case BulkCopy extends GpuFeature("bulk_copy")

object GpuFeature:
  def parse(names: String): Set[GpuFeature] = names
    .split(",")
    .map(_.trim)
    .filter(_.nonEmpty)
    .map(n =>
      values
        .find(_.featureName == n)
        .getOrElse(
          throw IllegalArgumentException(
            s"Unknown GPU feature ${n}; one of ${values.map(_.featureName).mkString(", ")}"
          )
        )
    )
    .toSet

/**
  * Settings of the GPU executor.
  *
  * @param poolFraction
  *   the share of free device memory the RMM pool takes at start-up
  * @param cacheFraction
  *   the share of the pool that cached table columns may use; the least recently used columns are
  *   evicted beyond it
  * @param disabled
  *   the optional features turned off
  * @param profile
  *   log the device time of each plan node (synchronizing after each, so slower)
  */
case class GpuConfig(
    poolFraction: Double = 0.8,
    cacheFraction: Double = 0.5,
    disabled: Set[GpuFeature] = Set.empty,
    profile: Boolean = false
):
  def withPoolFraction(f: Double): GpuConfig           = copy(poolFraction = f)
  def withCacheFraction(f: Double): GpuConfig          = copy(cacheFraction = f)
  def withDisabled(fs: Set[GpuFeature]): GpuConfig     = copy(disabled = fs)
  def noDisabled(): GpuConfig                          = copy(disabled = Set.empty)
  def withProfile(enabled: Boolean): GpuConfig         = copy(profile = enabled)
  def isEnabled(f: GpuFeature): Boolean                = !disabled.contains(f)

object GpuConfig:
  /**
    * Read `WVLET_GPU_POOL_FRACTION`, `WVLET_GPU_CACHE_FRACTION`, `WVLET_GPU_DISABLE`, and
    * `WVLET_GPU_PROFILE`, with defaults otherwise
    */
  def fromEnv(): GpuConfig =
    def frac(name: String, default: Double): Double = sys
      .env
      .get(name)
      .flatMap(_.toDoubleOption)
      .filter(f => f > 0 && f <= 1)
      .getOrElse(default)
    GpuConfig(
      frac("WVLET_GPU_POOL_FRACTION", 0.8),
      frac("WVLET_GPU_CACHE_FRACTION", 0.5),
      sys.env.get("WVLET_GPU_DISABLE").map(GpuFeature.parse).getOrElse(Set.empty),
      sys.env.get("WVLET_GPU_PROFILE").exists(v => v == "1" || v == "true")
    )

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

  /** A cached column and the largest absolute value of a numeric column (None for others) */
  private case class Entry(cv: ColumnVector, bound: Option[JBigDecimal])

  // Access order: the first entry is the least recently used
  private val entries = mutable.LinkedHashMap.empty[Key, Entry]
  private var bytes   = 0L

  private def keyOf(path: String, name: String): Key =
    val f = File(path)
    Key(f.getCanonicalPath, name, f.length(), f.lastModified())

  /** The column of a Parquet file, which the caller must close */
  def column(path: String, name: String): ColumnVector = entry(path, name, ref = true).cv

  /**
    * The largest absolute value of a numeric column of a Parquet file, computed once on the device
    * when the column is cached
    */
  def bound(path: String, name: String): Option[JBigDecimal] = entry(path, name, ref = false).bound

  /**
    * The cache entry of a column, read on first use. With `ref`, its column gets a reference for
    * the caller while the cache is locked, so that eviction cannot free it first
    */
  private def entry(path: String, name: String, ref: Boolean): Entry =
    val key = keyOf(path, name)
    synchronized {
      entries.remove(key) match
        case Some(e) =>
          entries(key) = e
          if ref then
            e.cv.incRefCount()
          return e
        case None =>
    }
    val cv = read(File(path), name)
    val e  =
      try Entry(cv, maxAbs(cv))
      catch
        case t: Throwable =>
          cv.close()
          throw t
    synchronized {
      entries.get(key) match
        case Some(other) =>
          // Read concurrently by another query
          cv.close()
          if ref then
            other.cv.incRefCount()
          other
        case None =>
          entries(key) = e
          bytes += cv.getDeviceMemorySize
          if ref then
            cv.incRefCount()
          evict()
          e
    }

  /** max(|min|, |max|) of a decimal or integer column, or None for other types */
  private def maxAbs(cv: ColumnVector): Option[JBigDecimal] =
    val t = cv.getType
    def value(s: Scalar): Option[JBigDecimal] =
      try
        if !s.isValid then
          Some(JBigDecimal.ZERO)
        else if t.isDecimalType then
          Some(s.getBigDecimal.abs())
        else
          t.getTypeId match
            case DType.DTypeEnum.INT32 =>
              Some(JBigDecimal.valueOf(s.getInt.toLong).abs())
            case DType.DTypeEnum.INT64 =>
              Some(JBigDecimal.valueOf(s.getLong).abs())
            case _ =>
              None
      finally s.close()
    if t.isDecimalType || t == DType.INT32 || t == DType.INT64 then
      value(cv.min()).zip(value(cv.max())).map(_.max(_))
    else
      None

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
      val (k, Entry(cv, _)) = entries.head
      entries.remove(k)
      bytes -= cv.getDeviceMemorySize
      debug(s"Evicted ${k.column} of ${k.path} from the GPU cache")
      cv.close()

  /** Device memory held by cached columns */
  def sizeBytes: Long = synchronized(bytes)

  def clear(): Unit = synchronized {
    entries.values.foreach(_.cv.close())
    entries.clear()
    bytes = 0
  }

end GpuColumnCache

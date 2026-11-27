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
import ai.rapids.cudf.CudfColumnSizeOverflowException
import ai.rapids.cudf.Cuda
import ai.rapids.cudf.ParquetChunkedReader
import ai.rapids.cudf.ParquetOptions
import ai.rapids.cudf.Rmm
import ai.rapids.cudf.RmmAllocationMode
import ai.rapids.cudf.DType
import ai.rapids.cudf.Scalar
import ai.rapids.cudf.Table
import wvlet.lang.engine.gpu.GpuPlans.using
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

  /** Don't gather join output columns that no node above reads (TPC-H q13's o_comment) */
  case PruneColumns extends GpuFeature("prune_columns")

  /** count(distinct x) as distinct rows, then a count: hash-based instead of cudf's `nunique` */
  case DistinctCount extends GpuFeature("distinct_count")

  /**
    * Filter a join's right input by the left input's keys before the right input aggregates or
    * joins further (TPC-H q17, q9)
    */
  case JoinKeyFilter extends GpuFeature("join_key_filter")

  /** Copy DECIMAL128 and string results to the host as whole buffers, not one value per row */
  case BulkCopy extends GpuFeature("bulk_copy")

  /**
    * Run scans of tables over `streamRows` rows in row ranges of `chunkRows`, with the nodes above
    * them per range, and merge the per-range results (TPC-H SF100's lineitem and orders)
    */
  case ChunkedScans extends GpuFeature("chunked_scans")

  /**
    * Read and cache the string columns of tables over `streamRows` rows in pieces of about
    * `pieceBytes`, below cudf's 2 GB limit for one string column (`l_shipmode`, `o_comment`)
    */
  case StringPieces extends GpuFeature("string_pieces")

  /**
    * Move the conjuncts of a left join's residual that read only right columns into its right
    * input, when that input streams: they filter each range (TPC-H q13's `o_comment` at SF100)
    */
  case StreamResidual extends GpuFeature("stream_residual")

  /**
    * Before a subtree runs, evict the cached columns it doesn't read until its estimated memory
    * fits in the pool
    */
  case ReserveMemory extends GpuFeature("reserve_memory")

  /**
    * Keep a subtree on the CPU when it would concatenate a string column of a streamed table, which
    * can exceed cudf's 2 GB string column limit
    */
  case OffloadLimits extends GpuFeature("offload_limits")

  /**
    * Group by a single key with cudf's sort-based path when the key is a cached column stored in
    * order (TPC-H q18's l_orderkey)
    */
  case SortedKeys extends GpuFeature("sorted_keys")

  /**
    * Aggregate over a filter that keeps most rows without copying them: the filter's mask becomes
    * one more group key, and the groups of dropped rows are removed from the result (TPC-H q1)
    */
  case MaskedFilter extends GpuFeature("masked_filter")

  /** avg of a decimal or integer as an exact sum and a count per group (TPC-H q1) */
  case AvgAsSum extends GpuFeature("avg_as_sum")

  /** Group by the INT32 codes of low-cardinality string scan columns (TPC-H q1) */
  case DictionaryKeys extends GpuFeature("dictionary_keys")

  /**
    * Evaluate the comparisons of a filter over fixed-width columns as one cudf AST kernel (TPC-H
    * q6, q12, q19)
    */
  case AstFilters extends GpuFeature("ast_filters")

  /**
    * Run a read of a shared CTE (`CTERef`) as the CTE's body. The CPU computes a CTE once for all
    * its readers; on the GPU each reader recomputes it from the cached columns instead (the
    * filtered scans TPC-H q15 and q22 share between a plan-time subquery and the main query)
    */
  case InlineCTEs extends GpuFeature("inline_ctes")

end GpuFeature

object GpuFeature:
  def parse(names: String): Set[GpuFeature] =
    names
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
  * @param streamRows
  *   scans of tables with more rows than this stream in row ranges ([[GpuFeature.ChunkedScans]])
  * @param streamFraction
  *   a scan of such a table streams only when its estimated working memory on the whole path
  *   exceeds this share of the pool, or when a string column of it is too large to concatenate (0:
  *   always)
  * @param chunkRows
  *   the rows of one range of a streamed scan
  * @param pieceBytes
  *   the size of one piece of a string column read in pieces ([[GpuFeature.StringPieces]])
  * @param stringLimit
  *   string columns of tables over `streamRows` rows with more estimated characters than this are
  *   cached in pieces and never concatenated, a margin below cudf's 2 GB string column limit
  */
case class GpuConfig(
    poolFraction: Double = 0.8,
    cacheFraction: Double = 0.5,
    disabled: Set[GpuFeature] = Set.empty,
    profile: Boolean = false,
    streamRows: Long = GpuConfig.DefaultStreamRows,
    streamFraction: Double = 0.9,
    chunkRows: Int = GpuConfig.DefaultChunkRows,
    pieceBytes: Long = GpuConfig.DefaultPieceBytes,
    stringLimit: Long = GpuConfig.DefaultStringLimit
):
  def withPoolFraction(f: Double): GpuConfig       = copy(poolFraction = f)
  def withCacheFraction(f: Double): GpuConfig      = copy(cacheFraction = f)
  def withDisabled(fs: Set[GpuFeature]): GpuConfig = copy(disabled = fs)
  def noDisabled(): GpuConfig                      = copy(disabled = Set.empty)
  def withProfile(enabled: Boolean): GpuConfig     = copy(profile = enabled)
  def withStreamRows(n: Long): GpuConfig           = copy(streamRows = n)
  def withStreamFraction(f: Double): GpuConfig     = copy(streamFraction = f)
  def withChunkRows(n: Int): GpuConfig             = copy(chunkRows = n)
  def withStringLimit(n: Long): GpuConfig          = copy(stringLimit = n)
  def withPieceBytes(n: Long): GpuConfig           = copy(pieceBytes = n)
  def isEnabled(f: GpuFeature): Boolean            = !disabled.contains(f)

object GpuConfig:
  /** More rows than TPC-H SF10's largest table (60M), fewer than SF100's orders (150M) */
  val DefaultStreamRows: Long = 100_000_000L

  /** 32M rows: string columns of up to 60 bytes a row stay below 2 GB per range */
  val DefaultChunkRows: Int = 1 << 25

  val DefaultPieceBytes: Long = 1L << 30

  /** 1.5 GB */
  val DefaultStringLimit: Long = 3L << 29

  /**
    * Read `WVLET_GPU_POOL_FRACTION`, `WVLET_GPU_CACHE_FRACTION`, `WVLET_GPU_DISABLE`,
    * `WVLET_GPU_PROFILE`, `WVLET_GPU_STREAM_ROWS`, `WVLET_GPU_STREAM_FRACTION`,
    * `WVLET_GPU_CHUNK_ROWS`, and `WVLET_GPU_PIECE_BYTES`, with defaults otherwise
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
      sys.env.get("WVLET_GPU_PROFILE").exists(v => v == "1" || v == "true"),
      positive("WVLET_GPU_STREAM_ROWS").getOrElse(DefaultStreamRows),
      sys
        .env
        .get("WVLET_GPU_STREAM_FRACTION")
        .flatMap(_.toDoubleOption)
        .filter(f => f >= 0 && f <= 1)
        .getOrElse(0.9),
      positive("WVLET_GPU_CHUNK_ROWS")
        .map(n => math.min(n, Int.MaxValue).toInt)
        .getOrElse(DefaultChunkRows),
      positive("WVLET_GPU_PIECE_BYTES").getOrElse(DefaultPieceBytes)
    )

  end fromEnv

  private def positive(name: String): Option[Long] = sys
    .env
    .get(name)
    .flatMap(_.trim.toLongOption)
    .filter(_ > 0)

end GpuConfig

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

  /** The size of the memory pool, or 0 before [[init]] */
  def poolSize: Long = synchronized(poolBytes)

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
  * A column held on the device as pieces of consecutive rows: one piece, unless it was read in
  * pieces to stay below cudf's 2 GB string column limit. `bound` is the largest absolute value of a
  * numeric column (None for others). Owns one reference to each piece.
  */
private[gpu] final class CachedColumn(
    val pieces: IndexedSeq[ColumnVector],
    val bound: Option[JBigDecimal]
) extends AutoCloseable:
  private val starts: Array[Long] = pieces.scanLeft(0L)(_ + _.getRowCount).toArray

  def rows: Long = starts.last

  def sizeBytes: Long = pieces.map(_.getDeviceMemorySize).sum

  /** Another reference to these pieces, which the caller closes */
  def retain(): CachedColumn =
    pieces.foreach(_.incRefCount())
    CachedColumn(pieces, bound)

  /** The whole column as one owned column (a reference when it is one piece) */
  def whole: ColumnVector = slice(0L, rows)

  /** Rows [from, to) as one owned column */
  def slice(from: Long, to: Long): ColumnVector =
    val parts = pieces.indices.filter(i => starts(i) < to && starts(i + 1) > from)
    if parts.isEmpty then
      pieces.head.subVector(0, 0)
    else
      val cols =
        GpuPlans.buildAll(parts.size) { k =>
          val i  = parts(k)
          val p  = pieces(i)
          val lo = (math.max(from, starts(i)) - starts(i)).toInt
          val hi = (math.min(to, starts(i + 1)) - starts(i)).toInt
          if lo == 0 && hi == p.getRowCount then
            p.incRefCount()
          else
            p.subVector(lo, hi)
        }
      if cols.size == 1 then
        cols.head
      else
        try ColumnVector.concatenate(cols*)
        finally cols.foreach(_.close())

  override def close(): Unit = pieces.foreach(_.close())

end CachedColumn

/**
  * Table columns kept on the device across queries, so that repeated scans skip reading and
  * decoding Parquet (as Sirius's `pin_table` does). Columns are read one at a time on first use and
  * evicted least-recently-used beyond `budgetBytes`. A changed file (size or modification time) is
  * read again. A column read with `pieces` (or one too large to read whole) is kept as pieces of
  * about `pieceBytes`.
  */
class GpuColumnCache(budgetBytes: Long, pieceBytes: Long = GpuConfig.DefaultPieceBytes)
    extends LogSupport:
  private case class Key(path: String, column: String, length: Long, modified: Long)

  // Access order: the first entry is the least recently used
  private val entries = mutable.LinkedHashMap.empty[Key, CachedColumn]
  private var bytes   = 0L

  // Per cached column, checked or built on first request, and dropped with the column
  private val sortedness = mutable.HashMap.empty[Key, Boolean]
  // Dictionaries of string columns (None: too many distinct values)
  private val dicts = mutable.HashMap.empty[Key, Option[GpuDictionary]]

  private def keyOf(path: String, name: String): Key =
    val f = File(path)
    Key(f.getCanonicalPath, name, f.length(), f.lastModified())

  /** The whole column of a Parquet file, which the caller must close */
  def column(path: String, name: String, pieces: Boolean = false): ColumnVector =
    val c = columnRef(path, name, pieces)
    try c.whole
    finally c.close()

  /**
    * The cached column of a Parquet file (read on first use, in pieces when `pieces`), with a
    * reference for the caller, who closes it: eviction cannot free it while the caller reads it
    */
  def columnRef(path: String, name: String, pieces: Boolean = false): CachedColumn = entry(
    path,
    name,
    ref = true,
    pieces
  )

  /**
    * The largest absolute value of a numeric column of a Parquet file, computed once on the device
    * when the column is cached
    */
  def bound(path: String, name: String, pieces: Boolean = false): Option[JBigDecimal] =
    entry(path, name, ref = false, pieces).bound

  /**
    * Whether an integer column of a Parquet file, cached in one piece, is non-decreasing and has no
    * NULLs: checked once on the device
    */
  def isSorted(path: String, name: String): Boolean =
    val key = keyOf(path, name)
    synchronized(sortedness.get(key)).getOrElse {
      using(columnRef(path, name)) { c =>
        val sorted = c.pieces.size == 1 && GpuColumnCache.isNonDecreasing(c.pieces.head)
        synchronized {
          // Not kept for a column evicted meanwhile
          if entries.contains(key) then
            sortedness(key) = sorted
        }
        sorted
      }
    }

  /**
    * The dictionary of a string column of a Parquet file, cached in one piece, with few distinct
    * values: built once on the device. The caller must close the returned codes and values.
    */
  def dictionary(path: String, name: String): Option[(ColumnVector, ColumnVector)] =
    val key                            = keyOf(path, name)
    def refs(d: Option[GpuDictionary]) = d.map(b => (b.codes.incRefCount(), b.values.incRefCount()))
    synchronized(dicts.get(key)) match
      case Some(d) =>
        synchronized(refs(d))
      case None =>
        using(columnRef(path, name)) { c =>
          if c.pieces.size != 1 then
            None
          else
            // Built outside the lock: other queries keep using the cache meanwhile
            val built = GpuDictionary.build(c.pieces.head)
            synchronized {
              dicts.get(key) match
                case Some(other) =>
                  // Built concurrently by another query
                  built.foreach(_.close())
                  refs(other)
                case None if !entries.contains(key) =>
                  // Evicted meanwhile: eviction would no longer release a dictionary kept now
                  try refs(built)
                  finally built.foreach(_.close())
                case None =>
                  dicts(key) = built
                  built.foreach(b => bytes += b.sizeBytes)
                  val out = refs(built)
                  evict()
                  out
            }
        }

  end dictionary

  /** Drop what is kept for a column being evicted */
  private def forget(k: Key): Unit =
    sortedness.remove(k)
    dicts
      .remove(k)
      .flatten
      .foreach { d =>
        bytes -= d.sizeBytes
        d.close()
      }

  /**
    * The cache entry of a column, read on first use. With `ref`, a retained copy, taken while the
    * cache is locked so that eviction cannot free it first
    */
  private def entry(path: String, name: String, ref: Boolean, pieces: Boolean): CachedColumn =
    val key = keyOf(path, name)
    synchronized {
      entries.remove(key) match
        case Some(e) =>
          entries(key) = e
          return if ref then
            e.retain()
          else
            e
        case None =>
    }
    val e = read(File(path), name, pieces)
    synchronized {
      entries.get(key) match
        case Some(other) =>
          // Read concurrently by another query
          e.close()
          if ref then
            other.retain()
          else
            other
        case None =>
          entries(key) = e
          bytes += e.sizeBytes
          val out =
            if ref then
              e.retain()
            else
              e
          evict()
          out
    }

  end entry

  /** max(|min|, |max|) of a decimal or integer column, or None for other types */
  private def maxAbs(cv: ColumnVector): Option[JBigDecimal] =
    val t                                     = cv.getType
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

  private def read(f: File, name: String, pieces: Boolean): CachedColumn =
    val nano = System.nanoTime()
    val cols =
      if pieces then
        readPieces(f, name)
      else
        try
          IndexedSeq(readWhole(f, name))
        catch
          case e: CudfColumnSizeOverflowException =>
            debug(s"${f.getName}.${name} exceeds cudf's column size limit: reading it in pieces")
            readPieces(f, name)
    try
      val bounds = cols.map(maxAbs)
      val c      = CachedColumn(
        cols,
        if bounds.forall(_.isDefined) then
          bounds.flatten.reduceOption(_.max(_))
        else
          None
      )
      debug(
        f"Cached ${f.getName}.${name} on the GPU: ${c.sizeBytes / 1e6}%.0f MB in ${cols
            .size} piece(s), ${(System.nanoTime() - nano) / 1e6}%.0f ms"
      )
      c
    catch
      case t: Throwable =>
        cols.foreach(_.close())
        throw t

  end read

  private def readWhole(f: File, name: String): ColumnVector =
    val opts = ParquetOptions.builder().includeColumn(name).build()
    val t    = Table.readParquet(opts, f)
    try t.getColumn(0).incRefCount()
    finally t.close()

  /** The column in pieces of about `pieceBytes` (at least one piece) */
  private def readPieces(f: File, name: String): IndexedSeq[ColumnVector] =
    val opts = ParquetOptions.builder().includeColumn(name).build()
    val out  = IndexedSeq.newBuilder[ColumnVector]
    var done = List.empty[ColumnVector]
    try
      val r = ParquetChunkedReader(pieceBytes, pieceBytes * 4, opts, f)
      try
        while r.hasNext do
          val t = r.readChunk()
          try
            val c = t.getColumn(0).incRefCount()
            done = c :: done
            out += c
          finally
            t.close()
      finally r.close()
      val all = out.result()
      if all.isEmpty then
        IndexedSeq(readWhole(f, name))
      else
        all
    catch
      case e: Throwable =>
        done.foreach(_.close())
        throw e

  end readPieces

  private def evict(): Unit =
    while bytes > budgetBytes && entries.size > 1 do
      evictHead()

  private def evictHead(): Unit =
    val (k, c) = entries.head
    entries.remove(k)
    bytes -= c.sizeBytes
    forget(k)
    debug(s"Evicted ${k.column} of ${k.path} from the GPU cache")
    c.close()

  /**
    * Evict the least recently used columns other than `keep` (path and column name pairs) until the
    * other columns and `needed` bytes fit in `limit`
    */
  def reserve(needed: Long, limit: Long, keep: Set[(String, String)]): Unit = synchronized {
    val kept           = keep.map((p, c) => (File(p).getCanonicalPath, c))
    def isKept(k: Key) = kept.contains((k.path, k.column))
    // A column's device memory includes its dictionary
    def size(k: Key) = entries(k).sizeBytes + dicts.get(k).flatten.map(_.sizeBytes).getOrElse(0L)
    var others       = entries.keysIterator.filterNot(isKept).map(size).sum
    val victims      = entries.keysIterator.filterNot(isKept).toList.iterator
    while others + needed > limit && victims.hasNext do
      val k = victims.next()
      others -= size(k)
      val c = entries.remove(k).get
      bytes -= c.sizeBytes
      forget(k)
      debug(s"Evicted ${k.column} of ${k.path} from the GPU cache to reserve memory")
      c.close()
  }

  /** Whether the column is cached */
  def contains(path: String, name: String): Boolean =
    val key = keyOf(path, name)
    synchronized(entries.contains(key))

  /** Device memory held by cached columns */
  def sizeBytes: Long = synchronized(bytes)

  def clear(): Unit = synchronized {
    entries.values.foreach(_.close())
    entries.clear()
    sortedness.clear()
    dicts.values.flatten.foreach(_.close())
    dicts.clear()
    bytes = 0
  }

end GpuColumnCache

object GpuColumnCache extends LogSupport:
  /** Whether an integer column has no NULLs and each value is at most the next one */
  private[gpu] def isNonDecreasing(cv: ColumnVector): Boolean =
    val t = cv.getType
    val n = cv.getRowCount.toInt
    (t == DType.INT32 || t == DType.INT64) && cv.getNullCount == 0 && n > 1 && {
      val nano = System.nanoTime()
      val a    = cv.subVector(0, n - 1)
      try
        val b = cv.subVector(1, n)
        try
          val le = a.binaryOp(ai.rapids.cudf.BinaryOp.LESS_EQUAL, b, DType.BOOL8)
          try
            val all = le.all()
            try
              val sorted = all.isValid && all.getBoolean
              debug(f"Sortedness check: ${sorted} in ${(System.nanoTime() - nano) / 1e6}%.1f ms")
              sorted
            finally
              all.close()
          finally
            le.close()
        finally
          b.close()
      finally
        a.close()
    }

end GpuColumnCache

/**
  * The distinct values of a string column, sorted, and the INT32 code of each row: its value's
  * position among them
  */
private[gpu] final class GpuDictionary(val values: ColumnVector, val codes: ColumnVector)
    extends AutoCloseable:
  def sizeBytes: Long        = values.getDeviceMemorySize + codes.getDeviceMemorySize
  override def close(): Unit =
    values.close()
    codes.close()

private[gpu] object GpuDictionary extends LogSupport:
  /** At most this many distinct values */
  val MaxValues = 1 << 15

  /** At most one distinct value per this many rows */
  val MinRowsPerValue = 16

  /** The dictionary of a string column without NULLs and with few distinct values, or None */
  def build(cv: ColumnVector): Option[GpuDictionary] =
    val n = cv.getRowCount
    if cv.getType != DType.STRING || cv.getNullCount != 0 || n < MinRowsPerValue then
      None
    else
      val nano     = System.nanoTime()
      val distinct =
        val t = Table(cv)
        try t.dropDuplicates(Array(0), Table.DuplicateKeepOption.KEEP_ANY, true)
        finally t.close()
      try
        val k = distinct.getRowCount
        if k > MaxValues || k * MinRowsPerValue > n then
          debug(s"No dictionary: ${k} distinct values in ${n} rows")
          None
        else
          val sorted = distinct.orderBy(ai.rapids.cudf.OrderByArg.asc(0))
          try
            val rowsT = Table(cv)
            try
              val codes  = sorted.lowerBound(Array(true), rowsT, Array(false))
              val values = sorted.getColumn(0).incRefCount()
              debug(
                f"Dictionary of ${k} values over ${n}%,d rows in ${(System.nanoTime() - nano) /
                    1e6}%.1f ms"
              )
              Some(GpuDictionary(values, codes))
            finally
              rowsT.close()
          finally
            sorted.close()
      finally
        distinct.close()
    end if

  end build

end GpuDictionary

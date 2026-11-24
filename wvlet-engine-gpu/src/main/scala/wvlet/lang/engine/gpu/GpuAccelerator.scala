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

import ai.rapids.cudf.BinaryOp
import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.DType
import ai.rapids.cudf.DistinctHashJoin
import ai.rapids.cudf.GroupByAggregation
import ai.rapids.cudf.GroupByOptions
import ai.rapids.cudf.HashJoin
import ai.rapids.cudf.NullPolicy
import ai.rapids.cudf.OrderByArg
import ai.rapids.cudf.OutOfBoundsPolicy
import ai.rapids.cudf.Scalar
import ai.rapids.cudf.Table
import wvlet.lang.api.StatusCode
import wvlet.lang.engine.catalog.ParquetFooterCache
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.runtime.*
import wvlet.lang.engine.runtime.RuntimePlan.*
import wvlet.lang.engine.vector.*
import wvlet.uni.log.LogSupport

import scala.util.chaining.*
import scala.util.control.NonFatal

/**
  * Runs the [[RuntimePlan]] subtrees it fully supports on an NVIDIA GPU with cudf: Parquet scans
  * (through a [[GpuColumnCache]] that keeps columns on the device across queries), filters,
  * projections, grouped and global aggregations, hash joins (inner, left, semi, anti), sorts,
  * limits, and DISTINCT. Anything else, and every subtree with an unsupported node or expression,
  * stays on the CPU engine.
  *
  * An example executor: results are copied back to the host once, at the root of each GPU subtree.
  */
class GpuAccelerator(config: GpuConfig = GpuConfig.fromEnv())
    extends PlanAccelerator
    with LogSupport:
  def name: String = "gpu"

  // One cache per JVM, so that accelerators with different settings (A/B runs) share columns
  private def cache: GpuColumnCache = GpuAccelerator.sharedCache(config)

  /** The columns cached on the device */
  def columnCache: GpuColumnCache = cache

  override def accelerate(p: RuntimePlan, cpu: RuntimePlan => Operator): Option[Operator] =
    // A subtree without a join or aggregation returns about as many rows as it reads: copying
    // them back to the host costs more than the CPU's own scan (TPC-H q13 and q22 read o_comment
    // and c_phone this way), so such subtrees stay on the CPU
    // and planning never touches the device: it is initialized when a GPU operator first runs
    if GpuRuntime.isAvailable && GpuPlans.reduces(p) && GpuPlans.supports(p)(using config) &&
      withinLimits(p)
    then
      Some(GpuOp(p, () => GpuPlans(cache, config), () => cpu(p), config))
    else
      None

  /**
    * Whether `p` can run within cudf's limits: with [[GpuFeature.OffloadLimits]], a subtree that
    * would concatenate a string column of a streamed table stays on the CPU
    */
  private def withinLimits(p: RuntimePlan): Boolean =
    given GpuConfig = config
    // Only tables over streamRows rows can have string columns over the limit
    !config.isEnabled(GpuFeature.OffloadLimits) ||
    !GpuPlans.scansOf(p).exists(GpuPlans.largeRows) ||
    !GpuPlans(null, config).concatsLargeStrings(GpuOp.rewrite(p, config))

end GpuAccelerator

object GpuAccelerator:
  private var cache: GpuColumnCache = null

  /** The column cache of this JVM, sized by the config of the first accelerator that runs */
  private[gpu] def sharedCache(config: GpuConfig): GpuColumnCache = synchronized {
    if cache == null then
      val pool = GpuRuntime.init(config)
      cache = GpuColumnCache((pool * config.cacheFraction).toLong, config.pieceBytes)
    cache
  }

/**
  * The device result of a plan node: owned columns, positionally matching the node's output, and
  * what is known of each column: an upper bound of the absolute values of a numeric column (see
  * [[Bounds]]), whether it is non-decreasing without NULLs, and the cached column (file path and
  * column name) whose rows it holds unchanged
  */
private[gpu] final class GTable(
    val cols: IndexedSeq[ColumnVector],
    val rows: Int,
    boundsOrEmpty: IndexedSeq[Option[java.math.BigDecimal]] = IndexedSeq.empty,
    sortedOrEmpty: IndexedSeq[Boolean] = IndexedSeq.empty,
    originsOrEmpty: IndexedSeq[Option[(String, String)]] = IndexedSeq.empty
) extends AutoCloseable:
  val bounds: IndexedSeq[Option[java.math.BigDecimal]] =
    if boundsOrEmpty.isEmpty then
      IndexedSeq.fill(cols.size)(None)
    else
      boundsOrEmpty

  val sorted: IndexedSeq[Boolean] =
    if sortedOrEmpty.isEmpty then
      IndexedSeq.fill(cols.size)(false)
    else
      sortedOrEmpty

  val origins: IndexedSeq[Option[(String, String)]] =
    if originsOrEmpty.isEmpty then
      IndexedSeq.fill(cols.size)(None)
    else
      originsOrEmpty

  def table: Table = Table(cols*)

  /** These columns with the given bounds (a row subset of a table keeps its bounds) */
  def withBounds(b: IndexedSeq[Option[java.math.BigDecimal]]): GTable = GTable(cols, rows, b)

  /** These columns, an order-preserving row subset of `t`: bounds and sortedness carry over */
  def subsetOf(t: GTable): GTable = GTable(cols, rows, t.bounds, t.sorted)

  override def close(): Unit = cols.foreach(_.close())

end GTable

private[gpu] object GTable:
  /** Take the columns of a cudf table (closing the table) */
  def of(t: Table): GTable =
    try GTable(
        (0 until t.getNumberOfColumns).map(i => t.getColumn(i).incRefCount()),
        t.getRowCount.toInt
      )
    finally t.close()

/**
  * A filter that a join passes to one of its inputs: rows whose columns `cols` (cast to `types`)
  * match no row of `values` (the other input's join keys, borrowed) cannot join, so the input may
  * drop them before it does further work. `origin` is the join that made it: within one loop over
  * ranges, filters of the same origin and columns hold the same keys
  */
private[gpu] final case class KeyFilter(
    cols: IndexedSeq[Int],
    types: IndexedSeq[PType],
    values: Table,
    origin: AnyRef
)

/**
  * A plan node and the key filters it runs with, compared by identity: the key of a result or hash
  * table reused across the ranges of a loop
  */
private[gpu] final class RunKey(val p: AnyRef, kfs: List[KeyFilter]):
  private val sig = kfs.map(f => (f.origin, f.cols))

  override def hashCode: Int =
    System.identityHashCode(p) * 31 +
      sig.map((o, c) => System.identityHashCode(o) * 17 + c.hashCode).hashCode

  override def equals(o: Any): Boolean =
    o match
      case k: RunKey =>
        (k.p eq p) && k.sig.size == sig.size &&
        k.sig.zip(sig).forall((a, b) => (a._1 eq b._1) && a._2 == b._2)
      case _ =>
        false

private[gpu] object KeyFilter:
  /** Apply a filter only to inputs with more than this many times its number of keys */
  val MinReduction = 4L

/**
  * How one aggregate call runs in a group-by: its owned input columns (none for count(*)), one cudf
  * aggregation per input, and how the per-group results combine into the call's value
  */
private[gpu] final class AggInput(
    val cols: IndexedSeq[ColumnVector],
    val aggs: IndexedSeq[GroupByAggregation],
    combineResults: IndexedSeq[ColumnVector] => ColumnVector = r => r.head.incRefCount()
) extends AutoCloseable:
  /** The call's value per group, as an owned column, from the borrowed group-by results */
  def combine(results: IndexedSeq[ColumnVector]): ColumnVector = combineResults(results)
  override def close(): Unit                                   = cols.foreach(_.close())

private[gpu] object AggInput:
  def single(col: ColumnVector, c: AggregateCall): AggInput = AggInput(
    IndexedSeq(col),
    IndexedSeq(GpuPlans.aggregation(c))
  )

/**
  * Exact decimal sums without 128-bit accumulators. cudf's hash group-by has no 128-bit atomic add,
  * so a DECIMAL128 sum takes its slower sort-based path for the whole group-by (TPC-H q1: 310 ms
  * against 25 ms). When every value fits in 64 bits, each is split into its high (signed) and low
  * (unsigned) 32 bits, both halves are summed as INT64, which cannot overflow below 2^31 rows, and
  * the sums are combined in 128 bits over the groups.
  */
private[gpu] object DecimalSum:
  import GpuPlans.using

  /**
    * The split inputs of a sum over `arg` (a decimal column at `scale`), or None if a value is
    * wider than 64 bits
    */
  def split(arg: ColumnVector, scale: Int): Option[AggInput] =
    val d64                          = DType.create(DType.DTypeEnum.DECIMAL64, -scale)
    val narrow: Option[ColumnVector] =
      if arg.getType == d64 then
        Some(arg.incRefCount())
      else if fitsLong(arg) then
        Some(arg.castTo(d64))
      else
        None
    narrow.map { n =>
      try
        val v = n.bitCastTo(DType.INT64)
        try
          val hi = using(Scalar.fromInt(32))(k => v.binaryOp(BinaryOp.SHIFT_RIGHT, k, DType.INT64))
          val lo =
            try
              using(Scalar.fromLong(0xffffffffL))(m =>
                v.binaryOp(BinaryOp.BITWISE_AND, m, DType.INT64)
              )
            catch
              case e: Throwable =>
                hi.close()
                throw e
          AggInput(
            IndexedSeq(hi, lo),
            IndexedSeq(GroupByAggregation.sum(), GroupByAggregation.sum()),
            r => combine(r(0), r(1), scale)
          )
        finally
          v.close()
      finally
        n.close()
    }

  end split

  /** Whether `rows` values of at most `bound` sum to an unscaled value within a Long at `scale` */
  def fitsSum(bound: Option[java.math.BigDecimal], scale: Int, rows: Int): Boolean = bound.exists(
    _.movePointRight(scale)
      .multiply(java.math.BigDecimal.valueOf(rows.toLong))
      .compareTo(java.math.BigDecimal.valueOf(Long.MaxValue)) < 0
  )

  /** Whether every value of a DECIMAL128 column has an unscaled value within a Long */
  private def fitsLong(cv: ColumnVector): Boolean =
    def ok(s: Scalar): Boolean =
      try !s.isValid || s.getBigDecimal.unscaledValue().bitLength() < 64
      finally s.close()
    ok(cv.min()) && ok(cv.max())

  /** hi * 2^32 + lo as DECIMAL128 at `scale`, from the INT64 sums of the two halves */
  private def combine(hiSum: ColumnVector, loSum: ColumnVector, scale: Int): ColumnVector =
    val d0 = DType.create(DType.DTypeEnum.DECIMAL128, 0)
    using(hiSum.castTo(d0)) { hi =>
      using(Scalar.fromDecimal(java.math.BigInteger.ONE.shiftLeft(32), d0)) { two32 =>
        using(hi.mul(two32, d0)) { h =>
          using(loSum.castTo(d0)) { lo =>
            using(h.add(lo, d0)) { unscaled =>
              // Reinterpret the unscaled sum at the scale: multiply by 1 at that scale
              val ds = DType.create(DType.DTypeEnum.DECIMAL128, -scale)
              using(Scalar.fromDecimal(java.math.BigInteger.ONE, ds))(one => unscaled.mul(one, ds))
            }
          }
        }
      }
    }

end DecimalSum

/**
  * An aggregation split for streamed input: `partial` aggregates each range, `merge` aggregates the
  * concatenated partial results (keys first, then one column per partial call), and [[finish]]
  * turns the merged columns into the aggregation's output. avg is a sum and a count until
  * [[finish]] divides them, so that the partial sums stay exact.
  */
private[gpu] final class PartialAgg(a: RuntimePlan.Aggregate):
  import GpuPlans.using
  private val nk = a.keys.size

  private def isAvg(c: AggregateCall): Boolean =
    val n = c.name.toLowerCase
    n == "avg" || n == "mean"

  /** The partial calls of each call, with their result types */
  private val parts: IndexedSeq[Seq[(AggregateCall, PType)]] = a
    .calls
    .zip(a.columns.drop(nk))
    .map { (c, col) =>
      if isAvg(c) then
        val sum = c.copy(name = "sum")
        val t   = GpuPlans.aggInputType(sum).get
        Seq((sum.copy(ptype = t), t), (c.copy(name = "count", ptype = PType.Int64), PType.Int64))
      else
        Seq((c, col.ptype))
    }

  private val flat = parts.flatten

  private def columnsOf(types: Seq[PType]): IndexedSeq[Column] =
    a.columns.take(nk) ++ types.map(t => a.columns.head.copy(ptype = t))

  val partial: RuntimePlan.Aggregate = a.copy(
    calls = flat.map(_._1),
    columns = columnsOf(flat.map(_._2))
  )

  val merge: RuntimePlan.Aggregate =
    val calls = flat
      .zipWithIndex
      .map { case ((c, t), i) =>
        val name =
          c.name.toLowerCase match
            case "count" | "count_star" =>
              "sum"
            case other =>
              other
        AggregateCall(name, Seq(BExpr(ColRef(nk + i, t, s"p${i}"))), false, None, t)
      }
    a.copy(
      keys = a.keys.indices.map(i => BExpr(ColRef(i, a.keys(i).ptype, s"k${i}"))),
      calls = calls,
      columns = columnsOf(flat.map(_._2))
    )

  /** The aggregation's output from the merged columns `m` (borrowed) */
  def finish(m: GTable): GTable =
    var at  = nk
    val out =
      GpuPlans.buildAll(a.columns.size) { i =>
        if i < nk then
          m.cols(i).incRefCount()
        else
          val c = a.calls(i - nk)
          val d = GpuTypes.dtype(a.columns(i).ptype)
          if isAvg(c) then
            val v =
              using(m.cols(at).castTo(DType.FLOAT64)) { sum =>
                using(m.cols(at + 1).castTo(DType.FLOAT64)) { n =>
                  // A NULL sum (no non-NULL values) gives NULL
                  sum.div(n)
                }
              }
            at += 2
            if v.getType == d then
              v
            else
              try v.castTo(d)
              finally v.close()
          else
            val v = m.cols(at)
            at += 1
            if v.getType == d then
              v.incRefCount()
            else
              v.castTo(d)
      }
    // Keys and min/max keep their bounds; an avg has none
    var b      = nk
    val bounds =
      m.bounds.take(nk) ++
        parts
          .indices
          .map { k =>
            val r =
              if isAvg(a.calls(k)) then
                None
              else
                m.bounds(b)
            b += parts(k).size
            r
          }
    GTable(out, m.rows, bounds)

  end finish

end PartialAgg

/** Counters of the GPU executor, for tests and logs */
object GpuStats:
  private val ranges = java.util.concurrent.atomic.AtomicLong()

  /** The row ranges that streamed scans have run so far in this JVM */
  def streamedRanges: Long = ranges.get()

  private[gpu] def addRange(): Unit = ranges.incrementAndGet()

/** Plan-time support checks and device execution of runtime plan nodes */
private[gpu] class GpuPlans(cache: GpuColumnCache, config: GpuConfig) extends LogSupport:
  import GpuPlans.*

  private var depth = 0

  /** The columns (by id) that nodes above each plan node read, from [[markNeeded]] */
  private val neededCols = java.util.IdentityHashMap[RuntimePlan, Set[ColumnId]]()

  private given GpuConfig = config

  /** The current row range of each streamed scan, while its ranges run */
  private val bound = java.util.IdentityHashMap[Scan, GTable]()

  /**
    * The results of one loop over the ranges of a streamed scan that are the same for every range:
    * subtrees that read no streamed scan, and the hash tables built over them
    */
  private final class Loop extends AutoCloseable:
    val results = scala.collection.mutable.HashMap.empty[RunKey, GTable]
    val hashes  = scala.collection.mutable.HashMap.empty[RunKey, AutoCloseable]

    /** Whether a reused subtree is running: its own nodes are not kept separately */
    var reusing = false

    override def close(): Unit =
      results.values.foreach(_.close())
      hashes.values.foreach(_.close())

  // The innermost loop first
  private var loops: List[Loop] = Nil

  /** Run a GPU subtree: all of its output is read */
  def run(p: RuntimePlan): GTable =
    markAll(p)
    exec(p)

  /** The scans of the running subtree that stream */
  private var streamed: java.util.Set[Scan] = java.util.Collections.emptySet()

  /** Decide what `p` streams and which columns its nodes read, before it runs */
  private def markAll(p: RuntimePlan): Unit =
    neededCols.clear()
    streamsOf.clear()
    streamed = streamedScans(p)
    markNeeded(p, p.output.map(_.id).toSet)

  /**
    * Whether running `p` would concatenate a string column of a table over `streamRows` rows that a
    * node above reads and whose estimated size ([[GpuPlans.stringBytes]]) exceeds `stringLimit`:
    * the ranges of a streamed scan are concatenated where they stop streaming, unless an
    * aggregation merges them, and such a column would exceed cudf's 2 GB string limit
    */
  def concatsLargeStrings(p: RuntimePlan): Boolean =
    val strings =
      scansOf(p)
        .collect {
          case s @ Scan(src: ParquetSource, _, _, _) if largeRows(s) =>
            // Unknown sizes count as too large
            s.columns.indices.filter(tooLongStrings(s, _)).map(s.columns(_).id)
        }
        .flatten
        .toSet
    strings.nonEmpty && {
      markAll(p)
      def visit(n: RuntimePlan, through: Option[Scan], merged: Boolean): Boolean =
        val s    = streamOf(n)
        val root = s.exists(x => !through.exists(_ eq x))
        val here = root && !merged && Option(neededCols.get(n)).exists(_.exists(strings))
        here || (
          n match
            case Filter(c, _, _) =>
              visit(c, s, false)
            case Project(c, _, _) =>
              visit(c, s, false)
            case j: Join =>
              visit(j.left, s, false) || visit(j.right, s, false)
            case a: Aggregate =>
              visit(a.child, None, mergeable(a))
            case other =>
              other.children.exists(visit(_, None, false))
        )
      visit(p, None, false)
    }

  end concatsLargeStrings

  /**
    * With [[GpuFeature.ReserveMemory]], evict the cached columns that `p` doesn't read until the
    * columns it reads and its estimated working memory fit in 90% of the pool. The estimate is each
    * scan's estimated size ([[GpuPlans.scanBytes]]) times its [[GpuPlans.scanFactors]] for a whole
    * scan, or its size plus four ranges for a streamed scan
    */
  def reserve(p: RuntimePlan): Unit =
    val pool = GpuRuntime.poolSize
    // Subtrees over smaller tables (all of TPC-H SF10) fit as the cache's budget allows
    if pool > 0 && config.isEnabled(GpuFeature.ReserveMemory) && scansOf(p).exists(largeRows) then
      val factors = scanFactors(p)
      val scans   = scansOf(p).collect { case s @ Scan(src: ParquetSource, _, _, _) =>
        (s, src)
      }
      val keep =
        scans
          .flatMap((s, src) => s.columns.indices.map(i => (src.path, s.sourceColumnName(i))))
          .toSet
      val needed =
        scans
          .map { (s, _) =>
            val bytes  = scanBytes(s)
            val factor = Option(factors.get(s)).getOrElse(1)
            if streams(s, factor) then
              val rows = math.max(1L, s.source.estimatedRows.getOrElse(1L))
              bytes + 4L * bytes * math.min(rows, config.chunkRows.toLong) / rows
            else
              factor * bytes
          }
          .sum
      cache.reserve(needed, (pool * 0.9).toLong, keep)

  end reserve

  private def markNeeded(p: RuntimePlan, neededHere: Set[ColumnId]): Unit =
    // A node reached from several parents keeps what any of them reads
    val needed = Option(neededCols.get(p)).fold(neededHere)(_ ++ neededHere)
    neededCols.put(p, needed)
    p match
      case Project(child, exprs, _) =>
        markNeeded(child, exprs.flatMap(_.refs).toSet)
      case Filter(child, cond, _) =>
        markNeeded(child, needed ++ cond.refs)
      case a: Aggregate =>
        markNeeded(
          a.child,
          (a.keys.flatMap(_.refs) ++ a.calls.flatMap(_.exprs.flatMap(_.refs))).toSet
        )
      case Sort(child, keys, _) =>
        markNeeded(child, needed ++ keys.flatMap(_.expr.refs))
      case Limit(child, _, _) =>
        markNeeded(child, needed)
      case j: Join =>
        val more = (j.leftKeys ++ j.rightKeys ++ j.residual).flatMap(_.refs).toSet
        markNeeded(j.left, needed ++ more)
        markNeeded(j.right, needed ++ more)
      case other =>
        // Distinct reads every column
        other.children.foreach(c => markNeeded(c, c.output.map(_.id).toSet))

  /** The value bounds of `t`'s columns, for evaluating expressions over them */
  private def boundsOf(t: GTable): Bounds = Bounds(
    t.bounds,
    config.isEnabled(GpuFeature.DecimalNarrow)
  )

  /**
    * Run `p` on the device. Key filters `kfs` (from joins above) may drop output rows whose keys
    * match none of the filter's values: they are pushed down as far as they stay valid
    */
  def exec(p: RuntimePlan, kfs: List[KeyFilter] = Nil): GTable =
    loops match
      case loop :: _ if !loop.reusing && !containsBound(p) =>
        // The same for every range: run once per loop. Only this subtree's result is kept, not
        // those of its nodes, which run as usual
        val key = RunKey(p, kfs)
        loop.results.get(key) match
          case Some(done) =>
            share(done)
          case None =>
            loop.reusing = true
            val t =
              try execProfiled(p, kfs)
              finally loop.reusing = false
            loop.results(key) = t
            share(t)
      case _ =>
        execProfiled(p, kfs)

  private def execProfiled(p: RuntimePlan, kfs: List[KeyFilter]): GTable =
    if config.profile then
      // Device time of each node, children included (the device is synchronized after each)
      val start = System.nanoTime()
      depth += 1
      val t =
        try execFiltered(p, kfs)
        finally depth -= 1
      ai.rapids.cudf.Cuda.DEFAULT_STREAM.sync()
      info(
        f"${"  " * depth}${p.getClass.getSimpleName}: ${t.rows}%,d rows, ${(
            System.nanoTime() - start
          ) / 1e6}%.1f ms"
      )
      t
    else
      execFiltered(p, kfs)

  private def execFiltered(p: RuntimePlan, kfs: List[KeyFilter]): GTable =
    val here = kfs.filter(f => pushDown(p, f).isEmpty)
    here.foldLeft(execNode(p, kfs))(applyFilter)

  /** Run `body`, and with profiling on, log its device time */
  private inline def timed[A](label: => String)(body: => A): A =
    if config.profile then
      val start = System.nanoTime()
      val a     = body
      ai.rapids.cudf.Cuda.DEFAULT_STREAM.sync()
      info(f"${"  " * (depth + 1)}. ${label}: ${(System.nanoTime() - start) / 1e6}%.1f ms")
      a
    else
      body

  /** Run `p`, passing the key filters it can push down to its children */
  private def execNode(p: RuntimePlan, kfs: List[KeyFilter]): GTable =
    def child0(c: RuntimePlan): GTable = exec(c, childFilters(p, kfs, 0))
    p match
      case s: Scan if bound.containsKey(s) =>
        share(bound.get(s))
      case a: Aggregate if startsStream(a.child) && mergeable(a) =>
        streamAggregate(a, kfs)
      case _ if startsStream(p) =>
        streamConcat(p, kfs)
      case s: Scan =>
        scan(s)
      case Filter(child, cond, _) =>
        using(child0(child))(t => filter(t, cond.lower(positions(child))))
      case Project(child, exprs, columns) =>
        using(child0(child)) { t =>
          val pos      = positions(child)
          given Bounds = boundsOf(t)
          val lowered  = exprs.map(_.lower(pos))
          val out      =
            buildAll(exprs.size)(i =>
              GpuExprs.evalColumnNarrow(lowered(i), columns(i).ptype, t.cols, t.rows)
            )
          // A column passed through unchanged (not cast) keeps its order and origin
          val passed = out
            .indices
            .map { i =>
              lowered(i) match
                case c: ColRef if out(i).getType == t.cols(c.index).getType =>
                  Some(c.index)
                case _ =>
                  None
            }
          GTable(
            out,
            t.rows,
            lowered.map(GpuExprs.bound),
            passed.map(_.exists(t.sorted)),
            passed.map(_.flatMap(t.origins))
          )
        }
      case a @ Aggregate(f: Filter, _, _, _)
          if config.isEnabled(GpuFeature.MaskedFilter) && !distinctCountPath(a) =>
        maskedAggregate(a, f, exec(f.child, childFilters(f, childFilters(p, kfs, 0), 0)))
      case a: Aggregate =>
        aggregate(a, child0(a.child))
      case Sort(child, keys, limit) =>
        using(child0(child))(t => sort(t, child, keys, limit))
      case Limit(child, limit, offset) =>
        using(child0(child)) { t =>
          val from = math.min(offset, t.rows.toLong).toInt
          // limit may be Long.MaxValue (an OFFSET without LIMIT): don't add it to from
          val to = from + math.min(limit, (t.rows - from).toLong).toInt
          GTable(t.cols.map(_.subVector(from, to)), to - from, t.bounds, t.sorted)
        }
      case j: Join =>
        join(j, kfs)
      case Values(columns, rows) =>
        // A constant relation (e.g. a subquery result computed at plan time): upload its rows
        GTable(
          buildAll(columns.size)(i =>
            GpuTypes.fromVec(rows.column(i), columns(i).ptype, rows.size)
          ),
          rows.size
        )
      case Distinct(child) =>
        using(child0(child)) { t =>
          using(t.table) { tbl =>
            GTable
              .of(
                tbl.dropDuplicates(t.cols.indices.toArray, Table.DuplicateKeepOption.KEEP_ANY, true)
              )
              .withBounds(t.bounds)
          }
        }
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Not supported on the GPU: ${other.getClass.getSimpleName}")
    end match

  end execNode

  private def scan(s: Scan): GTable =
    val src = s.source.asInstanceOf[ParquetSource]
    // Read each bound with its column, while that column is surely in the cache
    val bounds  = Array.fill[Option[java.math.BigDecimal]](s.columns.size)(None)
    val sorted  = Array.fill(s.columns.size)(false)
    val origins = Array.fill[Option[(String, String)]](s.columns.size)(None)
    val cols    =
      GpuPlans.buildAll(s.columns.size) { i =>
        val name   = s.sourceColumnName(i)
        val pieces = inPieces(s, i)
        bounds(i) = cache.bound(src.path, name, pieces)
        sorted(i) =
          !pieces && config.isEnabled(GpuFeature.SortedKeys) && cache.isSorted(src.path, name)
        val cv = cache.column(src.path, name, pieces)
        // A column read as cached, without a cast, holds the cached rows unchanged
        if !pieces && cv.getType == GpuTypes.dtype(s.columns(i).ptype) then
          origins(i) = Some((src.path, name))
        // Casts to the declared type keep the values (and their order)
        castOwned(cv, s.columns(i).ptype)
      }
    GTable(
      cols,
      if cols.isEmpty then
        0
      else
        cols.head.getRowCount.toInt
      ,
      bounds.toIndexedSeq,
      sorted.toIndexedSeq,
      origins.toIndexedSeq
    )

  end scan

  /** `cv` (consumed) in the declared type: casts keep the values */
  private def castOwned(cv: ColumnVector, p: PType): ColumnVector =
    val d = GpuTypes.dtype(p)
    if cv.getType == d then
      cv
    else
      try cv.castTo(d)
      finally cv.close()

  /** Whether column i of `s` is cached in pieces ([[GpuFeature.StringPieces]]) */
  private def inPieces(s: Scan, i: Int): Boolean =
    config.isEnabled(GpuFeature.StringPieces) && largeRows(s) && GpuPlans.tooLongStrings(s, i)

  /** Another reference to the columns of `t` */
  private def share(t: GTable): GTable = GTable(t.cols.map(_.incRefCount()), t.rows, t.bounds)

  /** Whether `p` reads a scan whose ranges are running */
  private def containsBound(p: RuntimePlan): Boolean =
    !bound.isEmpty && {
      p match
        case s: Scan =>
          bound.containsKey(s)
        case other =>
          other.children.exists(containsBound)
    }

  private val streamsOf = java.util.IdentityHashMap[RuntimePlan, Option[Scan]]()

  private def streamOf(p: RuntimePlan): Option[Scan] =
    val known = streamsOf.get(p)
    if known != null then
      known
    else
      val s = GpuPlans.streamOf(p, streamed)
      streamsOf.put(p, s)
      s

  /** Whether `p` streams over a scan whose ranges are not running yet: `p` starts the loop */
  private def startsStream(p: RuntimePlan): Boolean = streamOf(p).exists(s => !bound.containsKey(s))

  /**
    * Run `body` once per row range of the streamed scan `s`, which reads that range meanwhile, and
    * return the results. The scan's cached columns are held for the whole loop, so that eviction
    * cannot make a later range read them again
    */
  private def ranges(s: Scan)(body: => GTable): IndexedSeq[GTable] =
    val src  = s.source.asInstanceOf[ParquetSource]
    val cols =
      buildAll(s.columns.size, (c: CachedColumn) => c.close())(i =>
        cache.columnRef(src.path, s.sourceColumnName(i), inPieces(s, i))
      )
    try
      val rows  = cols.headOption.map(_.rows).getOrElse(0L)
      val chunk = config.chunkRows.toLong
      val n     = math.max(1L, (rows + chunk - 1) / chunk).toInt
      val loop  = Loop()
      loops = loop :: loops
      try
        buildAll(n, (t: GTable) => t.close()) { k =>
          val from  = k * chunk
          val to    = math.min(rows, from + chunk)
          val range = GTable(
            buildAll(cols.size)(i => castOwned(cols(i).slice(from, to), s.columns(i).ptype)),
            (to - from).toInt,
            cols.map(_.bound)
          )
          GpuStats.addRange()
          bound.put(s, range)
          try
            timed(f"range ${k + 1} of ${n} of ${s.source.name}, ${to - from}%,d rows")(body)
          finally
            bound.remove(s)
            range.close()
        }
      finally
        loops = loops.tail
        loop.close()
    finally
      cols.foreach(_.close())
    end try

  end ranges

  /**
    * The rows of `parts` (consumed) concatenated. Each column's bound is the largest of the parts'
    * (None if one is unknown)
    */
  private def concat(parts: IndexedSeq[GTable]): GTable =
    if parts.size == 1 then
      parts.head
    else
      try
        val n      = parts.head.cols.size
        val bounds = (0 until n).map(i =>
          val bs = parts.map(_.bounds(i))
          if bs.forall(_.isDefined) then
            bs.flatten.reduceOption(_.max(_))
          else
            None
        )
        if n == 0 then
          GTable(IndexedSeq.empty, parts.map(_.rows).sum)
        else
          val tables = parts.map(_.table)
          try GTable.of(Table.concatenate(tables*)).withBounds(bounds)
          finally tables.foreach(_.close())
      finally
        parts.foreach(_.close())

  /**
    * A streamed subtree whose parent doesn't stream: run it per range and concatenate the results.
    * Columns that no node above reads become all-NULL placeholders first, when pruning is on
    */
  private def streamConcat(p: RuntimePlan, kfs: List[KeyFilter]): GTable =
    val s    = streamOf(p).get
    val keep =
      Option(neededCols.get(p)) match
        case Some(needed) if config.isEnabled(GpuFeature.PruneColumns) =>
          val ids = p.output.map(_.id)
          (i: Int) => needed.contains(ids(i))
        case _ =>
          (_: Int) => true
    concat(ranges(s)(prune(execNode(p, kfs), keep)))

  /** `t` (consumed) with the columns `keep` rejects replaced by all-NULL placeholders */
  private def prune(t: GTable, keep: Int => Boolean): GTable =
    if t.cols.indices.forall(keep) then
      t
    else
      using(t) { in =>
        GTable(
          buildAll(in.cols.size) { i =>
            if keep(i) then
              in.cols(i).incRefCount()
            else
              using(Scalar.fromNull(in.cols(i).getType))(nul =>
                ColumnVector.fromScalar(nul, in.rows)
              )
          },
          in.rows,
          in.bounds
        )
      }

  /**
    * An aggregation over a streamed input: aggregate each range, then merge the partial results
    * (see [[PartialAgg]]); count(distinct) keeps the distinct (keys, argument) rows of each range
    */
  private def streamAggregate(a: Aggregate, kfs: List[KeyFilter]): GTable =
    val s   = streamOf(a.child).get
    val pos = positions(a.child)
    val ckf = childFilters(a, kfs, 0)
    if config.isEnabled(GpuFeature.DistinctCount) && isDistinctCountOnly(a) then
      val parts =
        ranges(s) {
          using(exec(a.child, ckf))(t => distinctRows(a, t, pos))
        }
      using(concat(parts)) { c =>
        using(c.table) { tbl =>
          using(
            GTable.of(
              tbl.dropDuplicates(c.cols.indices.toArray, Table.DuplicateKeepOption.KEEP_ANY, true)
            )
          )(d => countDistinctRows(a, d))
        }
      }
    else
      val split = PartialAgg(a)
      val parts =
        ranges(s) {
          using(exec(a.child, ckf))(t => groupByOver(split.partial, t, pos))
        }
      using(concat(parts)) { c =>
        using(groupByOver(split.merge, c, Array.tabulate(c.cols.size)(identity)))(split.finish)
      }

  end streamAggregate

  private def filter(t: GTable, cond: VExpr): GTable =
    val mask = GpuExprs.evalColumn(cond, PType.Bool, t.cols, t.rows)(using boundsOf(t))
    try filterBy(t, mask)
    finally mask.close()

  /** The rows of `t` where `mask` is true, in order */
  private def filterBy(t: GTable, mask: ColumnVector): GTable =
    using(t.table)(tbl => GTable.of(tbl.filter(mask)).subsetOf(t))

  private def sort(
      t: GTable,
      child: RuntimePlan,
      keys: IndexedSeq[OrderKey],
      limit: Option[Long]
  ): GTable =
    val pos     = positions(child)
    val keyCols =
      buildAll(keys.size)(i =>
        GpuExprs.evalColumn(keys(i).expr.lower(pos), keys(i).expr.ptype, t.cols, t.rows)
      )
    try
      val args = keys
        .zipWithIndex
        .map { (k, i) =>
          // NULLs first for an ascending key means NULLs sort as the smallest value
          val nullSmallest = k.ascending == k.nullsFirst
          if k.ascending then
            OrderByArg.asc(i, nullSmallest)
          else
            OrderByArg.desc(i, nullSmallest)
        }
      val order = using(Table(keyCols*))(_.sortOrder(args*))
      try
        val n   = limit.map(l => math.min(l, t.rows.toLong).toInt).getOrElse(t.rows)
        val map =
          if n == t.rows then
            order.incRefCount()
          else
            order.subVector(0, n)
        try using(t.table)(tbl => GTable.of(tbl.gather(map)).withBounds(t.bounds))
        finally map.close()
      finally
        order.close()
    finally
      keyCols.foreach(_.close())

  end sort

  private def aggregate(a: Aggregate, input: GTable): GTable =
    using(input) { t =>
      if distinctCountPath(a) then
        distinctCount(a, t)
      else
        groupByOver(a, t, positions(a.child))
    }

  private def distinctCountPath(a: Aggregate): Boolean =
    config.isEnabled(GpuFeature.DistinctCount) && isDistinctCountOnly(a)

  /**
    * Aggregate `f`'s rows (the rows of `input`, consumed, that pass its condition). When most rows
    * pass, they are not copied: the condition's mask is one more group key, and the groups of
    * failing rows are dropped from the result (TPC-H q1 keeps 59M of 60M rows)
    */
  private def maskedAggregate(a: Aggregate, f: Filter, input: GTable): GTable =
    using(input) { t =>
      val evaluated =
        GpuExprs.evalColumn(f.cond.lower(positions(f.child)), PType.Bool, t.cols, t.rows)(using
          boundsOf(t)
        )
      // A NULL condition drops the row, as false does
      val mask =
        if evaluated.getNullCount == 0 then
          evaluated
        else
          try using(Scalar.fromBool(false))(evaluated.replaceNulls)
          finally evaluated.close()
      using(mask) { m =>
        val kept =
          using(m.sum(DType.INT64))(s =>
            if s.isValid then
              s.getLong
            else
              0L
          )
        if kept * 2 >= t.rows && kept > 0 then
          groupByOver(a, t, positions(a.child), Some(m))
        else
          aggregate(a, filterBy(t, m))
      }
    }

  /**
    * count(distinct x) grouped by the keys, as the distinct (keys, x) rows and then a count of
    * non-NULL x per group: both steps stay on cudf's hash paths, while `nunique` sorts (TPC-H q16)
    */
  private def distinctCount(a: Aggregate, t: GTable): GTable =
    using(distinctRows(a, t, positions(a.child)))(d => countDistinctRows(a, d))

  /** The distinct (keys, argument) rows of `t` for count(distinct) */
  private def distinctRows(a: Aggregate, t: GTable, pos: Array[Int]): GTable =
    val exprs = a.keys ++ a.calls.take(1).map(_.args.head)
    val cols  =
      buildAll(exprs.size)(i =>
        GpuExprs.evalColumn(exprs(i).lower(pos), exprs(i).ptype, t.cols, t.rows)(using boundsOf(t))
      )
    val distinct =
      try
        timed(f"distinct of ${t.rows}%,d rows") {
          using(Table(cols*)) { tbl =>
            GTable.of(
              tbl.dropDuplicates(cols.indices.toArray, Table.DuplicateKeepOption.KEEP_ANY, true)
            )
          }
        }
      finally cols.foreach(_.close())
    distinct

  /** Count the non-NULL arguments per group of the distinct (keys, argument) rows `d` (borrowed) */
  private def countDistinctRows(a: Aggregate, d: GTable): GTable = locally {
    // Group the distinct rows (keys at 0..n-1, the argument at n) by the keys
    val exprs   = a.keys ++ a.calls.take(1).map(_.args.head)
    val n       = a.keys.size
    val arg     = BExpr(ColRef(n, exprs.last.ptype, "x"))
    val counted = a.copy(
      keys = a.keys.indices.map(i => BExpr(ColRef(i, a.keys(i).ptype, s"k${i}"))),
      calls = a.calls.map(_.copy(distinct = false, args = Seq(arg)))
    )
    groupByOver(counted, d, Array.tabulate(n + 1)(identity))
  }

  /**
    * Group `t`, the rows of `a.child` with the column of id `i` at `pos(i)`, by `a`'s keys. With a
    * `mask` (borrowed), only the rows where it is true are aggregated.
    */
  private def groupByOver(
      a: Aggregate,
      t: GTable,
      pos: Array[Int],
      mask: Option[ColumnVector] = None
  ): GTable = locally {
    given Bounds = boundsOf(t)
    val lowered  = a.keys.map(_.lower(pos))
    // The dictionary values of keys grouped by their codes
    val dictValues = Array.fill[Option[ColumnVector]](a.keys.size)(None)
    val keys       =
      try
        buildAll(a.keys.size) { i =>
          val dict =
            lowered(i) match
              case c: ColRef if config.isEnabled(GpuFeature.DictionaryKeys) =>
                t.origins(c.index).flatMap((path, name) => cache.dictionary(path, name))
              case _ =>
                None
          dict match
            case Some((codes, values)) =>
              dictValues(i) = Some(values)
              codes
            case None =>
              GpuExprs.evalColumn(lowered(i), a.keys(i).ptype, t.cols, t.rows)
        }
      catch
        case e: Throwable =>
          dictValues.flatten.foreach(_.close())
          throw e
    try groupByKeys(a, t, pos, lowered, keys, dictValues.toIndexedSeq, mask)
    finally dictValues.flatten.foreach(_.close())
  }

  /**
    * [[groupByOver]] with the key columns (owned) evaluated: dictionary codes for the keys with
    * `dictValues`, decoded in the result
    */
  private def groupByKeys(
      a: Aggregate,
      t: GTable,
      pos: Array[Int],
      lowered: IndexedSeq[VExpr],
      keys: IndexedSeq[ColumnVector],
      dictValues: IndexedSeq[Option[ColumnVector]],
      mask: Option[ColumnVector]
  )(using Bounds): GTable = locally {
    // The input columns and cudf aggregations of each call
    val inputs =
      try
        timed("aggregate arguments") {
          buildAll(a.calls.size, (in: AggInput) => in.close())(i => aggInput(a.calls(i), pos, t))
        }
      catch
        case e: Throwable =>
          keys.foreach(_.close())
          throw e
    try
      if keys.isEmpty && t.rows == 0 then
        globalOverEmpty(a)
      else
        // A global aggregation groups by one constant key (or only by the mask)
        val constKey =
          if keys.isEmpty && mask.isEmpty then
            val z = Scalar.fromInt(0)
            try Some(ColumnVector.fromScalar(z, t.rows))
            finally z.close()
          else
            None
        try
          val groupCols = keys ++ constKey ++ mask
          val aggs      = Seq.newBuilder[ai.rapids.cudf.GroupByAggregationOnColumn]
          var argIndex  = groupCols.size
          inputs.foreach { in =>
            if in.cols.isEmpty then
              // count(*) counts the rows of any column: the first key
              aggs += in.aggs.head.onColumn(0)
            else
              in.aggs
                .foreach { agg =>
                  aggs += agg.onColumn(argIndex)
                  argIndex += 1
                }
          }
          // A single key stored in order groups with cudf's sort-based path, without hashing
          val sortedKey =
            mask.isEmpty && dictValues.forall(_.isEmpty) && (
              lowered match
                case Seq(c: ColRef) =>
                  t.sorted(c.index) && keys.head.getType == t.cols(c.index).getType
                case _ =>
                  false
            )
          val opts = GroupByOptions
            .builder()
            .withIgnoreNullKeys(false)
            .withKeysSorted(sortedKey)
            .build()
          val grouped =
            timed(
              s"group-by of ${t.rows} rows, ${groupCols.size} key(s)${
                  if sortedKey then
                    ", sorted"
                  else
                    ""
                }"
            ) {
              using(Table((groupCols ++ inputs.flatMap(_.cols))*)) { tbl =>
                GTable.of(tbl.groupBy(opts, groupCols.indices*).aggregate(aggs.result()*))
              }
            }
          // Drop the groups of rows that the mask rejects
          val res =
            mask.fold(grouped) { _ =>
              using(grouped)(g => filterBy(g, g.cols(groupCols.size - 1)))
            }
          if keys.isEmpty && res.rows == 0 then
            res.close()
            return globalOverEmpty(a)
          using(res) { r =>
            val nKeys = keys.size
            // The results of each call, after the keys (and the constant key)
            var at     = groupCols.size
            val values =
              buildAll(inputs.size) { k =>
                val in = inputs(k)
                val n  = in.aggs.size
                val v  = in.combine(r.cols.slice(at, at + n))
                at += n
                v
              }
            // Each output takes over its call's value (or closes it after a cast)
            var taken = 0
            val out   =
              try
                buildAll(a.columns.size) { i =>
                  val d = GpuTypes.dtype(a.columns(i).ptype)
                  if i < nKeys then
                    dictValues(i) match
                      case Some(values) =>
                        // Decode the codes of the result groups
                        using(Table(values))(v =>
                          GTable.of(v.gather(r.cols(i), OutOfBoundsPolicy.DONT_CHECK))
                        ).cols.head
                      case None =>
                        val src = r.cols(i)
                        if src.getType == d then
                          src.incRefCount()
                        else
                          src.castTo(d)
                  else
                    val src = values(i - nKeys)
                    taken = i - nKeys + 1
                    if src.getType == d then
                      src
                    else
                      try src.castTo(d)
                      finally src.close()
                }
              catch
                case e: Throwable =>
                  values.drop(taken).foreach(_.close())
                  throw e
            // Keys and min/max keep the bounds of their input; sums and counts widen
            val outBounds =
              a.keys.map(k => GpuExprs.bound(k.lower(pos))) ++
                a.calls
                  .map { c =>
                    c.name.toLowerCase match
                      case "min" | "max" =>
                        GpuExprs.bound(c.args.head.lower(pos))
                      case _ =>
                        None
                  }
            GTable(out, r.rows, outBounds)
          }
        finally
          constKey.foreach(_.close())
        end try
    finally
      keys.foreach(_.close())
      inputs.foreach(_.close())
    end try
  }

  /** The input columns of an aggregate call, in the type the aggregation accumulates in */
  private def aggInput(c: AggregateCall, pos: Array[Int], t: GTable)(using Bounds): AggInput =
    aggInputType(c) match
      case None =>
        AggInput(IndexedSeq.empty, IndexedSeq(aggregation(c)))
      case Some(d: PType.Decimal) if isSum(c) && config.isEnabled(GpuFeature.DecimalSumSplit) =>
        exactSum(c.args.head.lower(pos), t) match
          case Right(sum) =>
            sum
          case Left(arg) =>
            // A DECIMAL128 sum
            AggInput.single(arg.get, c)
      case Some(PType.Float64) if isAvg(c) && config.isEnabled(GpuFeature.AvgAsSum) =>
        exactSum(c.args.head.lower(pos), t) match
          case Right(sum) =>
            // The count of non-NULL arguments: the sum's first input is NULL where they are
            val n = sum.aggs.size
            AggInput(
              sum.cols :+ sum.cols.head.incRefCount(),
              sum.aggs :+ GroupByAggregation.count(NullPolicy.EXCLUDE),
              r =>
                using(sum.combine(r.take(n))) { total =>
                  using(total.castTo(DType.FLOAT64)) { s =>
                    using(r(n).castTo(DType.FLOAT64))(k => s.div(k, DType.FLOAT64))
                  }
                }
            )
          case Left(Some(arg)) =>
            // The mean of the argument already evaluated
            AggInput.single(
              try arg.castTo(DType.FLOAT64)
              finally arg.close(),
              c
            )
          case Left(None) =>
            AggInput.single(
              GpuExprs.evalColumn(c.args.head.lower(pos), PType.Float64, t.cols, t.rows),
              c
            )
      case Some(p) =>
        AggInput.single(GpuExprs.evalColumn(c.args.head.lower(pos), p, t.cols, t.rows), c)

  /**
    * The inputs of an exact sum of `e` (a decimal or integer) with 64-bit accumulators: one sum, or
    * two halves (see [[DecimalSum]]). Otherwise Left: the owned DECIMAL128 argument when it was
    * evaluated, so that the caller need not evaluate it again.
    */
  private def exactSum(e: VExpr, t: GTable)(using Bounds): Either[Option[ColumnVector], AggInput] =
    e.ptype match
      case src: PType.Decimal =>
        // DECIMAL64 when the argument was computed in 64 bits: no range check needed then
        val arg = GpuExprs.evalColumnNarrow(e, PType.decimal(38, src.scale), t.cols, t.rows)
        try
          if arg.getType.getTypeId == DType.DTypeEnum.DECIMAL64 &&
            DecimalSum.fitsSum(GpuExprs.bound(e), src.scale, t.rows)
          then
            // Even the sum of all rows fits in 64 bits: a plain DECIMAL64 sum is exact
            Right(AggInput(IndexedSeq(arg.incRefCount()), IndexedSeq(GroupByAggregation.sum())))
          else
            DecimalSum.split(arg, src.scale).toRight(Some(arg.incRefCount()))
        finally arg.close()
      case PType.Int32 | PType.Int64 if DecimalSum.fitsSum(GpuExprs.bound(e), 0, t.rows) =>
        Right(
          AggInput(
            IndexedSeq(GpuExprs.evalColumn(e, PType.Int64, t.cols, t.rows)),
            IndexedSeq(GroupByAggregation.sum())
          )
        )
      case _ =>
        Left(None)

  /** A global aggregation of no rows: count is 0, the other aggregates NULL */
  private def globalOverEmpty(a: Aggregate): GTable =
    val cols = a
      .calls
      .zip(a.columns)
      .map { (c, col) =>
        val s =
          if isCount(c) then
            Scalar.fromLong(0L)
          else
            Scalar.fromNull(GpuTypes.dtype(col.ptype))
        try ColumnVector.fromScalar(s, 1)
        finally s.close()
      }
    GTable(cols, 1)

  private def join(j: Join, kfs: List[KeyFilter]): GTable =
    using(exec(j.left, childFilters(j, kfs, 0))) { l =>
      val lpos     = positions(j.left)
      val rpos     = positions(j.right)
      val keyTypes = j.leftKeys.zip(j.rightKeys).map((a, b) => joinKeyType(a.ptype, b.ptype).get)
      val lk       =
        buildAll(keyTypes.size)(i =>
          GpuExprs.evalColumn(j.leftKeys(i).lower(lpos), keyTypes(i), l.cols, l.rows)(using
            boundsOf(l)
          )
        )
      try
        using(Table(lk*)) { lkt =>
          // Right rows whose keys match no left row never appear in the output of these joins:
          // let the right input drop them early (e.g. before aggregating all of lineitem in q17).
          // Only when the left input was reduced (filtered or joined) below its largest table:
          // keys of a whole table, such as all suppliers, rarely drop any rows (q7, q13)
          val own =
            j.kind match
              case JoinKind.Inner | JoinKind.Left | JoinKind.Semi | JoinKind.Anti
                  if config.isEnabled(GpuFeature.JoinKeyFilter) &&
                    l.rows * KeyFilter.MinReduction <= maxSourceRows(j.left) &&
                    // The right input runs once per loop, so its filter can't depend on a range
                    !containsBound(j.left) =>
                val cols = j
                  .rightKeys
                  .map(_.lower(rpos))
                  .collect { case c: ColRef =>
                    c.index
                  }
                Option.when(cols.size == keyTypes.size)(KeyFilter(cols, keyTypes, lkt, j)).toList
              case _ =>
                Nil
          using(exec(j.right, childFilters(j, kfs, 1) ++ own)) { r =>
            val rk =
              buildAll(keyTypes.size)(i =>
                GpuExprs.evalColumn(j.rightKeys(i).lower(rpos), keyTypes(i), r.cols, r.rows)(using
                  boundsOf(r)
                )
              )
            try
              using(Table(rk*)) { rkt =>
                joinKey = RunKey(j, kfs)
                joinTables(j, l, r, lkt, rkt)
              }
            finally rk.foreach(_.close())
          }
        }
      finally lk.foreach(_.close())
      end try
    }

  /** Whether join output column i is read above (or by `extra`), when pruning is on */
  private def keepOf(j: Join, extra: Set[ColumnId] = Set.empty): Int => Boolean =
    Option(neededCols.get(j)) match
      case Some(needed) if config.isEnabled(GpuFeature.PruneColumns) =>
        val ids = j.inputs.map(_.id)
        i => needed.contains(ids(i)) || extra.contains(ids(i))
      case _ =>
        _ => true

  private def joinTables(j: Join, l: GTable, r: GTable, lkt: Table, rkt: Table): GTable =
    val residual = j.residual.map(_.lower(positions(j.inputs)))
    val resRefs  = j.residual.map(_.refs).getOrElse(Set.empty)
    j.kind match
      case JoinKind.Inner =>
        val maps = innerMaps(j, lkt, rkt)
        try
          val joined = gatherBoth(l, r, maps(0), maps(1), nullify = false, keepOf(j, resRefs))
          residual match
            case Some(cond) =>
              using(joined)(t => filter(t, cond))
            case None =>
              joined
        finally
          maps.foreach(_.close())
      case JoinKind.Left if residual.isEmpty =>
        val maps =
          buildSide(j) match
            case Some(1) =>
              lkt.leftJoinGatherMaps(hashOf(j, rkt))
            case _ =>
              lkt.leftJoinGatherMaps(rkt, false)
        try gatherBoth(l, r, maps(0), maps(1), nullify = true, keepOf(j))
        finally maps.foreach(_.close())
      case JoinKind.Left =>
        leftWithResidual(j, l, r, lkt, rkt, residual.get)
      case JoinKind.Semi | JoinKind.Anti if residual.isEmpty && buildSide(j).contains(1) =>
        semiByDistinctHash(j, l, lkt, rkt)
      case JoinKind.Semi | JoinKind.Anti if residual.isEmpty =>
        val map =
          if j.kind == JoinKind.Semi then
            lkt.leftSemiJoinGatherMap(rkt, false)
          else
            lkt.leftAntiJoinGatherMap(rkt, false)
        try
          val view = map.toColumnView(0, map.getRowCount.toInt)
          try using(l.table)(tbl => GTable.of(tbl.gather(view)).withBounds(l.bounds))
          finally view.close()
        finally
          map.close()
      case _ =>
        semiWithResidual(j, l, r, lkt, rkt, residual.get)
    end match

  end joinTables

  /**
    * Which input of `j` is the same for every range of the running loops, so that its hash table is
    * built once per loop: 1 for the right input, 0 for the left (inner joins only), or None
    */
  private def buildSide(j: Join): Option[Int] =
    if loops.isEmpty then
      None
    else
      (containsBound(j.left), containsBound(j.right)) match
        case (true, false) =>
          Some(1)
        case (false, true) if j.kind == JoinKind.Inner =>
          Some(0)
        case _ =>
          None

  /**
    * The join being run and its key filters, set right before [[joinTables]] (which runs no other
    * node): the key of its hash tables built once per loop
    */
  private var joinKey: RunKey = null

  /** The hash table over `keys` (the build input's keys) of `j`, built once per loop */
  private def hashOf(j: Join, keys: Table): HashJoin =
    val loop = loops.head
    loop.hashes.get(joinKey) match
      case Some(h: HashJoin) =>
        h
      case _ =>
        val h = HashJoin(keys, false)
        loop.hashes(joinKey) = h
        h

  /** The inner join pairs (left, right) of `j`'s keys, through a hash table built once per loop */
  private def innerMaps(j: Join, lkt: Table, rkt: Table): Array[ai.rapids.cudf.GatherMap] =
    buildSide(j) match
      case Some(1) =>
        lkt.innerJoinGatherMaps(hashOf(j, rkt))
      case Some(0) =>
        val m = rkt.innerJoinGatherMaps(hashOf(j, lkt))
        Array(m(1), m(0))
      case _ =>
        lkt.innerJoinGatherMaps(rkt, false)

  /**
    * A semi or anti join of a streamed left input: each range looks its keys up in a hash table of
    * the right input's distinct keys, built once per loop
    */
  private def semiByDistinctHash(j: Join, l: GTable, lkt: Table, rkt: Table): GTable =
    val loop = loops.head
    val h    =
      loop.hashes.get(joinKey) match
        case Some(h: DistinctHashJoin) =>
          h
        case _ =>
          val keys = (0 until rkt.getNumberOfColumns).toArray
          val h    =
            using(rkt.dropDuplicates(keys, Table.DuplicateKeepOption.KEEP_ANY, true))(
              DistinctHashJoin(_, false)
            )
          loop.hashes(joinKey) = h
          h
    using(lkt.leftDistinctJoinGatherMap(h)) { map =>
      val view = map.toColumnView(0, map.getRowCount.toInt)
      try
        // Left rows without a match get a negative index (cudf's JoinNoMatch)
        val matched =
          using(Scalar.fromInt(0))(z => view.binaryOp(BinaryOp.GREATER_EQUAL, z, DType.BOOL8))
        val mask =
          try
            if j.kind == JoinKind.Semi then
              matched.incRefCount()
            else
              matched.not()
          finally matched.close()
        try using(l.table)(tbl => GTable.of(tbl.filter(mask)).withBounds(l.bounds))
        finally mask.close()
      finally
        view.close()
    }

  end semiByDistinctHash

  /**
    * Where node `p` passes key filter `f` (on its output columns) down: the child index and the
    * filter on that child's columns, or None to apply it to `p`'s own output
    */
  private def pushDown(p: RuntimePlan, f: KeyFilter): Option[(Int, KeyFilter)] =
    def refs(exprs: IndexedSeq[VExpr]): Option[IndexedSeq[Int]] =
      val idx = f
        .cols
        .map(exprs)
        .collect { case c: ColRef =>
          c.index
        }
      Option.when(idx.size == f.cols.size)(idx)
    p match
      case Filter(_, _, _) | Distinct(_) | Sort(_, _, None) =>
        Some((0, f))
      case Project(child, exprs, _) =>
        val pos = positions(child)
        refs(exprs.map(_.lower(pos))).map(c => (0, f.copy(cols = c)))
      case a: Aggregate if f.cols.forall(_ < a.keys.size) =>
        // Removing child rows by group key removes whole groups
        val pos = positions(a.child)
        refs(a.keys.map(_.lower(pos))).map(c => (0, f.copy(cols = c)))
      case j: Join =>
        val n = j.left.output.size
        j.kind match
          case JoinKind.Inner | JoinKind.Left | JoinKind.Semi | JoinKind.Anti
              if f.cols.forall(_ < n) =>
            // Left rows without a match in the filter produce no output row that passes it
            Some((0, f))
          case JoinKind.Inner if f.cols.forall(_ >= n) =>
            Some((1, f.copy(cols = f.cols.map(_ - n))))
          case _ =>
            None
      case _ =>
        None

  end pushDown

  /** The most rows any table read by `p` has (0 when unknown) */
  private def maxSourceRows(p: RuntimePlan): Long =
    p match
      case s: Scan =>
        s.source.estimatedRows.getOrElse(0L)
      case v: Values =>
        v.rows.size.toLong
      case other =>
        other.children.map(maxSourceRows).maxOption.getOrElse(0L)

  private def childFilters(p: RuntimePlan, kfs: List[KeyFilter], child: Int): List[KeyFilter] = kfs
    .flatMap(f =>
      pushDown(p, f).collect {
        case (c, g) if c == child =>
          g
      }
    )

  /**
    * The rows of `t` (consumed) whose filter columns match a row of the filter's values. Skipped
    * unless `t` has many more rows than the filter: the semi join would not pay for itself
    */
  private def applyFilter(t: GTable, f: KeyFilter): GTable =
    if t.rows <= KeyFilter.MinReduction * f.values.getRowCount then
      t
    else
      using(t) { in =>
        val keys =
          buildAll(f.cols.size) { i =>
            val c = in.cols(f.cols(i))
            val d = GpuTypes.dtype(f.types(i))
            if c.getType == d then
              c.incRefCount()
            else
              c.castTo(d)
          }
        try
          timed(f"key filter of ${in.rows}%,d rows by ${f.values.getRowCount}%,d keys") {
            using(Table(keys*)) { kt =>
              val map = kt.leftSemiJoinGatherMap(f.values, false)
              try
                val view = map.toColumnView(0, map.getRowCount.toInt)
                try using(in.table)(tbl => GTable.of(tbl.gather(view)).withBounds(in.bounds))
                finally view.close()
              finally
                map.close()
            }
          }
        finally keys.foreach(_.close())
      }

  /** The rows of both inputs at matching gather-map positions, side by side */
  private def gatherBoth(
      l: GTable,
      r: GTable,
      lmap: ai.rapids.cudf.GatherMap,
      rmap: ai.rapids.cudf.GatherMap,
      nullify: Boolean,
      keep: Int => Boolean = _ => true
  ): GTable =
    val n  = lmap.getRowCount.toInt
    val lv = lmap.toColumnView(0, n)
    val rv = rmap.toColumnView(0, n)
    try
      val lt     = gatherKept(l, lv, OutOfBoundsPolicy.DONT_CHECK, keep)
      val policy =
        if nullify then
          OutOfBoundsPolicy.NULLIFY
        else
          OutOfBoundsPolicy.DONT_CHECK
      val rt =
        try
          gatherKept(r, rv, policy, i => keep(l.cols.size + i))
        catch
          case e: Throwable =>
            lt.foreach(_.close())
            throw e
      GTable(lt ++ rt, n, l.bounds ++ r.bounds)
    finally
      lv.close()
      rv.close()

  /**
    * The rows of `t` at the positions of `map`. Columns that no node above reads (`keep` false) are
    * not gathered: they become all-NULL placeholders (TPC-H q13 drops `o_comment` this way)
    */
  private def gatherKept(
      t: GTable,
      map: ai.rapids.cudf.ColumnView,
      policy: OutOfBoundsPolicy,
      keep: Int => Boolean
  ): IndexedSeq[ColumnVector] =
    val n    = map.getRowCount.toInt
    val kept = t.cols.indices.filter(keep)
    if kept.size == t.cols.size then
      using(t.table)(tbl => GTable.of(tbl.gather(map, policy))).cols
    else
      val gathered =
        if kept.isEmpty then
          IndexedSeq.empty
        else
          using(Table(kept.map(t.cols)*))(tbl => GTable.of(tbl.gather(map, policy))).cols
      var next = 0
      try
        buildAll(t.cols.size) { i =>
          if keep(i) then
            val c = gathered(next).incRefCount()
            next += 1
            c
          else
            using(Scalar.fromNull(t.cols(i).getType))(nul => ColumnVector.fromScalar(nul, n))
        }
      finally gathered.foreach(_.close())

  /**
    * A left outer join with a residual condition: the inner-joined pairs that pass the residual,
    * then each left row without such a pair, with NULLs for the right columns (TPC-H q13)
    */
  private def leftWithResidual(
      j: Join,
      l: GTable,
      r: GTable,
      lkt: Table,
      rkt: Table,
      residual: VExpr
  ): GTable =
    val maps = innerMaps(j, lkt, rkt)
    try
      val n         = maps(0).getRowCount.toInt
      val lv        = maps(0).toColumnView(0, n)
      val rightOnly = config.isEnabled(GpuFeature.PruneColumns) && rightOnlyResidual(j)
      // A residual over right columns only is evaluated once per right row, and its result
      // gathered: the pairs then need not carry the columns it reads (q13's o_comment)
      val keep = keepOf(
        j,
        if rightOnly then
          Set.empty
        else
          j.residual.get.refs
      )
      try
        using(gatherBoth(l, r, maps(0), maps(1), nullify = false, keep)) { pairs =>
          val pass =
            if rightOnly then
              val onRight = j.residual.get.lower(positions(j.right))
              using(GpuExprs.evalColumn(onRight, PType.Bool, r.cols, r.rows)(using boundsOf(r))) {
                mask =>
                  val rv = maps(1).toColumnView(0, n)
                  try
                    using(Table(mask))(t => GTable.of(t.gather(rv, OutOfBoundsPolicy.DONT_CHECK)))
                      .cols
                      .head
                  finally rv.close()
              }
            else
              GpuExprs.evalColumn(residual, PType.Bool, pairs.cols, pairs.rows)(using
                boundsOf(pairs)
              )
          try
            // A NULL residual does not pass
            val f    = Scalar.fromBool(false)
            val keep =
              try pass.replaceNulls(f)
              finally f.close()
            try using(pairs.table)(t => GTable.of(t.filter(keep))).pipe { passing =>
                using(passing) { pt =>
                  val unmatched = leftRowsWithout(l, lv, keep)
                  using(unmatched) { u =>
                    val padded = GTable(
                      buildAll(u.cols.size + r.cols.size) { i =>
                        if i < u.cols.size then
                          u.cols(i).incRefCount()
                        else
                          using(Scalar.fromNull(r.cols(i - u.cols.size).getType))(nul =>
                            ColumnVector.fromScalar(nul, u.rows)
                          )
                      },
                      u.rows
                    )
                    using(padded) { pd =>
                      using(pt.table) { a =>
                        using(pd.table)(b =>
                          GTable.of(Table.concatenate(a, b)).withBounds(l.bounds ++ r.bounds)
                        )
                      }
                    }
                  }
                }
              }
            finally keep.close()
          finally
            pass.close()
          end try
        }
      finally lv.close()
      end try
    finally
      maps.foreach(_.close())
    end try

  end leftWithResidual

  /** Whether the join's residual reads only columns of its right input */
  private def rightOnlyResidual(j: Join): Boolean = j
    .residual
    .exists(_.refs.subsetOf(j.right.output.map(_.id).toSet))

  /** The rows of `l` whose index is not among the left indices `lv` of pairs where `keep` holds */
  private def leftRowsWithout(
      l: GTable,
      lv: ai.rapids.cudf.ColumnView,
      keep: ColumnVector
  ): GTable =
    val matched =
      using(lv.copyToColumnVector()) { lc =>
        using(Table(lc))(lt => GTable.of(lt.filter(keep))).cols.head
      }
    try
      val f    = Scalar.fromBool(false)
      val none =
        try ColumnVector.fromScalar(f, l.rows)
        finally f.close()
      val flags =
        try
          using(Scalar.fromBool(true)) { t =>
            using(Table(none))(target => GTable.of(Table.scatter(Array(t), matched, target)))
          }
        finally none.close()
      using(flags) { fl =>
        using(fl.cols.head.not()) { mask =>
          using(l.table)(tbl => GTable.of(tbl.filter(mask)).withBounds(l.bounds))
        }
      }
    finally
      matched.close()

  /**
    * A semi or anti join with a residual condition: inner-join the keys, keep the pairs that pass
    * the residual, then the left rows with (semi) or without (anti) such a pair
    */
  private def semiWithResidual(
      j: Join,
      l: GTable,
      r: GTable,
      lkt: Table,
      rkt: Table,
      residual: VExpr
  ): GTable =
    val maps = innerMaps(j, lkt, rkt)
    try
      val n  = maps(0).getRowCount.toInt
      val lv = maps(0).toColumnView(0, n)
      try
        val matched: ColumnVector =
          using(
            gatherBoth(
              l,
              r,
              maps(0),
              maps(1),
              nullify = false,
              if config.isEnabled(GpuFeature.PruneColumns) then
                val ids  = j.inputs.map(_.id)
                val refs = j.residual.map(_.refs).getOrElse(Set.empty)
                i => refs.contains(ids(i))
              else
                _ => true
            )
          ) { pairs =>
            val pass =
              GpuExprs.evalColumn(residual, PType.Bool, pairs.cols, pairs.rows)(using
                boundsOf(pairs)
              )
            try
              // The left row of each passing pair
              using(lv.copyToColumnVector()) { lc =>
                using(Table(lc))(lt => GTable.of(lt.filter(pass))).cols.head
              }
            finally pass.close()
          }
        try
          // Mark the left rows that have a passing pair
          val f    = Scalar.fromBool(false)
          val none =
            try ColumnVector.fromScalar(f, l.rows)
            finally f.close()
          val flags =
            try
              val t = Scalar.fromBool(true)
              try using(Table(none))(target => GTable.of(Table.scatter(Array(t), matched, target)))
              finally t.close()
            finally
              none.close()
          using(flags) { fl =>
            val mask =
              if j.kind == JoinKind.Semi then
                fl.cols.head.incRefCount()
              else
                fl.cols.head.not()
            try using(l.table)(tbl => GTable.of(tbl.filter(mask)).withBounds(l.bounds))
            finally mask.close()
          }
        finally
          matched.close()
      finally
        lv.close()
      end try
    finally
      maps.foreach(_.close())
    end try

  end semiWithResidual

end GpuPlans

private[gpu] object GpuPlans:
  def apply(cache: GpuColumnCache, config: GpuConfig): GpuPlans = new GpuPlans(cache, config)

  inline def using[A <: AutoCloseable, B](a: A)(f: A => B): B =
    try f(a)
    finally a.close()

  def positions(p: RuntimePlan): Array[Int]          = Lowering.positions(p)
  def positions(out: IndexedSeq[Column]): Array[Int] = Lowering.positions(out)

  /**
    * `f(0)`, ..., `f(n - 1)`: when one throws, the results built so far are closed (device columns
    * hold memory, and cached ones a reference that would keep them from being evicted)
    */
  def buildAll(n: Int)(f: Int => ColumnVector): IndexedSeq[ColumnVector] =
    buildAll(n, (c: ColumnVector) => c.close())(f)

  def buildAll[A](n: Int, close: A => Unit)(f: Int => A): IndexedSeq[A] =
    val built = IndexedSeq.newBuilder[A]
    var done  = List.empty[A]
    try
      (0 until n).foreach { i =>
        val a = f(i)
        done = a :: done
        built += a
      }
      built.result()
    catch
      case e: Throwable =>
        done.foreach(close)
        throw e

  private def supported(e: BExpr, input: IndexedSeq[Column])(using GpuConfig): Boolean = GpuExprs
    .supports(e.lower(positions(input)))

  private def supportedTypes(p: RuntimePlan): Boolean = p
    .output
    .forall(c => GpuTypes.dtypeOf(c.ptype).isDefined)

  /** The scans of `p` */
  def scansOf(p: RuntimePlan): Seq[Scan] =
    p match
      case s: Scan =>
        Seq(s)
      case other =>
        other.children.flatMap(scansOf)

  private val stringSizes = java
    .util
    .concurrent
    .ConcurrentHashMap[(String, String, Long, Long), Option[Long]]()

  /**
    * The estimated characters of a string column of a Parquet file, from its footer: the
    * uncompressed size of plain-encoded column chunks, and the rows times the mean length of the
    * min and max values of dictionary-encoded ones (whose pages hold only the distinct values).
    * None when a dictionary-encoded chunk has no statistics
    */
  def stringBytes(path: String, column: String): Option[Long] =
    val f   = java.io.File(path)
    val key = (f.getCanonicalPath, column, f.length(), f.lastModified())
    stringSizes.computeIfAbsent(
      key,
      _ =>
        try
          val pf = ParquetFooterCache.open(path).file
          try
            val chunks = pf
              .rowGroups
              .flatMap(_.columns.find(_.path == Seq(column)))
              .map { c =>
                if c.encodings.exists(_.toString.contains("DICTIONARY")) then
                  c.statistics
                    .flatMap(st => st.min.zip(st.max))
                    .map((lo, hi) => c.numValues * (lo.length + hi.length) / 2)
                else
                  Some(c.totalUncompressedSize)
              }
            Option.when(chunks.nonEmpty && chunks.forall(_.isDefined))(chunks.flatten.sum)
          finally
            pf.close()
        catch
          case NonFatal(_) =>
            None
    )

  /** Whether `s` reads a table of more than `streamRows` rows */
  def largeRows(s: Scan)(using cfg: GpuConfig): Boolean = s
    .source
    .estimatedRows
    .exists(_ > cfg.streamRows)

  /**
    * Whether `s` runs in row ranges ([[GpuFeature.ChunkedScans]]): a table over `streamRows` rows
    * with a string column too large to concatenate, or whose whole scan, `factor` times its
    * estimated size ([[scanFactors]]), would take more than `streamFraction` of the pool. Before
    * the pool exists (planning before the first GPU run), every such table counts as streaming
    */
  def streams(s: Scan, factor: Int)(using cfg: GpuConfig): Boolean =
    cfg.isEnabled(GpuFeature.ChunkedScans) && largeRows(s) && {
      val pool = GpuRuntime.poolSize
      needsPieces(s) || pool == 0 || factor * scanBytes(s) > cfg.streamFraction * pool
    }

  /**
    * How many times its estimated size each scan of `p` takes on the device when it runs whole: 1,
    * plus 1 for a filter between it and the next join or aggregation above (which copies the rows
    * it keeps), plus 1 when that is an aggregation (whose arguments are as long as the scan). TPC-H
    * q1 (scan, filter, aggregate) gets 3; q9's lineitem, which a join reduces first, gets 1
    */
  def scanFactors(p: RuntimePlan): java.util.IdentityHashMap[Scan, Int] =
    val out = java.util.IdentityHashMap[Scan, Int]()
    def visit(n: RuntimePlan, filtered: Boolean, aggregated: Boolean): Unit =
      n match
        case s: Scan =>
          val f =
            1 + (
              if filtered then
                1
              else
                0
            ) + (
              if aggregated then
                1
              else
                0
            )
          out.put(s, math.max(f, Option(out.get(s)).getOrElse(0)))
        case a: Aggregate =>
          visit(a.child, false, true)
        case Filter(child, _, _) =>
          visit(child, true, aggregated)
        case j: Join =>
          j.children.foreach(visit(_, false, false))
        case other =>
          other.children.foreach(visit(_, filtered, aggregated))
    visit(p, false, false)
    out

  /** The scans of `p` that stream ([[streams]] with their [[scanFactors]]) */
  def streamedScans(p: RuntimePlan)(using GpuConfig): java.util.Set[Scan] =
    val out = java
      .util
      .Collections
      .newSetFromMap(java.util.IdentityHashMap[Scan, java.lang.Boolean]())
    scanFactors(p).forEach((s, f) =>
      if streams(s, f) then
        out.add(s)
    )
    out

  /** Whether a string column of `s` is estimated to exceed `stringLimit` */
  def needsPieces(s: Scan)(using GpuConfig): Boolean = s
    .columns
    .indices
    .exists(i => tooLongStrings(s, i))

  /** Whether column i of `s` is a string column estimated to exceed `stringLimit` */
  def tooLongStrings(s: Scan, i: Int)(using cfg: GpuConfig): Boolean =
    s.columns(i).ptype == PType.Str && {
      s.source match
        case src: ParquetSource =>
          stringBytes(src.path, s.sourceColumnName(i)).forall(_ > cfg.stringLimit)
        case _ =>
          false
    }

  /**
    * The estimated device size of the columns `s` reads: rows times the column widths, and for a
    * string column its estimated characters ([[stringBytes]]) plus 4-byte offsets
    */
  def scanBytes(s: Scan): Long =
    val rows = s.source.estimatedRows.getOrElse(0L)
    s.columns
      .indices
      .map { i =>
        val p = s.columns(i).ptype
        (s.source, p) match
          case (src: ParquetSource, PType.Str) =>
            stringBytes(src.path, s.sourceColumnName(i)).getOrElse(rows * 12) + rows * 4
          case _ =>
            rows * widthOf(p)
      }
      .sum

  /**
    * The streamed scan whose ranges `p` runs over, one range at a time: through filters,
    * projections, and the probe side of joins (either input of an inner join, the left input of the
    * others). When both inputs of an inner join stream, the one with more rows does, and the other
    * runs once and concatenates its own ranges
    */
  def streamOf(p: RuntimePlan, streamed: java.util.Set[Scan]): Option[Scan] =
    p match
      case s: Scan =>
        Option.when(streamed.contains(s))(s)
      case Filter(child, _, _) =>
        streamOf(child, streamed)
      case Project(child, _, _) =>
        streamOf(child, streamed)
      case j: Join =>
        j.kind match
          case JoinKind.Inner =>
            (streamOf(j.left, streamed), streamOf(j.right, streamed)) match
              case (Some(a), Some(b)) =>
                def rows(s: Scan) = s.source.estimatedRows.getOrElse(0L)
                if rows(b) > rows(a) then
                  Some(b)
                else
                  Some(a)
              case (a, b) =>
                a.orElse(b)
          case JoinKind.Left | JoinKind.Semi | JoinKind.Anti =>
            streamOf(j.left, streamed)
          case _ =>
            None
      case _ =>
        None

  /** Whether every call of `a` is count(distinct x) of the same single argument */
  def isDistinctCountOnly(a: Aggregate): Boolean =
    a.calls.nonEmpty &&
      a.calls.forall(c => isCount(c) && c.distinct && c.args.size == 1 && c.filter.isEmpty) &&
      a.calls.map(_.args.head).distinct.size == 1

  /** Whether `a` can aggregate ranges separately and merge the results ([[PartialAgg]]) */
  def mergeable(a: Aggregate)(using cfg: GpuConfig): Boolean =
    (cfg.isEnabled(GpuFeature.DistinctCount) && isDistinctCountOnly(a)) ||
      a.calls
        .forall(c =>
          !c.distinct &&
            Set("sum", "count", "count_star", "min", "max", "avg", "mean").contains(
              c.name.toLowerCase
            )
        )

  /** Bytes per row of a column of type `p` on the device, counting 16 for a string */
  def widthOf(p: PType): Long =
    if p == PType.Str then
      16L
    else
      GpuTypes.dtypeOf(p).map(d => math.max(1, d.getSizeInBytes).toLong).getOrElse(8L)

  /**
    * Move the conjuncts of each left join's residual that read only right columns into a filter
    * over its right input, when that input streams ([[GpuFeature.StreamResidual]]): a right row
    * failing them joins no left row, so the join's output is the same, and the filter runs per
    * range (TPC-H q13's `o_comment NOT LIKE` at SF100)
    */
  def streamResiduals(p: RuntimePlan, streamed: java.util.Set[Scan]): RuntimePlan =
    val q = p.withChildren(p.children.map(streamResiduals(_, streamed)))
    q match
      case j: Join
          if j.kind == JoinKind.Left && j.residual.isDefined &&
            streamOf(j.right, streamed).isDefined =>
        val rightIds      = j.right.output.map(_.id).toSet
        val (moved, kept) = conjuncts(j.residual.get.expr).partition { e =>
          val refs = BExpr(e).refs
          refs.nonEmpty && refs.subsetOf(rightIds)
        }
        if moved.isEmpty then
          j
        else
          j.copy(
            right = Filter(j.right, BExpr(moved.reduce(AndExpr(_, _)))),
            residual = kept.reduceOption(AndExpr(_, _)).map(BExpr(_))
          )
      case other =>
        other

  /** Whether `p` joins or aggregates, which the GPU speeds up and which shrink its output */
  def reduces(p: RuntimePlan): Boolean =
    p match
      case _: Join | _: Aggregate =>
        true
      case other =>
        other.children.exists(reduces)

  /** Whether all of `p` runs on the GPU */
  def supports(p: RuntimePlan)(using cfg: GpuConfig): Boolean =
    supportedTypes(p) && {
      p match
        case Values(columns, _) =>
          columns.nonEmpty && cfg.isEnabled(GpuFeature.UploadValues)
        case Scan(src: ParquetSource, columns, _, _) =>
          src.isSingleFile && columns.nonEmpty
        case Filter(child, cond, _) =>
          supports(child) && supported(cond, child.output)
        case Project(child, exprs, _) =>
          exprs.nonEmpty && supports(child) && exprs.forall(supported(_, child.output))
        case a: Aggregate =>
          a.columns.nonEmpty && supports(a.child) && a.child.output.nonEmpty &&
          a.keys.forall(supported(_, a.child.output)) &&
          a.calls.forall(c => aggSupported(c) && c.args.forall(supported(_, a.child.output)))
        case Sort(child, keys, _) =>
          supports(child) && keys.forall(k => supported(k.expr, child.output))
        case Limit(child, _, _) =>
          supports(child)
        case j: Join =>
          val kindOk =
            j.kind match
              case JoinKind.Inner | JoinKind.Semi | JoinKind.Anti =>
                true
              case JoinKind.Left =>
                j.residual.isEmpty || cfg.isEnabled(GpuFeature.OuterResidual)
              case _ =>
                false
          kindOk && j.leftKeys.nonEmpty && supports(j.left) && supports(j.right) &&
          j.leftKeys.forall(supported(_, j.left.output)) &&
          j.rightKeys.forall(supported(_, j.right.output)) &&
          j.leftKeys.zip(j.rightKeys).forall((a, b) => joinKeyType(a.ptype, b.ptype).isDefined) &&
          j.residual.forall(supported(_, j.inputs))
        case Distinct(child) =>
          supports(child)
        case _ =>
          false
    }

  /**
    * Push the single-input predicates that a filter's OR conditions imply below the inner joins
    * under it, keeping the filter itself. From `(n1 = 'FRANCE' and n2 = 'GERMANY') or (n1 =
    * 'GERMANY' and n2 = 'FRANCE')` above the joins (TPC-H q7), each nation scan gets `n = 'FRANCE'
    * or n = 'GERMANY'`, and the joins (and their key filters) then start from 2 of 25 nations.
    */
  def impliedFilters(p: RuntimePlan): RuntimePlan =
    val q = p.withChildren(p.children.map(impliedFilters))
    q match
      case f @ Filter(j: Join, cond, _) =>
        val ors = conjuncts(cond.expr).collect { case o: OrExpr =>
          o
        }
        if ors.isEmpty then
          f
        else
          f.copy(child = ors.foldLeft(j: RuntimePlan)((acc, o) => pushImplied(acc, o)))
      case other =>
        other

  /** Add the predicates `pred` implies on each input of the inner joins at the top of `p` */
  private def pushImplied(p: RuntimePlan, pred: VExpr): RuntimePlan =
    p match
      case j: Join if j.kind == JoinKind.Inner =>
        def side(input: RuntimePlan): RuntimePlan =
          implied(pred, input.output.map(_.id).toSet) match
            case Some(e) if input.isInstanceOf[Join] =>
              pushImplied(input, e)
            case Some(e) =>
              Filter(input, BExpr(e))
            case None =>
              input
        j.copy(left = side(j.left), right = side(j.right))
      case other =>
        other

  /**
    * A predicate over only the columns `ids` that `e` implies (by dropping conjuncts), or None
    */
  private def implied(e: VExpr, ids: Set[ColumnId]): Option[VExpr] =
    val refs = BExpr(e).refs
    if refs.nonEmpty && refs.subsetOf(ids) then
      Some(e)
    else
      e match
        case OrExpr(l, r) =>
          // Both branches must imply something, or the disjunction implies nothing
          implied(l, ids).zip(implied(r, ids)).map(OrExpr(_, _))
        case AndExpr(l, r) =>
          (implied(l, ids), implied(r, ids)) match
            case (Some(a), Some(b)) =>
              Some(AndExpr(a, b))
            case (a, b) =>
              a.orElse(b)
        case _ =>
          None

  private def conjuncts(e: VExpr): Seq[VExpr] =
    e match
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        Seq(other)

  /** The type both keys of a join pair are compared in, or None */
  def joinKeyType(l: PType, r: PType): Option[PType] =
    (l, r) match
      case _ if l == r =>
        Some(l)
      case (PType.Int32 | PType.Int64, PType.Int32 | PType.Int64) =>
        Some(PType.Int64)
      case _ =>
        None

  def isSum(c: AggregateCall): Boolean = c.name.toLowerCase == "sum" && !c.distinct

  private def isAvg(c: AggregateCall): Boolean =
    val n = c.name.toLowerCase
    (n == "avg" || n == "mean") && !c.distinct

  private def isCount(c: AggregateCall): Boolean =
    val n = c.name.toLowerCase
    n == "count" || n == "count_star"

  private def aggSupported(c: AggregateCall): Boolean =
    c.filter.isEmpty && {
      c.name.toLowerCase match
        case "count_star" =>
          !c.distinct
        case "count" =>
          c.args.size <= 1
        case "sum" | "avg" | "mean" | "min" | "max" =>
          !c.distinct && c.args.size == 1 && aggInputType(c).isDefined
        case _ =>
          false
    }

  /** The type the argument of `c` is cast to before aggregating it (None: no argument) */
  def aggInputType(c: AggregateCall): Option[PType] = c
    .args
    .headOption
    .flatMap { a =>
      c.name.toLowerCase match
        case "sum" =>
          a.ptype match
            case PType.Int32 | PType.Int64 =>
              Some(PType.Int64)
            case PType.Float64 =>
              Some(PType.Float64)
            case d: PType.Decimal =>
              // Sum exactly in 128 bits, as the CPU's decimal(38, s) result
              Some(PType.decimal(38, d.scale))
            case _ =>
              None
        case "avg" | "mean" =>
          if a.ptype.isNumeric then
            Some(PType.Float64)
          else
            None
        case _ =>
          Some(a.ptype)
    }

  def aggregation(c: AggregateCall): GroupByAggregation =
    c.name.toLowerCase match
      case "count_star" =>
        GroupByAggregation.count(NullPolicy.INCLUDE)
      case "count" if c.args.isEmpty =>
        GroupByAggregation.count(NullPolicy.INCLUDE)
      case "count" if c.distinct =>
        GroupByAggregation.nunique(NullPolicy.EXCLUDE)
      case "count" =>
        GroupByAggregation.count(NullPolicy.EXCLUDE)
      case "sum" =>
        GroupByAggregation.sum()
      case "avg" | "mean" =>
        GroupByAggregation.mean()
      case "min" =>
        GroupByAggregation.min()
      case _ =>
        GroupByAggregation.max()

end GpuPlans

/**
  * Runs a supported plan subtree on the GPU when first read, and returns its rows from the host. If
  * the device fails at run time (e.g. out of memory), the subtree runs on the CPU instead.
  */
class GpuOp(plan: RuntimePlan, plans: () => GpuPlans, cpu: () => Operator, config: GpuConfig)
    extends Operator
    with LogSupport:
  override val schema: Schema = plan.schema

  private var batches: Iterator[Batch] = null
  private var fallback: Operator       = null

  override def name: String = "Gpu"

  override protected def details: String = s"[${GpuOp.describe(plan)}]"

  override def next(): Batch =
    if fallback != null then
      fallback.next()
    else
      if batches == null then
        try
          batches = run()
        catch
          case e: Throwable if NonFatal(e) || e.isInstanceOf[OutOfMemoryError] =>
            warn(s"GPU execution failed, running on the CPU instead: ${e.getMessage}")
            fallback = cpu()
            return fallback.next()
      if batches.hasNext then
        batches.next()
      else
        null

  private def run(): Iterator[Batch] =
    val nano = System.nanoTime()
    val p    = GpuOp.rewrite(plan, config)
    val gp   = plans()
    gp.reserve(p)
    val t = gp.run(p)
    try
      val types = schema.types
      val bulk  = config.isEnabled(GpuFeature.BulkCopy)
      val vecs  = t.cols.indices.map(i => GpuTypes.toVec(t.cols(i), types(i), bulk)).toArray
      debug(f"GPU subtree: ${t.rows}%,d rows in ${(System.nanoTime() - nano) / 1e6}%.1f ms")
      if t.rows == 0 then
        Iterator.empty
      else
        Iterator(Batch(vecs, t.rows))
    finally
      t.close()

  override def close(): Unit =
    if fallback != null then
      fallback.close()

end GpuOp

object GpuOp:
  /** The GPU module's own rewrites of a subtree before it runs (see [[GpuFeature]]) */
  def rewrite(plan: RuntimePlan, config: GpuConfig): RuntimePlan =
    given GpuConfig = config
    val p           =
      if config.isEnabled(GpuFeature.ImpliedFilters) then
        GpuPlans.impliedFilters(plan)
      else
        plan
    if config.isEnabled(GpuFeature.StreamResidual) then
      GpuPlans.streamResiduals(p, GpuPlans.streamedScans(p))
    else
      p

  /** The nodes of a GPU subtree in flow order, e.g. `Scan(lineitem) -> Filter -> Aggregate` */
  def describe(p: RuntimePlan): String =
    p match
      case s: Scan =>
        s"Scan(${s.source.name})"
      case j: Join =>
        s"Join[${j.kind}](${describe(j.left)}, ${describe(j.right)})"
      case other =>
        val name = other.getClass.getSimpleName
        other.children match
          case Seq(c) =>
            s"${describe(c)} -> ${name}"
          case cs =>
            s"${name}(${cs.map(describe).mkString(", ")})"

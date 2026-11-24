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
import ai.rapids.cudf.GroupByAggregation
import ai.rapids.cudf.GroupByOptions
import ai.rapids.cudf.NullPolicy
import ai.rapids.cudf.OrderByArg
import ai.rapids.cudf.OutOfBoundsPolicy
import ai.rapids.cudf.Scalar
import ai.rapids.cudf.Table
import wvlet.lang.api.StatusCode
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
    if GpuRuntime.isAvailable && GpuPlans.reduces(p) && GpuPlans.supports(p)(using config) then
      Some(GpuOp(p, () => GpuPlans(cache, config), () => cpu(p), config))
    else
      None

end GpuAccelerator

object GpuAccelerator:
  private var cache: GpuColumnCache = null

  /** The column cache of this JVM, sized by the config of the first accelerator that runs */
  private[gpu] def sharedCache(config: GpuConfig): GpuColumnCache = synchronized {
    if cache == null then
      val pool = GpuRuntime.init(config)
      cache = GpuColumnCache((pool * config.cacheFraction).toLong)
    cache
  }

/**
  * The device result of a plan node: owned columns, positionally matching the node's output, and an
  * upper bound of the absolute values of each numeric column where known (see [[Bounds]])
  */
private[gpu] final class GTable(
    val cols: IndexedSeq[ColumnVector],
    val rows: Int,
    boundsOrEmpty: IndexedSeq[Option[java.math.BigDecimal]] = IndexedSeq.empty
) extends AutoCloseable:
  val bounds: IndexedSeq[Option[java.math.BigDecimal]] =
    if boundsOrEmpty.isEmpty then
      IndexedSeq.fill(cols.size)(None)
    else
      boundsOrEmpty

  def table: Table = Table(cols*)

  /** These columns with the given bounds (a row subset of a table keeps its bounds) */
  def withBounds(b: IndexedSeq[Option[java.math.BigDecimal]]): GTable = GTable(cols, rows, b)

  override def close(): Unit = cols.foreach(_.close())

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
  * drop them before it does further work
  */
private[gpu] final case class KeyFilter(
    cols: IndexedSeq[Int],
    types: IndexedSeq[PType],
    values: Table
)

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

/** Plan-time support checks and device execution of runtime plan nodes */
private[gpu] class GpuPlans(cache: GpuColumnCache, config: GpuConfig) extends LogSupport:
  import GpuPlans.*

  private var depth = 0

  /** The columns (by id) that nodes above each plan node read, from [[prepare]] */
  private val neededCols = java.util.IdentityHashMap[RuntimePlan, Set[ColumnId]]()

  /** Run a GPU subtree: all of its output is read */
  def run(p: RuntimePlan): GTable =
    neededCols.clear()
    markNeeded(p, p.output.map(_.id).toSet)
    exec(p)

  private def markNeeded(p: RuntimePlan, needed: Set[ColumnId]): Unit =
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
      case s: Scan =>
        scan(s)
      case Filter(child, cond, _) =>
        using(child0(child))(t => filter(t, cond.lower(positions(child))))
      case Project(child, exprs, columns) =>
        using(child0(child)) { t =>
          val pos      = positions(child)
          given Bounds = boundsOf(t)
          val lowered  = exprs.map(_.lower(pos))
          GTable(
            buildAll(exprs.size)(i =>
              GpuExprs.evalColumnNarrow(lowered(i), columns(i).ptype, t.cols, t.rows)
            ),
            t.rows,
            lowered.map(GpuExprs.bound)
          )
        }
      case a: Aggregate =>
        aggregate(a, child0(a.child))
      case Sort(child, keys, limit) =>
        using(child0(child))(t => sort(t, child, keys, limit))
      case Limit(child, limit, offset) =>
        using(child0(child)) { t =>
          val from = math.min(offset, t.rows.toLong).toInt
          // limit may be Long.MaxValue (an OFFSET without LIMIT): don't add it to from
          val to = from + math.min(limit, (t.rows - from).toLong).toInt
          GTable(t.cols.map(_.subVector(from, to)), to - from, t.bounds)
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
    val src  = s.source.asInstanceOf[ParquetSource]
    val cols =
      GpuPlans.buildAll(s.columns.size) { i =>
        val cv = cache.column(src.path, s.sourceColumnName(i))
        // Casts to the declared type keep the values
        val d = GpuTypes.dtype(s.columns(i).ptype)
        if cv.getType == d then
          cv
        else
          try cv.castTo(d)
          finally cv.close()
      }
    GTable(
      cols,
      if cols.isEmpty then
        0
      else
        cols.head.getRowCount.toInt
      ,
      s.columns.indices.map(i => cache.bound(src.path, s.sourceColumnName(i)))
    )

  private def filter(t: GTable, cond: VExpr): GTable =
    val mask = GpuExprs.evalColumn(cond, PType.Bool, t.cols, t.rows)(using boundsOf(t))
    try using(t.table)(tbl => GTable.of(tbl.filter(mask)).withBounds(t.bounds))
    finally mask.close()

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
      if config.isEnabled(GpuFeature.DistinctCount) && isDistinctCountOnly(a) then
        distinctCount(a, t)
      else
        groupByOver(a, t, positions(a.child))
    }

  /** Whether every call of `a` is count(distinct x) of the same single argument */
  private def isDistinctCountOnly(a: Aggregate): Boolean =
    a.calls.nonEmpty &&
      a.calls.forall(c => isCount(c) && c.distinct && c.args.size == 1 && c.filter.isEmpty) &&
      a.calls.map(_.args.head).distinct.size == 1

  /**
    * count(distinct x) grouped by the keys, as the distinct (keys, x) rows and then a count of
    * non-NULL x per group: both steps stay on cudf's hash paths, while `nunique` sorts (TPC-H q16)
    */
  private def distinctCount(a: Aggregate, t: GTable): GTable =
    val pos   = positions(a.child)
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
    using(distinct) { d =>
      // Group the distinct rows (keys at 0..n-1, the argument at n) by the keys
      val n       = a.keys.size
      val arg     = BExpr(ColRef(n, exprs.last.ptype, "x"))
      val counted = a.copy(
        keys = a.keys.indices.map(i => BExpr(ColRef(i, a.keys(i).ptype, s"k${i}"))),
        calls = a.calls.map(_.copy(distinct = false, args = Seq(arg)))
      )
      groupByOver(counted, d, Array.tabulate(n + 1)(identity))
    }

  /** Group `t`, the rows of `a.child` with the column of id `i` at `pos(i)`, by `a`'s keys */
  private def groupByOver(a: Aggregate, t: GTable, pos: Array[Int]): GTable = locally {
    given Bounds = boundsOf(t)
    val keys     =
      buildAll(a.keys.size)(i =>
        GpuExprs.evalColumn(a.keys(i).lower(pos), a.keys(i).ptype, t.cols, t.rows)
      )
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
        // A global aggregation groups by one constant key
        val constKey =
          if keys.isEmpty then
            val z = Scalar.fromInt(0)
            try Some(ColumnVector.fromScalar(z, t.rows))
            finally z.close()
          else
            None
        try
          val groupCols = keys ++ constKey
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
          val opts = GroupByOptions.builder().withIgnoreNullKeys(false).build()
          val res  =
            timed(s"group-by of ${t.rows} rows, ${groupCols.size} key(s)") {
              using(Table((groupCols ++ inputs.flatMap(_.cols))*)) { tbl =>
                GTable.of(tbl.groupBy(opts, groupCols.indices*).aggregate(aggs.result()*))
              }
            }
          using(res) { r =>
            val nKeys = keys.size
            // The results of each call, after the keys (and the constant key)
            var at     = nKeys + constKey.size
            val values = inputs.map { in =>
              val n = in.aggs.size
              val v = in.combine(r.cols.slice(at, at + n))
              at += n
              v
            }
            val out = a
              .columns
              .indices
              .map { i =>
                val d = GpuTypes.dtype(a.columns(i).ptype)
                if i < nKeys then
                  val src = r.cols(i)
                  if src.getType == d then
                    src.incRefCount()
                  else
                    src.castTo(d)
                else
                  val src = values(i - nKeys)
                  if src.getType == d then
                    src
                  else
                    try src.castTo(d)
                    finally src.close()
              }
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
        // DECIMAL64 when the argument was computed in 64 bits: no range check needed then
        val e   = c.args.head.lower(pos)
        val arg = GpuExprs.evalColumnNarrow(e, d, t.cols, t.rows)
        try
          if arg.getType.getTypeId == DType.DTypeEnum.DECIMAL64 &&
            DecimalSum.fitsSum(GpuExprs.bound(e), d.scale, t.rows)
          then
            // Even the sum of all rows fits in 64 bits: a plain DECIMAL64 sum is exact
            AggInput.single(arg.incRefCount(), c)
          else
            DecimalSum.split(arg, d.scale).getOrElse(AggInput.single(arg.incRefCount(), c))
        finally arg.close()
      case Some(p) =>
        AggInput.single(GpuExprs.evalColumn(c.args.head.lower(pos), p, t.cols, t.rows), c)

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
                    l.rows * KeyFilter.MinReduction <= maxSourceRows(j.left) =>
                val cols = j
                  .rightKeys
                  .map(_.lower(rpos))
                  .collect { case c: ColRef =>
                    c.index
                  }
                Option.when(cols.size == keyTypes.size)(KeyFilter(cols, keyTypes, lkt)).toList
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
        val maps = lkt.innerJoinGatherMaps(rkt, false)
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
        val maps = lkt.leftJoinGatherMaps(rkt, false)
        try gatherBoth(l, r, maps(0), maps(1), nullify = true, keepOf(j))
        finally maps.foreach(_.close())
      case JoinKind.Left =>
        leftWithResidual(j, l, r, lkt, rkt, residual.get)
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
      buildAll(t.cols.size) { i =>
        if keep(i) then
          val c = gathered(next).incRefCount()
          next += 1
          c
        else
          using(Scalar.fromNull(t.cols(i).getType))(nul => ColumnVector.fromScalar(nul, n))
      }.tap(_ => gathered.foreach(_.close()))

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
    val maps = lkt.innerJoinGatherMaps(rkt, false)
    try
      val n         = maps(0).getRowCount.toInt
      val lv        = maps(0).toColumnView(0, n)
      val rightOnly =
        config.isEnabled(GpuFeature.PruneColumns) &&
          j.residual.exists(_.refs.subsetOf(j.right.output.map(_.id).toSet))
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
                      u.cols.map(_.incRefCount()) ++
                        r.cols
                          .map { c =>
                            using(Scalar.fromNull(c.getType))(nul =>
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
    val maps = lkt.innerJoinGatherMaps(rkt, false)
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
    val p    =
      if config.isEnabled(GpuFeature.ImpliedFilters) then
        GpuPlans.impliedFilters(plan)
      else
        plan
    val t = plans().run(p)
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

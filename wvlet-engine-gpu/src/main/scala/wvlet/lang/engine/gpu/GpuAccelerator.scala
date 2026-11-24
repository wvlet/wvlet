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
      Some(GpuOp(p, () => GpuPlans(cache, config), () => cpu(p)))
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
  * How one aggregate call runs in a group-by: its owned input columns (none for count(*)), one
  * cudf aggregation per input, and how the per-group results combine into the call's value
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

  /** The split inputs of a sum over `arg` (a decimal column at `scale`), or None if a value is wider than 64 bits */
  def split(arg: ColumnVector, scale: Int): Option[AggInput] =
    val d64 = DType.create(DType.DTypeEnum.DECIMAL64, -scale)
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
          val hi =
            using(Scalar.fromInt(32))(k => v.binaryOp(BinaryOp.SHIFT_RIGHT, k, DType.INT64))
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
        finally v.close()
      finally n.close()
    }

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

  /** The value bounds of `t`'s columns, for evaluating expressions over them */
  private def boundsOf(t: GTable): Bounds =
    Bounds(t.bounds, config.isEnabled(GpuFeature.DecimalNarrow))

  def exec(p: RuntimePlan): GTable =
    if config.profile then
      // Device time of each node, children included (the device is synchronized after each)
      val start = System.nanoTime()
      depth += 1
      val t =
        try execNode(p)
        finally depth -= 1
      ai.rapids.cudf.Cuda.DEFAULT_STREAM.sync()
      info(
        f"${"  " * depth}${p.getClass.getSimpleName}: ${t.rows}%,d rows, ${(System.nanoTime() -
            start) / 1e6}%.1f ms"
      )
      t
    else
      execNode(p)

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

  private def execNode(p: RuntimePlan): GTable =
    p match
      case s: Scan =>
        scan(s)
      case Filter(child, cond, _) =>
        using(exec(child))(t => filter(t, cond.lower(positions(child))))
      case Project(child, exprs, columns) =>
        using(exec(child)) { t =>
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
        aggregate(a)
      case Sort(child, keys, limit) =>
        using(exec(child))(t => sort(t, child, keys, limit))
      case Limit(child, limit, offset) =>
        using(exec(child)) { t =>
          val from = math.min(offset, t.rows.toLong).toInt
          // limit may be Long.MaxValue (an OFFSET without LIMIT): don't add it to from
          val to = from + math.min(limit, (t.rows - from).toLong).toInt
          GTable(t.cols.map(_.subVector(from, to)), to - from, t.bounds)
        }
      case j: Join =>
        join(j)
      case Values(columns, rows) =>
        // A constant relation (e.g. a subquery result computed at plan time): upload its rows
        GTable(
          buildAll(columns.size)(i => GpuTypes.fromVec(rows.column(i), columns(i).ptype, rows.size)),
          rows.size
        )
      case Distinct(child) =>
        using(exec(child)) { t =>
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

  private def scan(s: Scan): GTable =
    val src  = s.source.asInstanceOf[ParquetSource]
    val cols =
      GpuPlans.buildAll(s.columns.size) { i =>
        val cv = cache.column(src.path, s.sourceColumnName(i))
        // Casts to the declared type keep the values
        val d  = GpuTypes.dtype(s.columns(i).ptype)
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

  private def aggregate(a: Aggregate): GTable =
    using(exec(a.child)) { t =>
      val pos      = positions(a.child)
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
              var at = nKeys + constKey.size
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
        val arg = GpuExprs.evalColumnNarrow(c.args.head.lower(pos), d, t.cols, t.rows)
        try DecimalSum.split(arg, d.scale).getOrElse(AggInput.single(arg.incRefCount(), c))
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

  private def join(j: Join): GTable =
    using(exec(j.left)) { l =>
      using(exec(j.right)) { r =>
        val lpos     = positions(j.left)
        val rpos     = positions(j.right)
        val lb       = boundsOf(l)
        val rb       = boundsOf(r)
        val keyTypes = j.leftKeys.zip(j.rightKeys).map((a, b) => joinKeyType(a.ptype, b.ptype).get)
        val lk       =
          buildAll(keyTypes.size)(i =>
            GpuExprs.evalColumn(j.leftKeys(i).lower(lpos), keyTypes(i), l.cols, l.rows)(using lb)
          )
        val rk =
          try
            buildAll(keyTypes.size)(i =>
              GpuExprs.evalColumn(j.rightKeys(i).lower(rpos), keyTypes(i), r.cols, r.rows)(using
                rb
              )
            )
          catch
            case e: Throwable =>
              lk.foreach(_.close())
              throw e
        try
          using(Table(lk*)) { lkt =>
            using(Table(rk*)) { rkt =>
              val residual = j.residual.map(_.lower(positions(j.inputs)))
              j.kind match
                case JoinKind.Inner =>
                  val maps = lkt.innerJoinGatherMaps(rkt, false)
                  try
                    val joined = gatherBoth(l, r, maps(0), maps(1), nullify = false)
                    residual match
                      case Some(cond) =>
                        using(joined)(t => filter(t, cond))
                      case None =>
                        joined
                  finally
                    maps.foreach(_.close())
                case JoinKind.Left if residual.isEmpty =>
                  val maps = lkt.leftJoinGatherMaps(rkt, false)
                  try gatherBoth(l, r, maps(0), maps(1), nullify = true)
                  finally maps.foreach(_.close())
                case JoinKind.Left =>
                  leftWithResidual(l, r, lkt, rkt, residual.get)
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
            }
          }
        finally
          lk.foreach(_.close())
          rk.foreach(_.close())
        end try
      }
    }

  /** The rows of both inputs at matching gather-map positions, side by side */
  private def gatherBoth(
      l: GTable,
      r: GTable,
      lmap: ai.rapids.cudf.GatherMap,
      rmap: ai.rapids.cudf.GatherMap,
      nullify: Boolean
  ): GTable =
    val n  = lmap.getRowCount.toInt
    val lv = lmap.toColumnView(0, n)
    val rv = rmap.toColumnView(0, n)
    try
      val lt     = using(l.table)(tbl => GTable.of(tbl.gather(lv, OutOfBoundsPolicy.DONT_CHECK)))
      val policy =
        if nullify then
          OutOfBoundsPolicy.NULLIFY
        else
          OutOfBoundsPolicy.DONT_CHECK
      val rt =
        try
          using(r.table)(tbl => GTable.of(tbl.gather(rv, policy)))
        catch
          case e: Throwable =>
            lt.close()
            throw e
      GTable(lt.cols ++ rt.cols, n, l.bounds ++ r.bounds)
    finally
      lv.close()
      rv.close()

  /**
    * A left outer join with a residual condition: the inner-joined pairs that pass the residual,
    * then each left row without such a pair, with NULLs for the right columns (TPC-H q13)
    */
  private def leftWithResidual(
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
        using(gatherBoth(l, r, maps(0), maps(1), nullify = false)) { pairs =>
          val pass = GpuExprs.evalColumn(residual, PType.Bool, pairs.cols, pairs.rows)(using
            boundsOf(pairs)
          )
          try
            // A NULL residual does not pass
            val f    = Scalar.fromBool(false)
            val keep =
              try pass.replaceNulls(f)
              finally f.close()
            try
              using(pairs.table)(t => GTable.of(t.filter(keep))).pipe { passing =>
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
          finally pass.close()
        }
      finally lv.close()
    finally maps.foreach(_.close())

  end leftWithResidual

  /** The rows of `l` whose index is not among the left indices `lv` of pairs where `keep` holds */
  private def leftRowsWithout(l: GTable, lv: ai.rapids.cudf.ColumnView, keep: ColumnVector): GTable =
    val matched = using(lv.copyToColumnVector()) { lc =>
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
    finally matched.close()

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
          using(gatherBoth(l, r, maps(0), maps(1), nullify = false)) { pairs =>
            val pass = GpuExprs.evalColumn(residual, PType.Bool, pairs.cols, pairs.rows)(using
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

  private def supported(e: BExpr, input: IndexedSeq[Column])(using GpuConfig): Boolean = GpuExprs.supports(
    e.lower(positions(input))
  )

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
class GpuOp(plan: RuntimePlan, plans: () => GpuPlans, cpu: () => Operator)
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
    val t    = plans().exec(plan)
    try
      val types = schema.types
      val vecs  = t.cols.indices.map(i => GpuTypes.toVec(t.cols(i), types(i))).toArray
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

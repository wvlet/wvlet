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

  private lazy val cache: GpuColumnCache =
    val pool = GpuRuntime.init(config)
    GpuColumnCache((pool * config.cacheFraction).toLong)

  /** The columns cached on the device */
  def columnCache: GpuColumnCache = cache

  override def accelerate(p: RuntimePlan, cpu: RuntimePlan => Operator): Option[Operator] =
    // A subtree without a join or aggregation returns about as many rows as it reads: copying
    // them back to the host costs more than the CPU's own scan (TPC-H q13 and q22 read o_comment
    // and c_phone this way), so such subtrees stay on the CPU
    // and planning never touches the device: it is initialized when a GPU operator first runs
    if GpuRuntime.isAvailable && GpuPlans.reduces(p) && GpuPlans.supports(p) then
      Some(GpuOp(p, () => GpuPlans(cache), () => cpu(p)))
    else
      None

end GpuAccelerator

/** The device result of a plan node: owned columns, positionally matching the node's output */
private[gpu] final class GTable(val cols: IndexedSeq[ColumnVector], val rows: Int)
    extends AutoCloseable:
  def table: Table           = Table(cols*)
  override def close(): Unit = cols.foreach(_.close())

private[gpu] object GTable:
  /** Take the columns of a cudf table (closing the table) */
  def of(t: Table): GTable =
    try GTable(
        (0 until t.getNumberOfColumns).map(i => t.getColumn(i).incRefCount()),
        t.getRowCount.toInt
      )
    finally t.close()

/** Plan-time support checks and device execution of runtime plan nodes */
private[gpu] class GpuPlans(cache: GpuColumnCache):
  import GpuPlans.*

  def exec(p: RuntimePlan): GTable =
    p match
      case s: Scan =>
        scan(s)
      case Filter(child, cond, _) =>
        using(exec(child))(t => filter(t, cond.lower(positions(child))))
      case Project(child, exprs, columns) =>
        using(exec(child)) { t =>
          val pos = positions(child)
          GTable(
            buildAll(exprs.size)(i =>
              GpuExprs.evalColumn(exprs(i).lower(pos), columns(i).ptype, t.cols, t.rows)
            ),
            t.rows
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
          GTable(t.cols.map(_.subVector(from, to)), to - from)
        }
      case j: Join =>
        join(j)
      case Distinct(child) =>
        using(exec(child)) { t =>
          using(t.table) { tbl =>
            GTable.of(
              tbl.dropDuplicates(t.cols.indices.toArray, Table.DuplicateKeepOption.KEEP_ANY, true)
            )
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
    )

  private def filter(t: GTable, cond: VExpr): GTable =
    val mask = GpuExprs.evalColumn(cond, PType.Bool, t.cols, t.rows)
    try using(t.table)(tbl => GTable.of(tbl.filter(mask)))
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
        try using(t.table)(tbl => GTable.of(tbl.gather(map)))
        finally map.close()
      finally
        order.close()
    finally
      keyCols.foreach(_.close())

  end sort

  private def aggregate(a: Aggregate): GTable =
    using(exec(a.child)) { t =>
      val pos  = positions(a.child)
      val keys =
        buildAll(a.keys.size)(i =>
          GpuExprs.evalColumn(a.keys(i).lower(pos), a.keys(i).ptype, t.cols, t.rows)
        )
      // One input column per call argument, in the type the aggregation accumulates in
      val args =
        try
          buildAll(a.calls.size, (o: Option[ColumnVector]) => o.foreach(_.close())) { i =>
            val c = a.calls(i)
            aggInputType(c).map(p => GpuExprs.evalColumn(c.args.head.lower(pos), p, t.cols, t.rows))
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
            // count(*) counts the rows of any column: the first key
            val inputs   = groupCols ++ args.flatten
            val aggs     = Seq.newBuilder[ai.rapids.cudf.GroupByAggregationOnColumn]
            var argIndex = groupCols.size
            a.calls
              .zip(args)
              .foreach { (c, arg) =>
                val idx =
                  arg match
                    case Some(_) =>
                      val i = argIndex
                      argIndex += 1
                      i
                    case None =>
                      0
                aggs += aggregation(c).onColumn(idx)
              }
            val opts = GroupByOptions.builder().withIgnoreNullKeys(false).build()
            val res  =
              using(Table(inputs*)) { tbl =>
                GTable.of(tbl.groupBy(opts, groupCols.indices*).aggregate(aggs.result()*))
              }
            using(res) { r =>
              val nKeys = keys.size
              val out   = a
                .columns
                .indices
                .map { i =>
                  // Skip the constant key of a global aggregation
                  val src =
                    if i < nKeys then
                      r.cols(i)
                    else
                      r.cols(i + constKey.size)
                  val d = GpuTypes.dtype(a.columns(i).ptype)
                  if src.getType == d then
                    src.incRefCount()
                  else
                    src.castTo(d)
                }
              GTable(out, r.rows)
            }
          finally
            constKey.foreach(_.close())
          end try
      finally
        keys.foreach(_.close())
        args.flatten.foreach(_.close())
      end try
    }

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
        val keyTypes = j.leftKeys.zip(j.rightKeys).map((a, b) => joinKeyType(a.ptype, b.ptype).get)
        val lk       =
          buildAll(keyTypes.size)(i =>
            GpuExprs.evalColumn(j.leftKeys(i).lower(lpos), keyTypes(i), l.cols, l.rows)
          )
        val rk =
          try
            buildAll(keyTypes.size)(i =>
              GpuExprs.evalColumn(j.rightKeys(i).lower(rpos), keyTypes(i), r.cols, r.rows)
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
                case JoinKind.Left =>
                  val maps = lkt.leftJoinGatherMaps(rkt, false)
                  try gatherBoth(l, r, maps(0), maps(1), nullify = true)
                  finally maps.foreach(_.close())
                case JoinKind.Semi | JoinKind.Anti if residual.isEmpty =>
                  val map =
                    if j.kind == JoinKind.Semi then
                      lkt.leftSemiJoinGatherMap(rkt, false)
                    else
                      lkt.leftAntiJoinGatherMap(rkt, false)
                  try
                    val view = map.toColumnView(0, map.getRowCount.toInt)
                    try using(l.table)(tbl => GTable.of(tbl.gather(view)))
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
      GTable(lt.cols ++ rt.cols, n)
    finally
      lv.close()
      rv.close()

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
            val pass = GpuExprs.evalColumn(residual, PType.Bool, pairs.cols, pairs.rows)
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
            try using(l.table)(tbl => GTable.of(tbl.filter(mask)))
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
  def apply(cache: GpuColumnCache): GpuPlans = new GpuPlans(cache)

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

  private def supported(e: BExpr, input: IndexedSeq[Column]): Boolean = GpuExprs.supports(
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
  def supports(p: RuntimePlan): Boolean =
    supportedTypes(p) && {
      p match
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
                j.residual.isEmpty
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

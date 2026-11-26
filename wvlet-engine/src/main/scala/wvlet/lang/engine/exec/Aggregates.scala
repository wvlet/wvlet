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
package wvlet.lang.engine.exec

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import java.math.BigDecimal as JBigDecimal
import scala.collection.mutable

/**
  * Per-group state of one aggregate. `update` folds a batch of argument vectors into the groups
  * given by `groups` (one group id per row)
  */
abstract class Accumulator:
  def ptype: PType
  def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit
  def result(numGroups: Int): Vec

  /**
    * Fold n rows whose only argument is `src` at the positions `sel` (-1 for NULL), reading `src`
    * in place instead of a gathered copy (`aggregate_selected_rows`). False when this accumulator
    * doesn't read such input, so that the caller gathers it and calls [[update]]
    */
  def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean = false

  protected var capacity: Int = 0

  /** Make room for group ids below numGroups */
  final def ensureGroups(numGroups: Int): Unit =
    if numGroups > capacity then
      val newCap = math.max(numGroups, math.max(16, capacity * 2))
      grow(newCap)
      capacity = newCap

  protected def grow(newCap: Int): Unit

/** Direction of an aggregate's ORDER BY key: ascending or not, and where NULLs go */
case class SortDir(ascending: Boolean, nullsFirst: Boolean)

/**
  * An aggregate call: function name, bound argument expressions, DISTINCT, and an optional FILTER
  * condition. With an ORDER BY (`array_agg(x ORDER BY y)`), `order` holds one direction per key,
  * and the keys are the last `order.size` expressions of `args`, so plan rewrites remap them with
  * the values. With `dictionaryFilter` (`dictionary_agg_filters`), the FILTER's parts over one
  * string column run once per dictionary entry ([[DictionaryPredicates.wrapShared]])
  */
case class AggCall(
    name: String,
    args: Seq[VExpr],
    distinct: Boolean,
    filter: Option[VExpr],
    ptype: PType,
    order: Seq[SortDir] = Nil,
    dictionaryFilter: Boolean = false
):
  /** The FILTER condition as evaluated */
  lazy val evalFilter: Option[VExpr] =
    if dictionaryFilter then
      filter.map(DictionaryPredicates.wrapShared)
    else
      filter

  /** The aggregated value arguments, without the ORDER BY keys */
  def valueArgs: Seq[VExpr] = args.dropRight(order.size)

  def sig: String =
    s"agg:${name}(${
        if distinct then
          "distinct "
        else
          ""
      }${valueArgs.map(_.sig).mkString(",")}${
        if order.isEmpty then
          ""
        else
          args
            .takeRight(order.size)
            .zip(order)
            .map((k, d) =>
              s"${k.sig}${
                  if d.ascending then
                    ""
                  else
                    " desc"
                }${
                  if d.nullsFirst then
                    " nulls first"
                  else
                    ""
                }"
            )
            .mkString(" order by ", ",", "")
      })${filter.map(f => s" filter ${f.sig}").getOrElse("")}"

  def newAccumulator(): Accumulator =
    val vs  = valueArgs.map(_.ptype)
    val acc = Aggregates.accumulator(name, vs, ptype)
    val a   =
      if distinct then
        DistinctAccumulator(acc)
      else
        acc
    if order.isEmpty then
      a
    else
      OrderedAccumulator(a, vs, order)

end AggCall

object Aggregates:

  private val names: Set[String] = Set(
    "count",
    "count_star",
    "sum",
    "avg",
    "mean",
    "min",
    "max",
    "arbitrary",
    "any_value",
    "first",
    "last",
    "max_by",
    "min_by",
    "arg_max",
    "arg_min",
    "bool_and",
    "bool_or",
    "every",
    "string_agg",
    "listagg",
    "group_concat",
    "array_agg",
    "list",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "approx_count_distinct",
    "count_distinct",
    "median",
    "quantile_cont",
    "quantile_disc",
    "quantile",
    "approx_quantile",
    "count_if",
    "countif",
    "sum_if",
    "product",
    "histogram",
    "mode",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "regr_r2",
    "regr_count",
    "kurtosis",
    "skewness",
    "bit_and",
    "bit_or",
    "bit_xor"
  )

  def isAggregate(name: String): Boolean = names.contains(name.toLowerCase)

  /** Every aggregate function name the engine binds, lowercase */
  def aggregateNames: Set[String] = names

  /** Result type of an aggregate over the given argument types (DuckDB rules) */
  def resultType(name: String, argTypes: Seq[PType]): PType =
    name.toLowerCase match
      case "count" | "count_star" | "approx_count_distinct" | "count_distinct" | "count_if" |
          "countif" =>
        PType.Int64
      case "sum" | "sum_if" =>
        argTypes.headOption match
          case Some(PType.Int32 | PType.Int64 | PType.Bool) =>
            PType.Int64
          case Some(d: PType.Decimal) =>
            PType.decimal(38, d.scale)
          case Some(PType.Null) =>
            PType.Int64
          case _ =>
            PType.Float64
      case "product" =>
        PType.Float64
      case "avg" | "mean" | "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" |
          "var_pop" | "quantile_cont" | "median" =>
        argTypes.headOption match
          case Some(PType.Date) | Some(PType.Timestamp) | Some(PType.TimestampTz)
              if name.toLowerCase == "median" =>
            argTypes.head
          case _ =>
            PType.Float64
      case "min" | "max" | "arbitrary" | "any_value" | "first" | "last" | "quantile_disc" |
          "quantile" | "approx_quantile" | "mode" =>
        argTypes.headOption.getOrElse(PType.Null)
      case "max_by" | "min_by" | "arg_max" | "arg_min" =>
        argTypes.headOption.getOrElse(PType.Null)
      case "bool_and" | "bool_or" | "every" =>
        PType.Bool
      case "corr" | "covar_pop" | "covar_samp" | "regr_slope" | "regr_intercept" | "regr_r2" |
          "kurtosis" | "skewness" =>
        PType.Float64
      case "regr_count" =>
        PType.Int64
      case "bit_and" | "bit_or" | "bit_xor" =>
        argTypes.headOption match
          case Some(t @ (PType.Int32 | PType.Int64)) =>
            t
          case Some(PType.Null) | None =>
            PType.Int64
          case Some(other) =>
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(s"${name} of ${other} is not supported in the engine; pass an integer")
      case "string_agg" | "listagg" | "group_concat" =>
        PType.Str
      case "array_agg" | "list" | "histogram" =>
        PType.AnyT
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported aggregate function: ${other}")

  def accumulator(name: String, argTypes: Seq[PType], out: PType): Accumulator =
    name.toLowerCase match
      case "count_star" =>
        CountStar()
      case "count" if argTypes.isEmpty =>
        CountStar()
      case "count" =>
        CountNonNull()
      case "count_if" | "countif" =>
        CountIf()
      case "approx_count_distinct" | "count_distinct" =>
        DistinctAccumulator(CountNonNull())
      case "sum" =>
        out match
          case PType.Int64 =>
            SumLong()
          case d: PType.Decimal =>
            SumDecimal(d)
          case _ =>
            SumDouble()
      case "sum_if" =>
        SumIf(accumulator("sum", argTypes.take(1), out))
      case "product" =>
        Product()
      case "avg" | "mean" =>
        Avg()
      case "min" =>
        MinMax(out, isMax = false)
      case "max" =>
        MinMax(out, isMax = true)
      case "any_value" =>
        // The first non-NULL value in primitive arrays: dependent_group_keys carries its
        // dropped keys this way
        MinMax(out, isMax = false, keepFirst = true)
      case "arbitrary" | "first" =>
        First(out)
      case "last" =>
        Last(out)
      case "max_by" | "arg_max" =>
        ByKey(out, isMax = true)
      case "min_by" | "arg_min" =>
        ByKey(out, isMax = false)
      case "bool_and" | "every" =>
        BoolAgg(isAnd = true)
      case "bool_or" =>
        BoolAgg(isAnd = false)
      case "string_agg" | "listagg" | "group_concat" =>
        StringAgg()
      case "array_agg" | "list" =>
        ListAgg()
      case "stddev" | "stddev_samp" =>
        Moments(Moments.StddevSamp)
      case "stddev_pop" =>
        Moments(Moments.StddevPop)
      case "variance" | "var_samp" =>
        Moments(Moments.VarSamp)
      case "var_pop" =>
        Moments(Moments.VarPop)
      case "median" =>
        Quantile(out, Some(0.5), continuous = true)
      case "quantile_cont" =>
        Quantile(out, None, continuous = true)
      case "quantile_disc" | "quantile" | "approx_quantile" =>
        Quantile(out, None, continuous = false)
      case "mode" =>
        Mode(out)
      case "histogram" =>
        Histogram()
      case "corr" =>
        CoMoments(CoMoments.Corr)
      case "covar_pop" =>
        CoMoments(CoMoments.CovarPop)
      case "covar_samp" =>
        CoMoments(CoMoments.CovarSamp)
      case "regr_slope" =>
        CoMoments(CoMoments.RegrSlope)
      case "regr_intercept" =>
        CoMoments(CoMoments.RegrIntercept)
      case "regr_r2" =>
        CoMoments(CoMoments.RegrR2)
      case "regr_count" =>
        CoMoments(CoMoments.RegrCount)
      case "kurtosis" =>
        PowerSums(isKurtosis = true)
      case "skewness" =>
        PowerSums(isKurtosis = false)
      case "bit_and" =>
        BitAgg(out, BitAgg.And)
      case "bit_or" =>
        BitAgg(out, BitAgg.Or)
      case "bit_xor" =>
        BitAgg(out, BitAgg.Xor)
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported aggregate function: ${other}")

  // ---------------------------------------------------------------------------------------

  final class CountStar extends Accumulator:
    // Read by FusedAccumulators, which adds the rows of a batch to it
    private[exec] var counts: Array[Long] = new Array[Long](0)
    def ptype: PType                      = PType.Int64
    protected def grow(newCap: Int): Unit = counts = java.util.Arrays.copyOf(counts, newCap)
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      var i = 0
      while i < n do
        counts(groups(i)) += 1
        i += 1

    def result(numGroups: Int): Vec = LongVec(
      PType.Int64,
      java.util.Arrays.copyOf(counts, numGroups),
      null
    )

  final class CountNonNull extends Accumulator:
    // Read by FusedAccumulators, which adds the rows of a batch without nulls to it
    private[exec] var counts: Array[Long] = new Array[Long](0)
    def ptype: PType                      = PType.Int64
    protected def grow(newCap: Int): Unit = counts = java.util.Arrays.copyOf(counts, newCap)
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      if v.nulls == null then
        while i < n do
          counts(groups(i)) += 1
          i += 1
      else
        while i < n do
          if !v.nulls(i) then
            counts(groups(i)) += 1
          i += 1

    override def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean =
      val vn = src.nulls
      var i  = 0
      while i < n do
        val k = sel(i)
        if k >= 0 && (vn == null || !vn(k)) then
          counts(groups(i)) += 1
        i += 1
      true

    def result(numGroups: Int): Vec = LongVec(
      PType.Int64,
      java.util.Arrays.copyOf(counts, numGroups),
      null
    )

  end CountNonNull

  final class CountIf extends Accumulator:
    private var counts: Array[Long]       = new Array[Long](0)
    def ptype: PType                      = PType.Int64
    protected def grow(newCap: Int): Unit = counts = java.util.Arrays.copyOf(counts, newCap)
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val m = Kernels.trueMask(args(0))
      var i = 0
      while i < n do
        if m(i) then
          counts(groups(i)) += 1
        i += 1

    def result(numGroups: Int): Vec = LongVec(
      PType.Int64,
      java.util.Arrays.copyOf(counts, numGroups),
      null
    )

  /** Shared bookkeeping for "has the group seen a non-null value" */
  abstract class SeenAccumulator extends Accumulator:
    protected var seen: Array[Boolean]        = new Array[Boolean](0)
    protected def growSeen(newCap: Int): Unit = seen = java.util.Arrays.copyOf(seen, newCap)
    protected def nullsOf(numGroups: Int): Array[Boolean] =
      var any = false
      val o   = new Array[Boolean](numGroups)
      var i   = 0
      while i < numGroups do
        if !seen(i) then
          o(i) = true
          any = true
        i += 1
      if any then
        o
      else
        null

  final class SumLong extends SeenAccumulator:
    private var sums: Array[Long]         = new Array[Long](0)
    def ptype: PType                      = PType.Int64
    protected def grow(newCap: Int): Unit =
      sums = java.util.Arrays.copyOf(sums, newCap)
      growSeen(newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      val a = Kernels.asLongs(v)
      var i = 0
      while i < n do
        if v.nulls == null || !v.nulls(i) then
          val g = groups(i)
          sums(g) = Math.addExact(sums(g), a(i))
          seen(g) = true
        i += 1

    override def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean =
      src match
        case l: LongVec if l.ptype == PType.Int64 =>
          val a  = l.values
          val vn = l.nulls
          var i  = 0
          while i < n do
            val k = sel(i)
            if k >= 0 && (vn == null || !vn(k)) then
              val g = groups(i)
              sums(g) = Math.addExact(sums(g), a(k))
              seen(g) = true
            i += 1
          true
        case _ =>
          false

    def result(numGroups: Int): Vec = LongVec(
      PType.Int64,
      java.util.Arrays.copyOf(sums, numGroups),
      nullsOf(numGroups)
    )

  end SumLong

  final class SumDouble extends SeenAccumulator:
    private var sums: Array[Double]       = new Array[Double](0)
    def ptype: PType                      = PType.Float64
    protected def grow(newCap: Int): Unit =
      sums = java.util.Arrays.copyOf(sums, newCap)
      growSeen(newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      val a = Kernels.asDoubles(v)
      var i = 0
      while i < n do
        if v.nulls == null || !v.nulls(i) then
          val g = groups(i)
          sums(g) += a(i)
          seen(g) = true
        i += 1

    override def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean =
      src match
        case d: DoubleVec =>
          val a  = d.values
          val vn = d.nulls
          var i  = 0
          while i < n do
            val k = sel(i)
            if k >= 0 && (vn == null || !vn(k)) then
              val g = groups(i)
              sums(g) += a(k)
              seen(g) = true
            i += 1
          true
        case _ =>
          false

    def result(numGroups: Int): Vec = DoubleVec(
      java.util.Arrays.copyOf(sums, numGroups),
      nullsOf(numGroups)
    )

  end SumDouble

  /**
    * Decimal sums on unscaled longs. A group's sum is `big(g) + sums(g)`: when adding to `sums(g)`
    * would overflow, its value moves into the BigDecimal part `big(g)` ([[spill]]) and `sums(g)`
    * starts again from the added value, so that every row can keep adding to `sums(g)`
    */
  final class SumDecimal(val ptype: PType.Decimal) extends SeenAccumulator:
    // Read by FusedAccumulators, which adds the rows of a batch to them
    private[exec] var sums: Array[Long]   = new Array[Long](0)
    private var big: Array[JBigDecimal]   = new Array[JBigDecimal](0)
    protected def grow(newCap: Int): Unit =
      sums = java.util.Arrays.copyOf(sums, newCap)
      big = java.util.Arrays.copyOf(big, newCap)
      growSeen(newCap)

    private[exec] def seenFlags: Array[Boolean] = seen

    /**
      * The unscaled values of `v` that [[FusedAccumulators]] can add to `sums`: `v` has no nulls
      * and holds unscaled Longs at this sum's scale. Otherwise null
      */
    private[exec] def fusableValues(v: Vec): Array[Long] =
      if v.ptype == PType.Null || v.nulls != null then
        null
      else
        Kernels.asUnscaled(v, ptype.scale)

    /** Add `y` to group g whose Long part `sums(g) + y` overflows */
    private[exec] def spill(g: Int, y: Long): Unit =
      val x = JBigDecimal.valueOf(sums(g), ptype.scale)
      big(g) =
        if big(g) == null then
          x
        else
          big(g).add(x)
      sums(g) = y

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      val u = Kernels.asUnscaled(v, ptype.scale)
      var i = 0
      while i < n do
        if v.nulls == null || !v.nulls(i) then
          val g = groups(i)
          seen(g) = true
          if u != null then
            val x = sums(g)
            val y = u(i)
            val z = x + y
            if ((x ^ z) & (y ^ z)) < 0 then
              spill(g, y)
            else
              sums(g) = z
          else
            val add = ValueOps.rescale(ValueOps.toBigDecimal(v.get(i)), ptype.scale)
            big(g) =
              if big(g) == null then
                add
              else
                big(g).add(add)
        i += 1

    private def atScale(t: PType): Boolean =
      t match
        case PType.Decimal(_, s) =>
          s == ptype.scale
        case _ =>
          false

    // Unscaled Longs at this sum's scale are added in place; other inputs are gathered
    override def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean =
      src match
        case l: LongVec if atScale(l.ptype) =>
          val u  = l.values
          val vn = l.nulls
          var i  = 0
          while i < n do
            val k = sel(i)
            if k >= 0 && (vn == null || !vn(k)) then
              val g = groups(i)
              seen(g) = true
              val x = sums(g)
              val y = u(k)
              val z = x + y
              if ((x ^ z) & (y ^ z)) < 0 then
                spill(g, y)
              else
                sums(g) = z
            i += 1
          true
        case _ =>
          false

    def result(numGroups: Int): Vec =
      val b = VecBuilder(ptype, numGroups).asInstanceOf[VecBuilder.DecimalBuilder]
      var g = 0
      while g < numGroups do
        if !seen(g) then
          b.addNull()
        else if big(g) != null then
          b.addBig(big(g).add(JBigDecimal.valueOf(sums(g), ptype.scale)))
        else
          b.addUnscaled(sums(g))
        g += 1
      b.result()

  end SumDecimal

  final class SumIf(inner: Accumulator) extends Accumulator:
    def ptype: PType                                               = inner.ptype
    protected def grow(newCap: Int): Unit                          = inner.ensureGroups(newCap)
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val m   = Kernels.trueMask(args(1))
      var cnt = 0
      var i   = 0
      while i < n do
        if m(i) then
          cnt += 1
        i += 1
      val idx = new Array[Int](cnt)
      val gs  = new Array[Int](cnt)
      var j   = 0
      i = 0
      while i < n do
        if m(i) then
          idx(j) = i
          gs(j) = groups(i)
          j += 1
        i += 1
      inner.update(gs, cnt, Array(args(0).gather(idx, cnt)))

    def result(numGroups: Int): Vec = inner.result(numGroups)

  final class Product extends SeenAccumulator:
    private var prods: Array[Double]      = new Array[Double](0)
    def ptype: PType                      = PType.Float64
    protected def grow(newCap: Int): Unit =
      val old = prods.length
      prods = java.util.Arrays.copyOf(prods, newCap)
      java.util.Arrays.fill(prods, old, newCap, 1.0)
      growSeen(newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      val a = Kernels.asDoubles(v)
      var i = 0
      while i < n do
        if !v.isNull(i) then
          prods(groups(i)) *= a(i)
          seen(groups(i)) = true
        i += 1

    def result(numGroups: Int): Vec = DoubleVec(
      java.util.Arrays.copyOf(prods, numGroups),
      nullsOf(numGroups)
    )

  final class Avg extends Accumulator:
    private var sums: Array[Double]       = new Array[Double](0)
    private var counts: Array[Long]       = new Array[Long](0)
    def ptype: PType                      = PType.Float64
    protected def grow(newCap: Int): Unit =
      sums = java.util.Arrays.copyOf(sums, newCap)
      counts = java.util.Arrays.copyOf(counts, newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      v.ptype match
        case d: PType.Decimal =>
          // Sum decimals exactly per batch to avoid accumulating binary rounding errors
          val u = Kernels.asUnscaled(v, d.scale)
          if u != null then
            val scale = math.pow(10, d.scale)
            var i     = 0
            while i < n do
              if !v.isNull(i) then
                val g = groups(i)
                sums(g) += u(i) / scale
                counts(g) += 1
              i += 1
            return
        case _ =>
      val a = Kernels.asDoubles(v)
      var i = 0
      while i < n do
        if !v.isNull(i) then
          val g = groups(i)
          sums(g) += a(i)
          counts(g) += 1
        i += 1

    def result(numGroups: Int): Vec =
      val o                     = new Array[Double](numGroups)
      var nulls: Array[Boolean] = null
      var g                     = 0
      while g < numGroups do
        if counts(g) == 0 then
          if nulls == null then
            nulls = new Array[Boolean](numGroups)
          nulls(g) = true
        else
          o(g) = sums(g) / counts(g)
        g += 1
      DoubleVec(o, nulls)

  end Avg

  /**
    * The min or max of each group, or with `keepFirst` its first non-NULL value (any_value), held
    * as longs, doubles or objects by type
    */
  final class MinMax(val ptype: PType, isMax: Boolean, keepFirst: Boolean = false)
      extends SeenAccumulator:
    private var longs: Array[Long]     = null
    private var doubles: Array[Double] = null
    private var objs: Array[Any]       = null
    private val mode: Int              =
      ptype match
        case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp | PType.TimestampTz =>
          0
        case PType.Float64 =>
          1
        case _ =>
          2

    protected def grow(newCap: Int): Unit =
      mode match
        case 0 =>
          longs =
            if longs == null then
              new Array[Long](newCap)
            else
              java.util.Arrays.copyOf(longs, newCap)
        case 1 =>
          doubles =
            if doubles == null then
              new Array[Double](newCap)
            else
              java.util.Arrays.copyOf(doubles, newCap)
        case _ =>
          val o = new Array[Any](newCap)
          if objs != null then
            System.arraycopy(objs, 0, o, 0, objs.length)
          objs = o
      growSeen(newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      mode match
        case 0 =>
          val a =
            v match
              case l: LongVec =>
                l.values
              case i: IntVec =>
                Kernels.asLongs(i)
              case other =>
                Kernels.asLongs(other)
          var i = 0
          while i < n do
            if v.nulls == null || !v.nulls(i) then
              val g = groups(i)
              val x = a(i)
              if !seen(g) ||
                !keepFirst && (
                  if isMax then
                    x > longs(g)
                  else
                    x < longs(g)
                )
              then
                longs(g) = x
                seen(g) = true
            i += 1
        case 1 =>
          val a = Kernels.asDoubles(v)
          var i = 0
          while i < n do
            if v.nulls == null || !v.nulls(i) then
              val g = groups(i)
              val x = a(i)
              if !seen(g) ||
                !keepFirst && (
                  if isMax then
                    x > doubles(g)
                  else
                    x < doubles(g)
                )
              then
                doubles(g) = x
                seen(g) = true
            i += 1
        case _ if keepFirst =>
          // Read a row's value only until its group has one
          var i = 0
          while i < n do
            val g = groups(i)
            if !seen(g) then
              val x = v.get(i)
              if x != null then
                objs(g) = x
                seen(g) = true
            i += 1
        case _ =>
          var i = 0
          while i < n do
            val x = v.get(i)
            if x != null then
              val g = groups(i)
              if !seen(g) then
                objs(g) = x
                seen(g) = true
              else
                val c = ValueOps.compare(x, objs(g))
                if (isMax && c > 0) || (!isMax && c < 0) then
                  objs(g) = x
            i += 1
      end match

    end update

    // Int64 and Double values are compared in place; other inputs are gathered
    override def updateSelected(groups: Array[Int], n: Int, src: Vec, sel: Array[Int]): Boolean =
      val vn = src.nulls
      (mode, src) match
        case (0, l: LongVec) if l.ptype == PType.Int64 && ptype == PType.Int64 =>
          val a = l.values
          var i = 0
          while i < n do
            val k = sel(i)
            if k >= 0 && (vn == null || !vn(k)) then
              val g = groups(i)
              val x = a(k)
              if !seen(g) ||
                !keepFirst && (
                  if isMax then
                    x > longs(g)
                  else
                    x < longs(g)
                )
              then
                longs(g) = x
                seen(g) = true
            i += 1
          true
        case (1, d: DoubleVec) =>
          val a = d.values
          var i = 0
          while i < n do
            val k = sel(i)
            if k >= 0 && (vn == null || !vn(k)) then
              val g = groups(i)
              val x = a(k)
              if !seen(g) ||
                !keepFirst && (
                  if isMax then
                    x > doubles(g)
                  else
                    x < doubles(g)
                )
              then
                doubles(g) = x
                seen(g) = true
            i += 1
          true
        case _ =>
          false
      end match

    end updateSelected

    def result(numGroups: Int): Vec =
      val nulls = nullsOf(numGroups)
      mode match
        case 0 =>
          ptype match
            case PType.Int32 | PType.Date =>
              val ints = new Array[Int](numGroups)
              var g    = 0
              while g < numGroups do
                ints(g) = longs(g).toInt
                g += 1
              IntVec(ptype, ints, nulls)
            case _ =>
              LongVec(ptype, java.util.Arrays.copyOf(longs, numGroups), nulls)
        case 1 =>
          DoubleVec(java.util.Arrays.copyOf(doubles, numGroups), nulls)
        case _ =>
          val b = VecBuilder(ptype, numGroups)
          var g = 0
          while g < numGroups do
            if seen(g) then
              b.add(objs(g))
            else
              b.addNull()
            g += 1
          b.result()

  end MinMax

  /** Boxed per-group values, for the less performance-critical aggregates */
  abstract class ObjAccumulator extends Accumulator:
    protected var values: Array[Any]      = new Array[Any](0)
    protected def grow(newCap: Int): Unit =
      val o = new Array[Any](newCap)
      System.arraycopy(values, 0, o, 0, values.length)
      values = o

  final class First(val ptype: PType) extends ObjAccumulator:
    private var seen: Array[Boolean]               = new Array[Boolean](0)
    override protected def grow(newCap: Int): Unit =
      super.grow(newCap)
      seen = java.util.Arrays.copyOf(seen, newCap)

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      while i < n do
        val g = groups(i)
        if !seen(g) then
          val x = v.get(i)
          if x != null then
            values(g) = x
            seen(g) = true
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(ptype, values.take(numGroups).toSeq)

  final class Last(val ptype: PType) extends ObjAccumulator:
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      while i < n do
        values(groups(i)) = v.get(i)
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(ptype, values.take(numGroups).toSeq)

  final class ByKey(val ptype: PType, isMax: Boolean) extends ObjAccumulator:
    private var keys: Array[Any]                   = new Array[Any](0)
    override protected def grow(newCap: Int): Unit =
      super.grow(newCap)
      val o = new Array[Any](newCap)
      System.arraycopy(keys, 0, o, 0, keys.length)
      keys = o

    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      val k = args(1)
      var i = 0
      while i < n do
        val key = k.get(i)
        if key != null then
          val g   = groups(i)
          val cur = keys(g)
          if cur == null || (
              if isMax then
                ValueOps.compare(key, cur) > 0
              else
                ValueOps.compare(key, cur) < 0
            )
          then
            keys(g) = key
            values(g) = v.get(i)
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(ptype, values.take(numGroups).toSeq)

  final class BoolAgg(isAnd: Boolean) extends ObjAccumulator:
    def ptype: PType                                               = PType.Bool
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = Kernels.asBool(args(0))
      var i = 0
      while i < n do
        if !v.isNull(i) then
          val g   = groups(i)
          val cur = values(g)
          val x   = v.values(i)
          values(g) =
            if cur == null then
              x
            else if isAnd then
              cur.asInstanceOf[Boolean] && x
            else
              cur.asInstanceOf[Boolean] || x
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(PType.Bool, values.take(numGroups).toSeq)

  final class StringAgg extends ObjAccumulator:
    def ptype: PType                                               = PType.Str
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v   = args(0)
      val sep =
        if args.length > 1 then
          args(1)
        else
          null
      var i = 0
      while i < n do
        val x = v.get(i)
        if x != null then
          val g = groups(i)
          values(g) match
            case null =>
              val sb = StringBuilder(ValueOps.toStr(x, v.ptype))
              values(g) = sb
            case sb: StringBuilder =>
              val s =
                if sep == null then
                  ","
                else
                  Option(sep.get(i)).map(ValueOps.toStr).getOrElse(",")
              sb.append(s).append(ValueOps.toStr(x, v.ptype))
            case _ =>
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(
      PType.Str,
      values
        .take(numGroups)
        .toSeq
        .map(x =>
          if x == null then
            null
          else
            x.toString
        )
    )

  end StringAgg

  final class ListAgg extends ObjAccumulator:
    def ptype: PType                                               = PType.AnyT
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      while i < n do
        val g = groups(i)
        values(g) match
          case null =>
            val b = mutable.ArrayBuffer.empty[Any]
            b += v.get(i)
            values(g) = b
          case b: mutable.ArrayBuffer[Any] @unchecked =>
            b += v.get(i)
          case _ =>
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(
      PType.AnyT,
      values
        .take(numGroups)
        .toSeq
        .map(x =>
          if x == null then
            null
          else
            x.asInstanceOf[mutable.ArrayBuffer[Any]].toList
        )
    )

  final class Histogram extends ObjAccumulator:
    def ptype: PType                                               = PType.AnyT
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      while i < n do
        val x = v.get(i)
        if x != null then
          val g = groups(i)
          val m =
            values(g) match
              case null =>
                val m = mutable.LinkedHashMap.empty[Any, Long]
                values(g) = m
                m
              case m: mutable.LinkedHashMap[Any, Long] @unchecked =>
                m
              case _ =>
                null
          m(x) = m.getOrElse(x, 0L) + 1
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(
      PType.AnyT,
      values
        .take(numGroups)
        .toSeq
        .map(x =>
          if x == null then
            null
          else
            x.asInstanceOf[mutable.LinkedHashMap[Any, Long]].toMap
        )
    )

  end Histogram

  object Moments:
    val VarSamp    = 0
    val VarPop     = 1
    val StddevSamp = 2
    val StddevPop  = 3

  /** Welford's online variance */
  final class Moments(kind: Int) extends Accumulator:
    private var n: Array[Long]            = new Array[Long](0)
    private var mean: Array[Double]       = new Array[Double](0)
    private var m2: Array[Double]         = new Array[Double](0)
    def ptype: PType                      = PType.Float64
    protected def grow(newCap: Int): Unit =
      n = java.util.Arrays.copyOf(n, newCap)
      mean = java.util.Arrays.copyOf(mean, newCap)
      m2 = java.util.Arrays.copyOf(m2, newCap)

    def update(groups: Array[Int], size: Int, args: Array[Vec]): Unit =
      val v = args(0)
      val a = Kernels.asDoubles(v)
      var i = 0
      while i < size do
        if !v.isNull(i) then
          val g = groups(i)
          val x = a(i)
          n(g) += 1
          val delta = x - mean(g)
          mean(g) += delta / n(g)
          m2(g) += delta * (x - mean(g))
        i += 1

    def result(numGroups: Int): Vec =
      val b = VecBuilder(PType.Float64, numGroups)
      var g = 0
      while g < numGroups do
        val c      = n(g)
        val v: Any =
          kind match
            case Moments.VarSamp =>
              if c < 2 then
                null
              else
                m2(g) / (c - 1)
            case Moments.VarPop =>
              if c < 1 then
                null
              else
                m2(g) / c
            case Moments.StddevSamp =>
              if c < 2 then
                null
              else
                math.sqrt(m2(g) / (c - 1))
            case _ =>
              if c < 1 then
                null
              else
                math.sqrt(m2(g) / c)
        b.add(v)
        g += 1
      b.result()

    end result

  end Moments

  object CoMoments:
    val Corr          = 0
    val CovarPop      = 1
    val CovarSamp     = 2
    val RegrSlope     = 3
    val RegrIntercept = 4
    val RegrR2        = 5
    val RegrCount     = 6

  /**
    * Online co-moments of (y, x) pairs, skipping pairs with a NULL, for correlation, covariance,
    * and linear regression. The results follow DuckDB: NaN where a variance is zero (corr,
    * regr_slope), NULL for regr_intercept and regr_r2 when x is constant, and regr_r2 = 1 when y is
    * constant
    */
  final class CoMoments(kind: Int) extends Accumulator:
    private var n: Array[Long]       = new Array[Long](0)
    private var meanX: Array[Double] = new Array[Double](0)
    private var meanY: Array[Double] = new Array[Double](0)
    private var m2x: Array[Double]   = new Array[Double](0)
    private var m2y: Array[Double]   = new Array[Double](0)
    private var cxy: Array[Double]   = new Array[Double](0)
    def ptype: PType                 =
      if kind == CoMoments.RegrCount then
        PType.Int64
      else
        PType.Float64

    protected def grow(newCap: Int): Unit =
      n = java.util.Arrays.copyOf(n, newCap)
      meanX = java.util.Arrays.copyOf(meanX, newCap)
      meanY = java.util.Arrays.copyOf(meanY, newCap)
      m2x = java.util.Arrays.copyOf(m2x, newCap)
      m2y = java.util.Arrays.copyOf(m2y, newCap)
      cxy = java.util.Arrays.copyOf(cxy, newCap)

    def update(groups: Array[Int], size: Int, args: Array[Vec]): Unit =
      val yv = args(0)
      val xv = args(1)
      if yv.ptype == PType.Null || xv.ptype == PType.Null then
        return
      val ys = Kernels.asDoubles(yv)
      val xs = Kernels.asDoubles(xv)
      var i  = 0
      while i < size do
        if !yv.isNull(i) && !xv.isNull(i) then
          val g = groups(i)
          val x = xs(i)
          val y = ys(i)
          n(g) += 1
          val c  = n(g).toDouble
          val dx = x - meanX(g)
          val dy = y - meanY(g)
          meanX(g) += dx / c
          meanY(g) += dy / c
          m2x(g) += dx * (x - meanX(g))
          m2y(g) += dy * (y - meanY(g))
          cxy(g) += dx * (y - meanY(g))
        i += 1

    def result(numGroups: Int): Vec =
      val b = VecBuilder(ptype, numGroups)
      var g = 0
      while g < numGroups do
        val c      = n(g)
        val v: Any =
          if kind == CoMoments.RegrCount then
            c
          else if c == 0 then
            null
          else
            val varX = m2x(g) / c
            val varY = m2y(g) / c
            val cov  = cxy(g) / c
            kind match
              case CoMoments.Corr =>
                cov / (math.sqrt(varX) * math.sqrt(varY))
              case CoMoments.CovarPop =>
                cov
              case CoMoments.CovarSamp =>
                if c < 2 then
                  null
                else
                  cxy(g) / (c - 1)
              case CoMoments.RegrSlope =>
                cov / varX
              case CoMoments.RegrIntercept =>
                if varX == 0 then
                  null
                else
                  meanY(g) - cov / varX * meanX(g)
              case _ =>
                if varX == 0 then
                  null
                else if varY == 0 then
                  1.0
                else
                  val r = cov / (math.sqrt(varX) * math.sqrt(varY))
                  r * r
        b.add(v)
        g += 1
      end while
      b.result()

    end result

  end CoMoments

  /**
    * Sample excess kurtosis or sample skewness from power sums, with DuckDB's formulas: NULL for
    * fewer than 4 (kurtosis) or 3 (skewness) values, or when all values are equal
    */
  final class PowerSums(isKurtosis: Boolean) extends Accumulator:
    private var n: Array[Long]            = new Array[Long](0)
    private var s1: Array[Double]         = new Array[Double](0)
    private var s2: Array[Double]         = new Array[Double](0)
    private var s3: Array[Double]         = new Array[Double](0)
    private var s4: Array[Double]         = new Array[Double](0)
    def ptype: PType                      = PType.Float64
    protected def grow(newCap: Int): Unit =
      n = java.util.Arrays.copyOf(n, newCap)
      s1 = java.util.Arrays.copyOf(s1, newCap)
      s2 = java.util.Arrays.copyOf(s2, newCap)
      s3 = java.util.Arrays.copyOf(s3, newCap)
      s4 = java.util.Arrays.copyOf(s4, newCap)

    def update(groups: Array[Int], size: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      val a = Kernels.asDoubles(v)
      var i = 0
      while i < size do
        if !v.isNull(i) then
          val g  = groups(i)
          val x  = a(i)
          val x2 = x * x
          n(g) += 1
          s1(g) += x
          s2(g) += x2
          s3(g) += x2 * x
          s4(g) += x2 * x2
        i += 1

    private def kurtosis(g: Int): Any =
      val c = n(g).toDouble
      if n(g) <= 3 then
        null
      else
        val t   = 1.0 / c
        val sum = s1(g)
        val m4  =
          t * (
            s4(g) - 4 * s3(g) * sum * t + 6 * s2(g) * sum * sum * t * t -
              3 * math.pow(sum, 4) * math.pow(t, 3)
          )
        val m2 = t * (s2(g) - sum * sum * t)
        if m2 <= 0 then
          null
        else
          (c - 1) *
            ((c + 1) * m4 / (m2 * m2) - 3 * (c - 1)) /
            ((c - 2) * (c - 3))

    private def skewness(g: Int): Any =
      val c = n(g).toDouble
      if n(g) <= 2 then
        null
      else
        val t   = 1.0 / c
        val sum = s1(g)
        val div = math.sqrt(math.pow(t * (s2(g) - sum * sum * t), 3))
        if div == 0 then
          null
        else
          val t1 = math.sqrt(c * (c - 1)) / (c - 2)
          t1 * t *
            (s3(g) - 3 * s2(g) * sum * t + 2 * math.pow(sum, 3) * t * t) / div

    def result(numGroups: Int): Vec =
      val b = VecBuilder(PType.Float64, numGroups)
      var g = 0
      while g < numGroups do
        b.add(
          if isKurtosis then
            kurtosis(g)
          else
            skewness(g)
        )
        g += 1
      b.result()

  end PowerSums

  object BitAgg:
    val And = 0
    val Or  = 1
    val Xor = 2

  /** Bitwise AND / OR / XOR of the non-NULL integers of each group; NULL for none */
  final class BitAgg(val ptype: PType, op: Int) extends SeenAccumulator:
    private var bits: Array[Long]         = new Array[Long](0)
    protected def grow(newCap: Int): Unit =
      bits = java.util.Arrays.copyOf(bits, newCap)
      growSeen(newCap)

    def update(groups: Array[Int], size: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if v.ptype == PType.Null then
        return
      val a = Kernels.asLongs(v)
      var i = 0
      while i < size do
        if !v.isNull(i) then
          val g = groups(i)
          val x = a(i)
          bits(g) =
            if !seen(g) then
              x
            else
              op match
                case BitAgg.And =>
                  bits(g) & x
                case BitAgg.Or =>
                  bits(g) | x
                case _ =>
                  bits(g) ^ x
          seen(g) = true
        i += 1

    def result(numGroups: Int): Vec =
      val b = VecBuilder(ptype, numGroups)
      var g = 0
      while g < numGroups do
        if !seen(g) then
          b.add(null)
        else if ptype == PType.Int32 then
          b.add(bits(g).toInt)
        else
          b.add(bits(g))
        g += 1
      b.result()

  end BitAgg

  /** Exact quantiles over collected values; the fraction comes from the second argument */
  final class Quantile(val ptype: PType, fixed: Option[Double], continuous: Boolean)
      extends ObjAccumulator:
    private var fraction: Option[Double]                           = fixed
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      if fraction.isEmpty && args.length > 1 && args(1).size > 0 then
        fraction = Option(args(1).get(0)).map(ValueOps.toDouble)
      var i = 0
      while i < n do
        val x = v.get(i)
        if x != null then
          val g = groups(i)
          values(g) match
            case null =>
              val b = mutable.ArrayBuffer.empty[Any]
              b += x
              values(g) = b
            case b: mutable.ArrayBuffer[Any] @unchecked =>
              b += x
            case _ =>
        i += 1

    def result(numGroups: Int): Vec =
      val q = fraction.getOrElse(0.5)
      val b = VecBuilder(ptype, numGroups)
      var g = 0
      while g < numGroups do
        values(g) match
          case null =>
            b.addNull()
          case buf: mutable.ArrayBuffer[Any] @unchecked =>
            val sorted = buf.sortWith((x, y) => ValueOps.compare(x, y) < 0)
            if continuous && ptype == PType.Float64 then
              val pos = q * (sorted.size - 1)
              val lo  = math.floor(pos).toInt
              val hi  = math.ceil(pos).toInt
              val lv  = ValueOps.toDouble(sorted(lo))
              val hv  = ValueOps.toDouble(sorted(hi))
              b.add(lv + (hv - lv) * (pos - lo))
            else
              val idx = math.max(0, math.ceil(q * sorted.size).toInt - 1)
              b.add(sorted(math.min(idx, sorted.size - 1)))
          case _ =>
            b.addNull()
        g += 1
      b.result()

  end Quantile

  final class Mode(val ptype: PType) extends ObjAccumulator:
    def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
      val v = args(0)
      var i = 0
      while i < n do
        val x = v.get(i)
        if x != null then
          val g = groups(i)
          val m =
            values(g) match
              case null =>
                val m = mutable.LinkedHashMap.empty[Any, Long]
                values(g) = m
                m
              case m: mutable.LinkedHashMap[Any, Long] @unchecked =>
                m
              case _ =>
                null
          m(x) = m.getOrElse(x, 0L) + 1
        i += 1

    def result(numGroups: Int): Vec = Vec.fromValues(
      ptype,
      values
        .take(numGroups)
        .toSeq
        .map {
          case null =>
            null
          case m: mutable.LinkedHashMap[Any, Long] @unchecked =>
            m.maxBy(_._2)._1
          case _ =>
            null
        }
    )

  end Mode

end Aggregates

/**
  * Buffers each group's rows (values, then ORDER BY keys) and feeds them to `inner` sorted by the
  * keys when the result is read, for order-sensitive aggregates such as `array_agg(x ORDER BY y)`.
  * The sort is stable, so ties keep their input order
  */
final class OrderedAccumulator(inner: Accumulator, valueTypes: Seq[PType], order: Seq[SortDir])
    extends Accumulator:
  private val numValues = valueTypes.size
  private var rows      = new Array[mutable.ArrayBuffer[Array[Any]]](0)
  def ptype: PType      = inner.ptype

  protected def grow(newCap: Int): Unit =
    inner.ensureGroups(newCap)
    rows = java.util.Arrays.copyOf(rows, newCap)

  def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
    var i = 0
    while i < n do
      val g = groups(i)
      if rows(g) == null then
        rows(g) = mutable.ArrayBuffer.empty[Array[Any]]
      rows(g) += Array.tabulate(args.length)(j => args(j).get(i))
      i += 1

  private val rowOrdering: Ordering[Array[Any]] =
    (x, y) =>
      var c = 0
      var k = 0
      while c == 0 && k < order.size do
        val d = order(k)
        val a = x(numValues + k)
        val b = y(numValues + k)
        c =
          if a == null && b == null then
            0
          else if a == null then
            if d.nullsFirst then
              -1
            else
              1
          else if b == null then
            if d.nullsFirst then
              1
            else
              -1
          else
            val r = ValueOps.compare(a, b)
            if d.ascending then
              r
            else
              -r
        k += 1
      c

  // Feed one group at a time and release its rows, so the sorted copy is never the whole input
  def result(numGroups: Int): Vec =
    var g = 0
    while g < numGroups && g < rows.length do
      val r = rows(g)
      if r != null then
        r.sortInPlace()(using rowOrdering)
        val n = r.size
        inner.update(
          Array.fill(n)(g),
          n,
          Array.tabulate(numValues)(j => Vec.fromValues(valueTypes(j), r.view.map(_(j)).toSeq))
        )
        rows(g) = null
      g += 1
    inner.result(numGroups)

end OrderedAccumulator

/** Feeds each distinct (group, argument values) once into the inner accumulator */
final class DistinctAccumulator(inner: Accumulator) extends Accumulator:
  private val seen                                               = mutable.HashSet.empty[Any]
  def ptype: PType                                               = inner.ptype
  protected def grow(newCap: Int): Unit                          = inner.ensureGroups(newCap)
  def update(groups: Array[Int], n: Int, args: Array[Vec]): Unit =
    val keep = new Array[Int](n)
    var k    = 0
    var i    = 0
    while i < n do
      val key: Any =
        if args.length == 1 then
          (groups(i), args(0).get(i))
        else
          (groups(i), args.toSeq.map(_.get(i)))
      if seen.add(key) then
        keep(k) = i
        k += 1
      i += 1
    if k > 0 then
      val gs = new Array[Int](k)
      var j  = 0
      while j < k do
        gs(j) = groups(keep(j))
        j += 1
      inner.update(gs, k, args.map(_.gather(keep, k)))

  def result(numGroups: Int): Vec = inner.result(numGroups)

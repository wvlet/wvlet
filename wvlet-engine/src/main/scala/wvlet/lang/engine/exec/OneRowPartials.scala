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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * The partial result of an aggregate over each row of a batch on its own, read from the
  * aggregate's argument without an [[Accumulator]]: a sum is its argument (NULL stays NULL), a
  * count is 1 or 0, and a min or max is the value itself. Partial aggregates that stop grouping
  * ([[PartialAggSkip]]) emit these one-row partial results for every passed-through row.
  *
  * [[of]] gives null when an aggregate has no such direct form (DISTINCT, ORDER BY, other
  * functions, or a value that doesn't fit the result type exactly); its rows are then folded by an
  * accumulator, one group per row.
  */
object OneRowPartials:

  /**
    * The partial result of `agg` for each row of `b` as its own group, or null. `args` are the
    * values of `agg.args` over `b`, evaluated once so an accumulator can fold them when this gives
    * null
    */
  def of(agg: AggCall, args: Array[Vec], b: Batch): Vec =
    if agg.distinct || agg.order.nonEmpty then
      return null
    val n = b.size
    // Rows the FILTER keeps (null: every row)
    def keep: Array[Boolean] = agg.evalFilter.map(f => Kernels.trueMask(f.eval(b))).orNull
    // NULL where the argument is NULL or the FILTER drops the row
    def nullsOf(v: Vec, m: Array[Boolean]): Array[Boolean] =
      if m == null then
        v.nulls
      else
        val o = new Array[Boolean](n)
        var i = 0
        while i < n do
          o(i) = !m(i) || v.isNull(i)
          i += 1
        o
    agg.name.toLowerCase match
      case "count" | "count_star" if agg.args.isEmpty =>
        counts(n, keep, null)
      case "count" =>
        counts(n, keep, args(0))
      case "count_if" | "countif" =>
        val c = Kernels.trueMask(args(0))
        val m = keep
        counts(
          n,
          if m == null then
            c
          else
            Array.tabulate[Boolean](n)(i => c(i) && m(i))
          ,
          null
        )
      case "sum" if args.length == 1 =>
        val v = args(0)
        if v.ptype == PType.Null then
          null
        else
          agg.ptype match
            case PType.Int64 =>
              val m = keep
              LongVec(PType.Int64, exact(Kernels.asLongs(v), n), nullsOf(v, m))
            case PType.Float64 =>
              val m = keep
              DoubleVec(exact(Kernels.asDoubles(v), n), nullsOf(v, m))
            case d: PType.Decimal =>
              val u = Kernels.asUnscaled(v, d.scale)
              if u == null then
                null
              else
                val m = keep
                LongVec(d, exact(u, n), nullsOf(v, m))
            case _ =>
              null
      case "min" | "max" if args.length == 1 && agg.filter.isEmpty =>
        val v = args(0)
        if v.ptype == agg.ptype then
          v
        else
          null
      case _ =>
        null

    end match

  end of

  /** 1 for each row that counts: kept by `m` (null: all rows) and non-null in `v` (null: all) */
  private def counts(n: Int, m: Array[Boolean], v: Vec): Vec =
    val o = new Array[Long](n)
    var i = 0
    while i < n do
      if (m == null || m(i)) && (v == null || !v.isNull(i)) then
        o(i) = 1L
      i += 1
    LongVec(PType.Int64, o, null)

  /** `a` trimmed to `n` values, as a vector's size is its values' length */
  private def exact(a: Array[Long], n: Int): Array[Long] =
    if a.length == n then
      a
    else
      java.util.Arrays.copyOf(a, n)

  private def exact(a: Array[Double], n: Int): Array[Double] =
    if a.length == n then
      a
    else
      java.util.Arrays.copyOf(a, n)

end OneRowPartials

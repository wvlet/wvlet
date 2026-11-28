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
package wvlet.lang.engine.catalog

import wvlet.lang.engine.expr.Kernels
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.*

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * The distinct values of a string column in a sample of `rows` rows of a source, each with the
  * number of sampled rows holding it, and the number of sampled NULLs: an estimate of the fraction
  * of rows a condition over the column keeps, as DuckDB samples string columns for its filter
  * estimates
  */
final class ValueCounts(
    val values: Array[String],
    val counts: Array[Long],
    val nulls: Long,
    val rows: Long
):

  /**
    * The column's distinct values, when the sample has seen almost all of them: at least
    * [[ValueCounts.MinSaturatedRows]] non-null rows, and values seen once (the Good-Turing estimate
    * of the share of rows holding an unseen value) at most [[ValueCounts.MaxSingletonShare]] of
    * them. None otherwise, as for a key with a value per row or a sample too small to tell
    */
  def sampledDistinct: Option[Double] =
    val nonNull    = rows - nulls
    var singletons = 0L
    var i          = 0
    while i < counts.length do
      if counts(i) == 1 then
        singletons += 1
      i += 1
    Option.when(
      values.nonEmpty && nonNull >= ValueCounts.MinSaturatedRows &&
        singletons <= nonNull * ValueCounts.MaxSingletonShare
    )(values.length.toDouble)

  /**
    * The fraction of the sampled rows for which `cond`, a condition reading the column as column 0,
    * is TRUE: evaluated once per distinct value and once for NULL, and at least 1 / rows, as a
    * value missing from the sample may still be in the unsampled rows. None for an empty sample or
    * when `cond` fails to evaluate
    */
  def selectivity(cond: VExpr): Option[Double] =
    if rows <= 0 then
      None
    else
      // The join search asks for the same conditions many times per query
      estimates.computeIfAbsent(cond.sig, _ => evaluate(cond))

  // Estimates by condition signature (conditions over one string column and constants only)
  private val estimates = java.util.concurrent.ConcurrentHashMap[String, Option[Double]]()

  private def evaluate(cond: VExpr): Option[Double] =
    val n      = values.length
    val withNa = nulls > 0
    val size   =
      if withNa then
        n + 1
      else
        n
    val vs =
      if withNa then
        java.util.Arrays.copyOf(values, size)
      else
        values
    val ns =
      if withNa then
        val a = new Array[Boolean](size)
        a(n) = true
        a
      else
        null
    try
      val mask = Kernels.trueMask(cond.eval(Batch(Array[Vec](StrVec(vs, ns)), size)))
      var kept = 0L
      var i    = 0
      while i < size do
        if mask(i) then
          kept += (
            if i < n then
              counts(i)
            else
              nulls
          )
        i += 1
      Some(math.max(1L, kept).toDouble / rows.toDouble)
    catch
      case NonFatal(_) =>
        None

  end evaluate

end ValueCounts

object ValueCounts:

  /** The fewest non-null sampled rows for [[ValueCounts.sampledDistinct]] */
  val MinSaturatedRows: Long = 10000

  /** The largest share of sampled rows holding a value seen once, for `sampledDistinct` */
  val MaxSingletonShare: Double = 0.001

  /**
    * Counts the strings of the sampled batches' column 0, or None when they hold more than
    * `maxValues` distinct values: a high-cardinality column keeps its default estimates. Rows
    * decoded from a Parquet dictionary are counted per code, and their value looked up once per
    * code and batch
    */
  def count(batches: Iterator[Batch], maxValues: Int): Option[ValueCounts] =
    val index                            = mutable.HashMap.empty[String, Int]
    val values                           = mutable.ArrayBuffer.empty[String]
    val counts                           = mutable.ArrayBuffer.empty[Long]
    var nulls                            = 0L
    var rows                             = 0L
    def add(v: String, k: Long): Boolean =
      val j = index.getOrElseUpdate(
        v, {
          values += v
          counts += 0L
          values.size - 1
        }
      )
      counts(j) += k
      values.size <= maxValues
    var ok = true
    while ok && batches.hasNext do
      val b = batches.next()
      val n = b.size
      rows += n
      b.column(0) match
        case s: StrVec if s.hasCodes && s.dictSize > 0 =>
          val codes = s.codes
          // Per code, its row count and the first row holding it
          val perCode = new Array[Long](s.dictSize)
          val first   = new Array[Int](s.dictSize)
          var i       = 0
          while ok && i < n do
            val c = codes(i)
            if c >= 0 then
              if perCode(c) == 0 then
                first(c) = i
              perCode(c) += 1
            else if s.isNull(i) then
              nulls += 1
            else
              ok = add(s.values(i), 1)
            i += 1
          var c = 0
          while ok && c < perCode.length do
            if perCode(c) > 0 then
              ok = add(s.values(first(c)), perCode(c))
            c += 1
        case v =>
          var i = 0
          while ok && i < n do
            if v.isNull(i) then
              nulls += 1
            else
              ok = add(v.get(i).toString, 1)
            i += 1
      end match
    end while
    Option.when(ok)(ValueCounts(values.toArray, counts.toArray, nulls, rows))

  end count

end ValueCounts

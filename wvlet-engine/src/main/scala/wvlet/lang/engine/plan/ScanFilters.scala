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
package wvlet.lang.engine.plan

import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.catalog.ValueCounts
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetFilter

import java.math.BigDecimal as JBigDecimal
import java.time.Instant
import java.time.LocalDate
import scala.collection.mutable

/**
  * Translates a bound filter condition into a [[ParquetFilter]] that a Parquet scan uses to skip
  * row groups by their min/max statistics. The engine still evaluates the full condition on every
  * row it reads, so the translation only has to be implied by the condition: conjuncts it can't
  * express are dropped, and a disjunction is kept only when both sides translate.
  *
  * Only comparisons whose Parquet statistics order matches the engine's order are translated:
  * integers, dates, timestamps and booleans, and string equality (Parquet orders strings by UTF-8
  * bytes while the engine compares UTF-16 code units). A timestamp constant T (micros) bounds `=`,
  * `<=` and `>` by T + 999ns: the engine floors TIMESTAMP(NANOS) values to micros, so a row at T +
  * 500ns equals T. For MILLIS and MICROS columns, which uni compares exactly, this is the same
  * filter. With `numeric`, also:
  *
  *   - Decimal comparisons, with the constant as a `java.math.BigDecimal`. uni doesn't skip row
  *     groups by decimal statistics, so [[wvlet.lang.engine.catalog.DecimalStats]] checks them for
  *     single-file scans.
  *   - Floating-point `<`, `<=` and `=`. The engine orders NaN above every number and -0.0 below
  *     0.0 (`Double.compare`), while writers leave NaN out of the statistics and uni treats -0.0 as
  *     0.0. So `>` and `>=`, which NaN rows satisfy, stay out, and `x < 0.0` becomes `x <= 0.0`,
  *     which keeps row groups holding -0.0.
  */
object ScanFilters:

  /**
    * `columnName(i)` is the Parquet column name of input position i, or None when the source can't
    * filter on that column
    */
  def toParquet(
      cond: VExpr,
      columnName: Int => Option[String],
      numeric: Boolean = false
  ): Option[ParquetFilter] =
    def go(e: VExpr): Option[ParquetFilter] =
      e match
        case AndExpr(a, b) =>
          (go(a), go(b)) match
            case (Some(x), Some(y)) =>
              Some(x && y)
            case (x, y) =>
              x.orElse(y)
        case OrExpr(a, b) =>
          for
            x <- go(a)
            y <- go(b)
          yield x || y
        case Compare(op, c: ColRef, k: Const) =>
          columnName(c.index).flatMap(compare(_, c.ptype, op, k, numeric))
        case Compare(op, k: Const, c: ColRef) =>
          columnName(c.index).flatMap(compare(_, c.ptype, flip(op), k, numeric))
        case IsNullExpr(c: ColRef, negate, _) =>
          columnName(c.index).map { name =>
            if negate then
              ParquetFilter.IsNotNull(name)
            else
              ParquetFilter.IsNull(name)
          }
        case InList(c: ColRef, values, _, false, _) =>
          columnName(c.index).flatMap(equalities(_, c.ptype, values.toSeq, numeric))
        case _ =>
          None
    go(cond)

  end toParquet

  /**
    * A [[ParquetFilter]] matching exactly the rows for which `cond` is TRUE, for a source to filter
    * rows by ([[wvlet.lang.engine.catalog.ScanSpec.rowFilter]]), or None when any part of `cond`
    * doesn't translate. Unlike [[toParquet]], no conjunct is dropped. NULLs agree with the engine:
    * the translation has no negation, and a row is kept only when the filter is TRUE.
    * `columnName(i)` is the Parquet column name of input position i, or None when the source can't
    * filter rows by that column exactly
    */
  def exact(cond: VExpr, columnName: Int => Option[String]): Option[ParquetFilter] =
    def go(e: VExpr): Option[ParquetFilter] =
      e match
        case AndExpr(a, b) =>
          for
            x <- go(a)
            y <- go(b)
          yield x && y
        case OrExpr(a, b) =>
          for
            x <- go(a)
            y <- go(b)
          yield x || y
        // A boolean column as a condition
        case c: ColRef if c.ptype == PType.Bool =>
          columnName(c.index).map(ParquetFilter.col(_) === true)
        // Without `numeric`: its floating-point and decimal leaves are implied, not exact
        case _ =>
          toParquet(e, columnName, numeric = false)
    go(cond)

  /**
    * The row filters of `conds` (conjuncts with their [[exact]] filters) as one AND, most selective
    * first: a source decodes the first conjunct's columns for every row, and later ones only for
    * the rows still kept. Conjuncts over the same column are estimated together and stay adjacent
    * ([[selectivity]]); on a tie, conjuncts over a numeric column come before strings, which cost
    * more to decode. `columnName(i)` is the source column name of input position i. With `values`,
    * string conjuncts are estimated from the source's sampled values ([[selectivity]])
    */
  def ordered(
      conds: Seq[(VExpr, ParquetFilter)],
      source: TableSource,
      columnName: Int => String,
      values: Boolean = false
  ): ParquetFilter = byColumns(conds)(_._1)
    .toSeq
    .zipWithIndex
    .sortBy { case ((cols, members), pos) =>
      val strings = cols.size != 1 || members.exists(m => refType(m._1).contains(PType.Str))
      (selectivity(cols, members.map(_._1), source, columnName, values), strings, pos)
    }
    .flatMap(_._1._2.map(_._2))
    .reduce(_ && _)

  /**
    * The largest estimated fraction of a scan's rows its row filter may keep with
    * `selective_scan_row_filters`: selective TPC-H q4, q6, q12 scans (4-14%) decode faster
    * filter-first, while q09's 20% and q1's 96% decode slower
    */
  final val MaxRowFilterSelectivity = 0.15

  /**
    * [[MaxRowFilterSelectivity]] with `wider_row_filter_gate`: one-year date ranges over TPC-H's
    * dates (15.2%) pass, TPC-DS q09's 20% ranges do not
    */
  final val WiderMaxRowFilterSelectivity = 0.18

  /**
    * The estimated fraction of rows passing all `conds` together (conjuncts over the same input
    * positions estimated together by [[selectivity]], independent otherwise). `columnName(i)` is
    * the source column name of input position i. With `values`, string conjuncts are estimated from
    * the source's sampled values ([[selectivity]])
    */
  def estimatedSelectivity(
      conds: Seq[VExpr],
      source: TableSource,
      columnName: Int => String,
      values: Boolean = false
  ): Double =
    byColumns(conds)(identity)
      .map((cols, members) => selectivity(cols, members, source, columnName, values))
      .product

  /**
    * The fraction of rows for which `cond` is TRUE, from the sampled values ([[ValueCounts]]) of
    * the one string column it reads, when it is made only of comparisons, IN lists, LIKE, IS NULL,
    * AND, OR and NOT over that column and constants (`string_value_selectivity`). `counts(i)` gives
    * the sampled values of input position i. None otherwise
    */
  def valueSelectivity(cond: VExpr, counts: Int => Option[ValueCounts]): Option[Double] =
    if cond.ptype != PType.Bool || cond.isConstant then
      None
    else
      DictionaryPredicates
        .singleStrColumn(cond)
        .flatMap(c => counts(c).flatMap(_.selectivity(DictionaryPredicates.rebase(cond, c))))

  /** `conds` grouped by the input positions their conjunct (`cond(c)`) reads, in first order */
  private def byColumns[A](conds: Seq[A])(
      cond: A => VExpr
  ): mutable.LinkedHashMap[Set[Int], List[A]] =
    val groups = mutable.LinkedHashMap.empty[Set[Int], List[A]]
    conds.foreach { c =>
      val refs = mutable.BitSet()
      VExprs.collectRefs(cond(c), refs)
      val key = refs.toSet
      groups.update(key, groups.getOrElse(key, Nil) :+ c)
    }
    groups

  /**
    * The estimated fraction of rows passing all `conds`, conjuncts that read the input positions
    * `cols`. Over one integer or date column, comparisons with literals narrow a [min, max] range
    * measured against the source's [[TableSource.columnRange]]; `=` and IN on other columns use the
    * source's distinct estimate (not for strings, whose estimate may scan the column); with
    * `values`, conjuncts over one string column are evaluated together over the source's sampled
    * values ([[valueSelectivity]]); anything else counts
    * [[CardinalityEstimator.DefaultSelectivity]]
    */
  private def selectivity(
      cols: Set[Int],
      conds: Seq[VExpr],
      source: TableSource,
      columnName: Int => String,
      values: Boolean
  ): Double =
    val default      = CardinalityEstimator.DefaultSelectivity
    lazy val sampled =
      if values && cols.size == 1 && conds.nonEmpty then
        valueSelectivity(conds.reduce(AndExpr(_, _)), i => source.valueCounts(columnName(i)))
      else
        None
    if cols.size != 1 then
      math.pow(default, conds.size)
    else if sampled.isDefined then
      sampled.get
    else
      val name    = columnName(cols.head)
      val ptype   = conds.flatMap(refType).headOption
      val ordered = ptype.exists(t => t == PType.Int32 || t == PType.Int64 || t == PType.Date)
      lazy val distinct: Option[Double] =
        if ptype.contains(PType.Str) then
          None
        else
          source.estimatedDistinct(name).filter(_ >= 1)
      var lo                              = Long.MinValue
      var hi                              = Long.MaxValue
      var bounded                         = false
      var other                           = 1.0
      def bound(op: CmpOp, v: Long): Unit =
        if !ordered then
          other *= (
            if op == CmpOp.Eq then
              distinct.map(1 / _).getOrElse(default)
            else
              default
          )
        else
          bounded = true
          op match
            case CmpOp.Eq =>
              lo = math.max(lo, v)
              hi = math.min(hi, v)
            case CmpOp.Lt =>
              hi = math.min(
                hi,
                if v == Long.MinValue then
                  v
                else
                  v - 1
              )
            case CmpOp.Le =>
              hi = math.min(hi, v)
            case CmpOp.Gt =>
              lo = math.max(
                lo,
                if v == Long.MaxValue then
                  v
                else
                  v + 1
              )
            case CmpOp.Ge =>
              lo = math.max(lo, v)
            case CmpOp.Ne =>
              other *= default
      conds.foreach {
        case Compare(op, _: ColRef, k: Const) if k.value != null =>
          bound(
            op,
            if ordered then
              ValueOps.toLong(k.value)
            else
              0L
          )
        case Compare(op, k: Const, _: ColRef) if k.value != null =>
          bound(
            flip(op),
            if ordered then
              ValueOps.toLong(k.value)
            else
              0L
          )
        case in: InList =>
          other *= distinct.map(n => math.min(1.0, in.values.size / n)).getOrElse(default)
        case _ =>
          other *= default
      }
      val range =
        if !bounded then
          1.0
        else
          source
            .columnRange(name)
            .map { (min, max) =>
              val a = math.max(lo, min)
              val b = math.min(hi, max)
              if b < a then
                0.0
              else
                (b.toDouble - a.toDouble + 1) / (max.toDouble - min.toDouble + 1)
            }
            .getOrElse(default)
      range * other
    end if

  end selectivity

  /**
    * The filters implied by `cond` when it is an `or` reading several input positions: for each
    * position that every branch tests with conjuncts reading only it, the `or` over the branches of
    * those conjuncts (TPC-H q19's `(p_brand = 'Brand#12' and p_size <= 5 ...) or (p_brand =
    * 'Brand#23' and ...)` implies `p_brand = 'Brand#12' or p_brand = 'Brand#23' or ...`). A row for
    * which `cond` is TRUE has one branch TRUE, so it passes each implied filter: they may drop rows
    * before `cond` runs, but not replace it. Nil otherwise
    */
  def columnImplied(cond: VExpr): List[VExpr] =
    def branches(e: VExpr): List[VExpr] =
      e match
        case OrExpr(a, b) =>
          branches(a) ++ branches(b)
        case other =>
          List(other)
    def refs(e: VExpr): Set[Int] =
      val bits = mutable.BitSet()
      VExprs.collectRefs(e, bits)
      bits.toSet
    cond match
      case _: OrExpr if refs(cond).size > 1 =>
        // Per branch, its single-position conjuncts by position
        val byPos = branches(cond).map { b =>
          conjuncts(b)
            .flatMap { c =>
              refs(c).toList match
                case List(i) =>
                  Some(i -> c)
                case _ =>
                  None
            }
            .groupMap(_._1)(_._2)
        }
        val common = byPos.map(_.keySet).reduce(_ intersect _)
        common.toList.sorted.map(i => byPos.map(_(i).reduce(AndExpr(_, _))).reduce(OrExpr(_, _)))
      case _ =>
        Nil

  end columnImplied

  /** The type of the column a comparison, IN, or IS NULL test reads */
  private def refType(e: VExpr): Option[PType] =
    e match
      case Compare(_, c: ColRef, _) =>
        Some(c.ptype)
      case Compare(_, _, c: ColRef) =>
        Some(c.ptype)
      case InList(c: ColRef, _, _, _, _) =>
        Some(c.ptype)
      case IsNullExpr(c: ColRef, _, _) =>
        Some(c.ptype)
      case _ =>
        None

  /** The top-level conjuncts of `cond` */
  def conjuncts(cond: VExpr): List[VExpr] =
    cond match
      case AndExpr(a, b) =>
        conjuncts(a) ++ conjuncts(b)
      case other =>
        List(other)

  /**
    * A filter implied by `column` holding one of `values` (of type `t`, non-null): their [min, max]
    * range for ordered types, or up to 64 equalities otherwise. None when it can't be expressed
    */
  def valuesFilter(column: String, t: PType, values: Iterable[Any]): Option[ParquetFilter] =
    if values.isEmpty then
      None
    else
      t match
        case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp | PType.TimestampTz =>
          range(column, t, values.minBy(ValueOps.toLong), values.maxBy(ValueOps.toLong))
        case _ =>
          equalities(column, t, values.toSeq.distinct, numeric = false)

  /**
    * `min <= column <= max` for an integer, date (`min` and `max` in epoch days), or timestamp
    * (micros) column of type `t`, or None for other types
    */
  def range(column: String, t: PType, min: Any, max: Any): Option[ParquetFilter] =
    for
      lo <- compare(column, t, CmpOp.Ge, Const(min, t), numeric = false)
      hi <- compare(column, t, CmpOp.Le, Const(max, t), numeric = false)
    yield lo && hi

  /** `column = v1 or column = v2 ...` for up to 64 non-null values, when every one translates */
  private def equalities(
      column: String,
      t: PType,
      values: Seq[Any],
      numeric: Boolean
  ): Option[ParquetFilter] =
    if values.isEmpty || values.size > 64 then
      None
    else
      val eqs = values.map(v => compare(column, t, CmpOp.Eq, Const(v, t), numeric))
      Option.when(eqs.forall(_.isDefined))(eqs.flatten.reduce(_ || _))

  private def flip(op: CmpOp): CmpOp =
    op match
      case CmpOp.Lt =>
        CmpOp.Gt
      case CmpOp.Le =>
        CmpOp.Ge
      case CmpOp.Gt =>
        CmpOp.Lt
      case CmpOp.Ge =>
        CmpOp.Le
      case other =>
        other

  private def compare(
      column: String,
      t: PType,
      op: CmpOp,
      k: Const,
      numeric: Boolean
  ): Option[ParquetFilter] =
    if k.value == null || k.ptype != t then
      None
    else if numeric && t == PType.Float64 then
      realCompare(column, op, k.value)
    else
      val value: Option[Any] =
        t match
          case PType.Int32 | PType.Int64 =>
            Some(ValueOps.toLong(k.value))
          case PType.Date =>
            Some(LocalDate.ofEpochDay(ValueOps.toInt(k.value).toLong))
          case PType.Timestamp | PType.TimestampTz =>
            val micros = ValueOps.toLong(k.value)
            Some(
              Instant.ofEpochSecond(
                Math.floorDiv(micros, ValueOps.MicrosPerSecond),
                Math.floorMod(micros, ValueOps.MicrosPerSecond) * 1000L
              )
            )
          case PType.Bool =>
            Some(ValueOps.toBoolean(k.value))
          case PType.Str if op == CmpOp.Eq =>
            Some(k.value.toString)
          case _: PType.Decimal if numeric =>
            k.value match
              case d: JBigDecimal =>
                Some(d)
              case _ =>
                None
          case _ =>
            None
      val col = ParquetFilter.col(column)
      value.flatMap {
        // The engine floors TIMESTAMP(NANOS) values to micros, while uni compares the nanos in
        // the statistics: every nanosecond of the constant's micro equals it to the engine
        case t: Instant if op == CmpOp.Eq || op == CmpOp.Le || op == CmpOp.Gt =>
          val last = t.plusNanos(999)
          op match
            case CmpOp.Eq =>
              Some(col >= t && col <= last)
            case CmpOp.Le =>
              Some(col <= last)
            case _ =>
              Some(col > last)
        case v =>
          op match
            case CmpOp.Eq =>
              Some(col === v)
            case CmpOp.Lt =>
              Some(col < v)
            case CmpOp.Le =>
              Some(col <= v)
            case CmpOp.Gt =>
              Some(col > v)
            case CmpOp.Ge =>
              Some(col >= v)
            case CmpOp.Ne =>
              None
      }

  /**
    * A floating-point comparison whose row groups uni may skip without losing rows the engine's
    * `Double.compare` order would keep (see the object comment)
    */
  private def realCompare(column: String, op: CmpOp, v: Any): Option[ParquetFilter] =
    val d   = ValueOps.toDouble(v)
    val col = ParquetFilter.col(column)
    if d.isNaN then
      None
    else
      op match
        case CmpOp.Eq =>
          Some(col === d)
        // -0.0 < 0.0 in the engine, while uni reads a -0.0 minimum as 0.0
        case CmpOp.Lt if d == 0.0 =>
          Some(col <= 0.0)
        case CmpOp.Lt =>
          Some(col < d)
        case CmpOp.Le =>
          Some(col <= d)
        case _ =>
          None

end ScanFilters

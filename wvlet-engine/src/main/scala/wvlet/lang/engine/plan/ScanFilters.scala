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

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.parquet.ParquetFilter

import java.time.Instant
import java.time.LocalDate

/**
  * Translates a bound filter condition into a [[ParquetFilter]] that a Parquet scan uses to skip
  * row groups by their min/max statistics. The engine still evaluates the full condition on every
  * row it reads, so the translation only has to be implied by the condition: conjuncts it can't
  * express are dropped, and a disjunction is kept only when both sides translate.
  *
  * Only comparisons whose Parquet statistics order matches the engine's order are translated:
  * integers, dates, timestamps and booleans, and string equality (Parquet orders strings by UTF-8
  * bytes while the engine compares UTF-16 code units). Floating-point and decimal columns are left
  * out.
  */
object ScanFilters:

  /**
    * `columnName(i)` is the Parquet column name of input position i, or None when the source can't
    * filter on that column
    */
  def toParquet(cond: VExpr, columnName: Int => Option[String]): Option[ParquetFilter] =
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
          columnName(c.index).flatMap(compare(_, c.ptype, op, k))
        case Compare(op, k: Const, c: ColRef) =>
          columnName(c.index).flatMap(compare(_, c.ptype, flip(op), k))
        case IsNullExpr(c: ColRef, negate) =>
          columnName(c.index).map { name =>
            if negate then
              ParquetFilter.IsNotNull(name)
            else
              ParquetFilter.IsNull(name)
          }
        case InList(c: ColRef, values, _, false) =>
          columnName(c.index).flatMap(equalities(_, c.ptype, values.toSeq))
        case _ =>
          None
    go(cond)

  end toParquet

  /**
    * A filter implied by `column` holding one of `values` (of type `t`, non-null): their [min, max]
    * range for ordered types, or up to 64 equalities otherwise. None when it can't be expressed
    */
  def valuesFilter(column: String, t: PType, values: Iterable[Any]): Option[ParquetFilter] =
    if values.isEmpty then
      None
    else
      t match
        case PType.Int32 | PType.Int64 | PType.Date | PType.Timestamp =>
          range(column, t, values.minBy(ValueOps.toLong), values.maxBy(ValueOps.toLong))
        case _ =>
          equalities(column, t, values.toSeq.distinct)

  /**
    * `min <= column <= max` for an integer, date (`min` and `max` in epoch days), or timestamp
    * (micros) column of type `t`, or None for other types
    */
  def range(column: String, t: PType, min: Any, max: Any): Option[ParquetFilter] =
    for
      lo <- compare(column, t, CmpOp.Ge, Const(min, t))
      hi <- compare(column, t, CmpOp.Le, Const(max, t))
    yield lo && hi

  /** `column = v1 or column = v2 ...` for up to 64 non-null values, when every one translates */
  private def equalities(column: String, t: PType, values: Seq[Any]): Option[ParquetFilter] =
    if values.isEmpty || values.size > 64 then
      None
    else
      val eqs = values.map(v => compare(column, t, CmpOp.Eq, Const(v, t)))
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

  private def compare(column: String, t: PType, op: CmpOp, k: Const): Option[ParquetFilter] =
    if k.value == null || k.ptype != t then
      None
    else
      val value: Option[Any] =
        t match
          case PType.Int32 | PType.Int64 =>
            Some(ValueOps.toLong(k.value))
          case PType.Date =>
            Some(LocalDate.ofEpochDay(ValueOps.toInt(k.value).toLong))
          case PType.Timestamp =>
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
          case _ =>
            None
      val col = ParquetFilter.col(column)
      value.flatMap { v =>
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

end ScanFilters

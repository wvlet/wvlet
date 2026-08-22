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
package wvlet.lang.tablestore.read

import wvlet.lang.tablestore.ColumnStat
import wvlet.lang.tablestore.schema.ColumnType

enum Scalar:
  case SLong(v: Long)
  case SDouble(v: Double)
  case SString(v: String)
  case SBoolean(v: Boolean)

/** A conjunctive predicate over table columns, used for catalog-side pruning */
enum Predicate:
  case Eq(column: String, value: Scalar)
  case Lt(column: String, value: Scalar)
  case Lte(column: String, value: Scalar)
  case Gt(column: String, value: Scalar)
  case Gte(column: String, value: Scalar)
  case And(parts: List[Predicate])

/**
  * Catalog-side pruning: a file survives when its min/max stat ranges can still satisfy the
  * predicate. Statistics are advisory — a missing stats row (or missing bound) means "must scan",
  * never "can skip".
  */
object Pruning:

  /**
    * @param stats
    *   the file's column statistics keyed by column name
    * @param typeOf
    *   resolves the type used to decode canonical stat values for a column
    */
  def canMatch(stats: Map[String, ColumnStat], typeOf: String => ColumnType): Predicate => Boolean =

    def apply(predicate: Predicate): Boolean =
      predicate match
        case Predicate.And(parts) =>
          parts.forall(apply)
        case pred =>
          val col = columnName(pred)
          stats.get(col) match
            case None =>
              true // no stats -> must scan
            case Some(stat) =>
              val t   = typeOf(col)
              val min = stat.minValue.map(decode(t, _))
              val max = stat.maxValue.map(decode(t, _))
              pred match
                case Predicate.Eq(_, v) =>
                  val c = asComparable(v, t)
                  min.forall(m => compare(m, c) <= 0) && max.forall(m => compare(m, c) >= 0)
                case Predicate.Lt(_, v) =>
                  max.forall(m => compare(m, asComparable(v, t)) > 0)
                case Predicate.Lte(_, v) =>
                  max.forall(m => compare(m, asComparable(v, t)) >= 0)
                case Predicate.Gt(_, v) =>
                  min.forall(m => compare(m, asComparable(v, t)) < 0)
                case Predicate.Gte(_, v) =>
                  min.forall(m => compare(m, asComparable(v, t)) <= 0)
                case Predicate.And(_) =>
                  true // handled recursively above

    apply
  end canMatch

  private def columnName(p: Predicate): String =
    p match
      case Predicate.Eq(c, _) =>
        c
      case Predicate.Lt(c, _) =>
        c
      case Predicate.Lte(c, _) =>
        c
      case Predicate.Gt(c, _) =>
        c
      case Predicate.Gte(c, _) =>
        c
      case Predicate.And(_) =>
        throw new IllegalStateException("And is handled recursively")

  /**
    * Canonical stat strings round-trip through the column's type so comparisons follow type
    * semantics, not text collation.
    */
  def decode(t: ColumnType, encoded: String): Comparable[?] =
    t match
      case ColumnType.LongType =>
        encoded.toLong
      case ColumnType.DoubleType =>
        encoded.toDouble
      case ColumnType.BooleanType =>
        java.lang.Boolean.parseBoolean(encoded)
      case _ =>
        encoded

  /** Canonical encoding of stat values — must round-trip through [[decode]] */
  def encode(t: ColumnType, s: Scalar): Option[String] =
    s match
      case Scalar.SLong(v) =>
        Some(v.toString)
      case Scalar.SDouble(v) =>
        Some(v.toString)
      case Scalar.SString(v) =>
        Some(v)
      case Scalar.SBoolean(v) =>
        Some(v.toString)

  private def asComparable(s: Scalar, t: ColumnType): Comparable[?] =
    (t, s) match
      case (_, Scalar.SBoolean(v)) =>
        java.lang.Boolean.valueOf(v)
      case (_, Scalar.SString(v)) =>
        v
      case (ColumnType.DoubleType, Scalar.SLong(v)) =>
        java.lang.Double.valueOf(v.toDouble)
      case (ColumnType.StringType, Scalar.SLong(v)) =>
        v.toString
      case (ColumnType.StringType, Scalar.SDouble(v)) =>
        v.toString
      case (_, Scalar.SLong(v)) =>
        java.lang.Long.valueOf(v)
      case (_, Scalar.SDouble(v)) =>
        java.lang.Double.valueOf(v)

  @SuppressWarnings(Array("unchecked"))
  private def compare(a: Comparable[?], b: Comparable[?]): Int = a
    .asInstanceOf[Comparable[AnyRef]]
    .compareTo(b.asInstanceOf[AnyRef])

end Pruning

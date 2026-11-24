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
package wvlet.lang.engine.vector

import wvlet.lang.model.DataType

/**
  * Physical value types of the engine. Each type has one in-memory representation:
  *
  *   - Bool: [[BoolVec]]
  *   - Int32, Date (days since 1970-01-01): [[IntVec]]
  *   - Int64, Timestamp (microseconds since the epoch, UTC), TimestampTz (an instant, as
  *     microseconds since the epoch; the engine's session time zone is UTC): [[LongVec]]
  *   - Float64: [[DoubleVec]]
  *   - Decimal: [[LongVec]] of unscaled values, or [[ObjVec]] of `java.math.BigDecimal` when the
  *     unscaled values do not fit in a Long
  *   - Str: [[StrVec]]
  *   - Null (the type of an untyped NULL literal), Any (lists, maps, structs, intervals):
  *     [[ObjVec]]
  */
sealed trait PType:
  def name: String
  override def toString: String = name

  def isNumeric: Boolean =
    this match
      case PType.Int32 | PType.Int64 | PType.Float64 =>
        true
      case _: PType.Decimal =>
        true
      case _ =>
        false

  def isIntegral: Boolean = this == PType.Int32 || this == PType.Int64
  def isDecimal: Boolean  = this.isInstanceOf[PType.Decimal]

  /** TIMESTAMP or TIMESTAMP WITH TIME ZONE: both hold microseconds since the epoch */
  def isTimestamp: Boolean = this == PType.Timestamp || this == PType.TimestampTz

  /** The wvlet DataType used for describing the result schema */
  def toDataType: DataType =
    this match
      case PType.Bool =>
        DataType.BooleanType
      case PType.Int32 =>
        DataType.IntType
      case PType.Int64 =>
        DataType.LongType
      case PType.Float64 =>
        DataType.DoubleType
      case PType.Decimal(p, s) =>
        DataType.DecimalType(DataType.IntConstant(p), DataType.IntConstant(s))
      case PType.Str =>
        DataType.StringType
      case PType.Date =>
        DataType.DateType
      case PType.Timestamp =>
        DataType.TimestampType(DataType.TimestampField.TIMESTAMP, withTimeZone = false)
      case PType.TimestampTz =>
        DataType.TimestampType(DataType.TimestampField.TIMESTAMP, withTimeZone = true)
      case PType.Null =>
        DataType.NullType
      case PType.AnyT =>
        DataType.AnyType

end PType

object PType:
  case object Bool extends PType:
    val name = "boolean"

  case object Int32 extends PType:
    val name = "int"

  case object Int64 extends PType:
    val name = "long"

  case object Float64 extends PType:
    val name = "double"

  case object Str extends PType:
    val name = "string"

  case object Date extends PType:
    val name = "date"

  case object Timestamp extends PType:
    val name = "timestamp"

  /** An instant (TIMESTAMP WITH TIME ZONE), shown in the session time zone, UTC */
  case object TimestampTz extends PType:
    val name = "timestamptz"

  case object Null extends PType:
    val name = "null"

  case object AnyT extends PType:
    val name = "any"

  case class Decimal(precision: Int, scale: Int) extends PType:
    def name: String = s"decimal(${precision},${scale})"

  val MaxDecimalPrecision = 38

  def decimal(precision: Int, scale: Int): Decimal = Decimal(
    math.min(MaxDecimalPrecision, math.max(precision, 1)),
    math.max(0, scale)
  )

  /** Map a compiler DataType to the engine's physical type */
  def fromDataType(dt: DataType): PType =
    dt match
      case DataType.BooleanType =>
        Bool
      case DataType.ByteType | DataType.ShortType | DataType.IntType =>
        Int32
      case DataType.LongType =>
        Int64
      case DataType.FloatType | DataType.RealType | DataType.DoubleType =>
        Float64
      case d: DataType.DecimalType =>
        (d.precision, d.scale) match
          case (DataType.IntConstant(p), DataType.IntConstant(s)) =>
            decimal(p, s)
          case _ =>
            decimal(18, 3)
      case DataType.StringType | DataType.JsonType =>
        Str
      case _: DataType.VarcharType | _: DataType.CharType =>
        Str
      case DataType.DateType =>
        Date
      case t: DataType.TimestampType if t.withTimeZone =>
        TimestampTz
      case _: DataType.TimestampType =>
        Timestamp
      case DataType.NullType =>
        Null
      case n: DataType.NamedType =>
        fromDataType(n.dataType)
      case _ =>
        AnyT

  /**
    * The common type two values are compared or combined in, following DuckDB's implicit cast rules
    * for the types the engine supports
    */
  def common(a: PType, b: PType): PType =
    (a, b) match
      case (x, y) if x == y =>
        x
      case (Null, y) =>
        y
      case (x, Null) =>
        x
      case (Int32, Int64) | (Int64, Int32) =>
        Int64
      case (Float64, y) if y.isNumeric =>
        Float64
      case (x, Float64) if x.isNumeric =>
        Float64
      case (d: Decimal, Int32) =>
        decimalUnion(d, Decimal(10, 0))
      case (Int32, d: Decimal) =>
        decimalUnion(d, Decimal(10, 0))
      case (d: Decimal, Int64) =>
        decimalUnion(d, Decimal(19, 0))
      case (Int64, d: Decimal) =>
        decimalUnion(d, Decimal(19, 0))
      case (x: Decimal, y: Decimal) =>
        decimalUnion(x, y)
      case (Date, Timestamp) | (Timestamp, Date) =>
        Timestamp
      // DuckDB casts a plain timestamp to TIMESTAMPTZ, not the other way
      case (Date | Timestamp, TimestampTz) | (TimestampTz, Date | Timestamp) =>
        TimestampTz
      case (Str, TimestampTz) | (TimestampTz, Str) =>
        TimestampTz
      case (Str, Date) | (Date, Str) =>
        Date
      case (Str, Timestamp) | (Timestamp, Str) =>
        Timestamp
      case (Str, y) if y.isNumeric =>
        y
      case (x, Str) if x.isNumeric =>
        x
      case (Str, Bool) | (Bool, Str) =>
        Bool
      case _ =>
        AnyT

  /** The smallest decimal holding every value of both decimals */
  def decimalUnion(x: Decimal, y: Decimal): Decimal =
    val scale = math.max(x.scale, y.scale)
    val ints  = math.max(x.precision - x.scale, y.precision - y.scale)
    decimal(ints + scale, scale)

end PType

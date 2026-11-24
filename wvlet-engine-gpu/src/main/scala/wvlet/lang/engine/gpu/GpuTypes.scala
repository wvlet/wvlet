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
import ai.rapids.cudf.HostColumnVector
import ai.rapids.cudf.Scalar
import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*

import java.math.BigDecimal as JBigDecimal
import java.math.BigInteger

/** Mapping between the engine's physical types and cudf types, and device-to-host conversion */
object GpuTypes:

  /** Decimals of up to this precision are DECIMAL64 on the device, wider ones DECIMAL128 */
  val MaxDecimal64Precision = 18

  /** The cudf type holding values of `p`, or None when the GPU executor doesn't support it */
  def dtypeOf(p: PType): Option[DType] =
    p match
      case PType.Bool =>
        Some(DType.BOOL8)
      case PType.Int32 =>
        Some(DType.INT32)
      case PType.Int64 =>
        Some(DType.INT64)
      case PType.Float64 =>
        Some(DType.FLOAT64)
      case PType.Str =>
        Some(DType.STRING)
      case PType.Date =>
        Some(DType.TIMESTAMP_DAYS)
      case PType.Timestamp | PType.TimestampTz =>
        Some(DType.TIMESTAMP_MICROSECONDS)
      case PType.Decimal(precision, scale) =>
        Some(decimalType(precision, scale))
      case _ =>
        None

  def dtype(p: PType): DType = dtypeOf(p).getOrElse(
    throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported type on the GPU: ${p}")
  )

  /** cudf decimal scales are negated: DECIMAL64 with scale -2 holds values with 2 decimal places */
  def decimalType(precision: Int, scale: Int): DType =
    if precision <= MaxDecimal64Precision then
      DType.create(DType.DTypeEnum.DECIMAL64, -scale)
    else
      DType.create(DType.DTypeEnum.DECIMAL128, -scale)

  def isDecimal(d: DType): Boolean = d.isDecimalType

  /**
    * Whether [[scalarOf]] can hold `value` (a non-null engine value of type `p`), checked on the
    * host
    */
  def convertible(value: Any, p: PType): Boolean =
    (p, value) match
      case (PType.Bool, _: Boolean) | (PType.Str, _: String) =>
        true
      case (PType.Int32 | PType.Int64 | PType.Float64 | PType.Date, _: Number) =>
        true
      case (PType.Timestamp | PType.TimestampTz, _: Number) =>
        true
      case (d: PType.Decimal, v) =>
        unscaled(v, d.scale).isDefined
      case _ =>
        false

  /** A device scalar holding `value` (an engine value of type `p`), or None if not convertible */
  def scalarOf(value: Any, p: PType): Option[Scalar] =
    if value == null then
      dtypeOf(p).map(Scalar.fromNull)
    else
      (p, value) match
        case (PType.Bool, b: Boolean) =>
          Some(Scalar.fromBool(b))
        case (PType.Int32, n: Number) =>
          Some(Scalar.fromInt(n.intValue()))
        case (PType.Int64, n: Number) =>
          Some(Scalar.fromLong(n.longValue()))
        case (PType.Float64, n: Number) =>
          Some(Scalar.fromDouble(n.doubleValue()))
        case (PType.Str, s: String) =>
          Some(Scalar.fromString(s))
        case (PType.Date, n: Number) =>
          Some(Scalar.timestampDaysFromInt(n.intValue()))
        case (PType.Timestamp | PType.TimestampTz, n: Number) =>
          Some(Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, n.longValue()))
        case (d: PType.Decimal, v) =>
          unscaled(v, d.scale).map(u => Scalar.fromDecimal(u, dtype(d)))
        case _ =>
          None

  /** The unscaled value of a decimal constant at `scale`, or None when that would round it */
  def unscaled(value: Any, scale: Int): Option[BigInteger] =
    val big: Option[JBigDecimal] =
      value match
        case b: JBigDecimal =>
          Some(b)
        case b: scala.math.BigDecimal =>
          Some(b.bigDecimal)
        case n: (Int | Long | Short | Byte) =>
          Some(JBigDecimal.valueOf(n.asInstanceOf[Number].longValue()))
        case _ =>
          None
    big.flatMap { b =>
      try
        Some(b.setScale(scale).unscaledValue())
      catch
        case _: ArithmeticException =>
          None
    }

  /** Copy a device column to a host vector of engine type `p` */
  def toVec(cv: ColumnVector, p: PType): Vec =
    val n = Math.toIntExact(cv.getRowCount)
    val h = cv.copyToHost()
    try
      val nulls: Array[Boolean] =
        if h.hasNulls then
          Array.tabulate(n)(i => h.isNull(i))
        else
          null
      p match
        case PType.Bool =>
          BoolVec(Array.tabulate(n)(i => !h.isNull(i) && h.getBoolean(i)), nulls)
        case PType.Int32 | PType.Date =>
          val o = new Array[Int](n)
          if n > 0 then
            h.getData.getInts(o, 0, 0, n)
          IntVec(p, o, nulls)
        case PType.Int64 | PType.Timestamp | PType.TimestampTz =>
          val o = new Array[Long](n)
          if n > 0 then
            h.getData.getLongs(o, 0, 0, n)
          LongVec(p, o, nulls)
        case PType.Float64 =>
          // HostMemoryBuffer has no bulk double getter: read the bits as longs
          val bits = new Array[Long](n)
          if n > 0 then
            h.getData.getLongs(bits, 0, 0, n)
          DoubleVec(bits.map(java.lang.Double.longBitsToDouble), nulls)
        case d: PType.Decimal if cv.getType.getTypeId == DType.DTypeEnum.DECIMAL64 =>
          val o = new Array[Long](n)
          if n > 0 then
            h.getData.getLongs(o, 0, 0, n)
          LongVec(d, o, nulls)
        case d: PType.Decimal =>
          // DECIMAL128: unscaled longs when every value fits, as the CPU engine keeps them
          val big =
            Array.tabulate[JBigDecimal](n)(i =>
              if h.isNull(i) then
                null
              else
                h.getBigDecimal(i)
            )
          val fits = big.forall(b => b == null || b.unscaledValue().bitLength() < 64)
          if fits then
            LongVec(
              d,
              big.map(b =>
                if b == null then
                  0L
                else
                  b.unscaledValue().longValue()
              ),
              nulls
            )
          else
            ObjVec(
              d,
              big.map(b =>
                if b == null then
                  null
                else
                  b.setScale(d.scale)
              ),
              nulls
            )
        case PType.Str =>
          StrVec(
            Array.tabulate(n)(i =>
              if h.isNull(i) then
                null
              else
                h.getJavaString(i)
            ),
            nulls
          )
        case other =>
          throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported type on the GPU: ${other}")
      end match
    finally
      h.close()
    end try

  end toVec

end GpuTypes

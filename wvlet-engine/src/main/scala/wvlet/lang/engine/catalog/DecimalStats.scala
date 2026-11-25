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

import wvlet.uni.cloud.parquet.format.LogicalType
import wvlet.uni.cloud.parquet.ParquetField
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.cloud.parquet.ParquetSchema
import wvlet.uni.cloud.parquet.format.PhysicalType
import wvlet.uni.cloud.parquet.format.RowGroupMeta
import wvlet.uni.cloud.parquet.format.Statistics

import java.math.BigDecimal as JBigDecimal
import java.math.BigInteger

/**
  * Skips Parquet row groups by the min/max statistics of DECIMAL columns, which uni's row-group
  * filter treats as always matching. A comparison of a decimal column with a `java.math.BigDecimal`
  * ([[wvlet.lang.engine.plan.ScanFilters]]) is checked against the row group's statistics; every
  * other part of the filter counts as matching here, since uni checks it.
  *
  * Parquet orders DECIMAL statistics as signed numbers: INT32 and INT64 as integers, and
  * FIXED_LEN_BYTE_ARRAY and BYTE_ARRAY as big-endian two's complement. The deprecated min/max
  * fields of byte arrays were ordered as signed bytes by old writers, so those are not trusted.
  */
object DecimalStats:

  /** Whether `f` names a decimal column of `schema` in a comparison with a BigDecimal */
  def hasDecimalCompare(f: ParquetFilter, schema: ParquetSchema): Boolean =
    f match
      case ParquetFilter.And(a, b) =>
        hasDecimalCompare(a, schema) || hasDecimalCompare(b, schema)
      case ParquetFilter.Or(a, b) =>
        hasDecimalCompare(a, schema) || hasDecimalCompare(b, schema)
      case ParquetFilter.Compare(c, _, _: JBigDecimal) =>
        decimalColumn(schema, c).isDefined
      case _ =>
        false

  /** False only when the decimal statistics of `rg` prove that no row matches `f` */
  def mayMatch(f: ParquetFilter, schema: ParquetSchema, rg: RowGroupMeta): Boolean =
    f match
      case ParquetFilter.And(a, b) =>
        mayMatch(a, schema, rg) && mayMatch(b, schema, rg)
      case ParquetFilter.Or(a, b) =>
        mayMatch(a, schema, rg) || mayMatch(b, schema, rg)
      case ParquetFilter.Compare(c, op, v: JBigDecimal) =>
        decimalColumn(schema, c) match
          case Some((p, scale)) if p.columnIndex < rg.columns.size =>
            rg.columns(p.columnIndex)
              .statistics
              .forall(s => compareMayMatch(p, scale, s, op, v, rg.numRows))
          case _ =>
            true
      case _ =>
        true

  /** A top-level DECIMAL column and its scale */
  private def decimalColumn(
      schema: ParquetSchema,
      column: String
  ): Option[(ParquetField.Primitive, Int)] =
    schema.field(column) match
      case Some(p: ParquetField.Primitive) if !p.isRepeated =>
        p.logicalType match
          case Some(LogicalType.Decimal(scale, _)) =>
            Some((p, scale))
          case _ =>
            None
      case _ =>
        None

  private def compareMayMatch(
      p: ParquetField.Primitive,
      scale: Int,
      s: Statistics,
      op: ParquetFilter.Op,
      v: JBigDecimal,
      numRows: Long
  ): Boolean =
    if s.nullCount.contains(numRows) then
      // Every value is null, so no comparison matches
      false
    else
      val bounds =
        for
          minB <- s.min
          maxB <- s.max
          min  <- unscaled(p.physicalType, minB, s.isLegacyMinMax)
          max  <- unscaled(p.physicalType, maxB, s.isLegacyMinMax)
        yield (JBigDecimal(min, scale).compareTo(v), JBigDecimal(max, scale).compareTo(v))
      bounds match
        case Some((lo, hi)) =>
          op match
            case ParquetFilter.Op.Eq =>
              lo <= 0 && hi >= 0
            case ParquetFilter.Op.Lt =>
              lo < 0
            case ParquetFilter.Op.LtEq =>
              lo <= 0
            case ParquetFilter.Op.Gt =>
              hi > 0
            case ParquetFilter.Op.GtEq =>
              hi >= 0
        case None =>
          true

  end compareMayMatch

  /** The unscaled value of a PLAIN-encoded DECIMAL statistic, or None when it can't be trusted */
  private[catalog] def unscaled(
      t: PhysicalType,
      b: Array[Byte],
      legacy: Boolean
  ): Option[BigInteger] =
    t match
      case PhysicalType.INT32 if b.length == 4 =>
        Some(BigInteger.valueOf(littleEndian(b, 4)))
      case PhysicalType.INT64 if b.length == 8 =>
        Some(BigInteger.valueOf(littleEndian(b, 8)))
      case PhysicalType.FIXED_LEN_BYTE_ARRAY | PhysicalType.BYTE_ARRAY if !legacy && b.length > 0 =>
        Some(BigInteger(b))
      case _ =>
        None

  /** A signed little-endian integer of `n` (4 or 8) bytes */
  private def littleEndian(b: Array[Byte], n: Int): Long =
    var v = 0L
    var i = n - 1
    while i >= 0 do
      v = (v << 8) | (b(i) & 0xffL)
      i -= 1
    if n == 4 then
      v.toInt.toLong
    else
      v

end DecimalStats

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner

import wvlet.lang.api.Span
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*

/**
  * Converts a query result cell to the literal bound to a for-loop variable. Engines return cells
  * either as native values (JDBC) or as strings (cross-platform connectors), so the column type
  * decides how a string cell is read back
  */
object LoopValue:
  def toLiteral(value: Any, dataType: DataType, span: Span): Literal =
    def bool(b: Boolean): Literal =
      if b then
        TrueLiteral(span)
      else
        FalseLiteral(span)

    value match
      case null =>
        NullLiteral(span)
      case b: Boolean =>
        bool(b)
      case n: (Byte | Short | Int | Long) =>
        val l = n.asInstanceOf[Number].longValue
        LongLiteral(l, l.toString, span)
      case n: (Float | Double) =>
        val d = n.asInstanceOf[Number].doubleValue
        DoubleLiteral(d, d.toString, span)
      case other =>
        val str = other.toString
        dataType match
          case DataType.IntType | DataType.LongType if str.toLongOption.isDefined =>
            LongLiteral(str.toLong, str, span)
          case DataType.FloatType | DataType.DoubleType if str.toDoubleOption.isDefined =>
            DoubleLiteral(str.toDouble, str, span)
          case _: DataType.DecimalType =>
            DecimalLiteral(str, str, span)
          case DataType.BooleanType if str.toBooleanOption.isDefined =>
            bool(str.toBoolean)
          case DataType.DateType | _: DataType.TimestampType =>
            GenericLiteral(dataType, StringLiteral.fromString(str, span), span)
          case _ =>
            StringLiteral.fromString(str, span)

  end toLiteral

end LoopValue

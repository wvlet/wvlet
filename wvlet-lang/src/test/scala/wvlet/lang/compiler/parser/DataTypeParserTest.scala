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
package wvlet.lang.compiler.parser

import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.{
  ArrayType,
  FixedSizeArrayType,
  GenericType,
  IntConstant,
  MapType,
  TimestampField
}
import wvlet.airspec.AirSpec

class DataTypeParserTest extends AirSpec:

  private def parse(s: String): DataType = DataType.parse(s)

  test("parse primitives") {
    DataType
      .knownPrimitiveTypes
      .foreach { (typeName, tpe) =>
        test(s"parse ${typeName}") {
          parse(typeName.name)
        }
      }
  }

  test("parse generic types") {
    DataType
      .knownGenericTypeNames
      .foreach { typeName =>
        test(s"parse ${typeName}") {
          parse(typeName.name) shouldMatch { case g: GenericType =>
            g.typeName shouldBe typeName
          }
        }
      }
  }

  test("parse timestamp") {
    parse("timestamp") shouldBe DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    parse("timestamp with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true)
    parse("timestamp(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true, Some(IntConstant(0)))
  }

  test("parse timestamp without time zone") {
    parse("timestamp without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    parse("timestamp(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, Some(IntConstant(1)))
  }

  test("parse time") {
    parse("time") shouldBe DataType.TimestampType(TimestampField.TIME, false, None)
    parse("time with time zone") shouldBe DataType.TimestampType(TimestampField.TIME, true)
    parse("time(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, true, Some(IntConstant(0)))
    parse("time without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, None)
    parse("time(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, Some(IntConstant(1)))
  }

  test("parse decimal types") {
    parse("decimal(10,2)") shouldBe DataType.DecimalType(IntConstant(10), IntConstant(2))
    parse("decimal(10,0)") shouldBe DataType.DecimalType(IntConstant(10), IntConstant(0))
  }

  test("parse decimal") {
    parse("decimal") shouldBe DataType.DecimalType(IntConstant(18), IntConstant(3))
  }

  test("invalid decimal types") {
    intercept[WvletLangException] {
      parse("decimal(10)")
    }
    intercept[WvletLangException] {
      parse("decimal(10,2,3)")
    }
  }

  test("parse any") {
    parse("any") shouldBe DataType.AnyType
  }

  test("parse array") {
    parse("array(int)") shouldBe ArrayType(DataType.IntType)
  }

  test("parse map") {
    parse("map(string,any)") shouldBe MapType(DataType.StringType, DataType.AnyType)
  }

  test("parse duckdb list type") {
    parse("integer[]") shouldBe ArrayType(DataType.IntType)
    parse("integer[10]") shouldBe FixedSizeArrayType(DataType.IntType, 10)
  }

  test("parse trino timestamp types") {
    parse("timestamp") shouldBe DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    parse("timestamp with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true)
    parse("timestamp(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true, Some(IntConstant(0)))
    parse("timestamp without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    parse("timestamp(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, Some(IntConstant(1)))
    parse("time") shouldBe DataType.TimestampType(TimestampField.TIME, false, None)
    parse("time with time zone") shouldBe DataType.TimestampType(TimestampField.TIME, true)
    parse("time(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, true, Some(IntConstant(0)))
    parse("time without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, None)
    parse("time(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, Some(IntConstant(1)))
  }

  test("parse varchar(x) with type variables") {
    parse("varchar(x)") shouldBe
      DataType.VarcharType(Some(DataType.TypeVariable(Name.typeName("x"))))
  }

  test("parse varchar(1)") {
    parse("varchar(1)") shouldBe DataType.VarcharType(Some(IntConstant(1)))
  }

  test("parse generic array") {
    parse("array(T)") shouldBe ArrayType(DataType.TypeVariable(Name.typeName("T")))
    parse("array(x)") shouldBe ArrayType(DataType.TypeVariable(Name.typeName("x")))
  }

  test("parse timestamp with type variables") {
    parse("timestamp(x)") shouldBe
      DataType.TimestampType(
        TimestampField.TIMESTAMP,
        false,
        Some(DataType.TypeVariable(Name.typeName("x")))
      )
  }

end DataTypeParserTest

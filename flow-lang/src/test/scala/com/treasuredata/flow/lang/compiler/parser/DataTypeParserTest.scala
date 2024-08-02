package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{
  ArrayType,
  GenericType,
  IntConstant,
  MapType,
  TimestampField
}
import wvlet.airspec.AirSpec

class DataTypeParserTest extends AirSpec:

  test("parse primitives") {
    DataType
      .knownPrimitiveTypes
      .foreach { (typeName, tpe) =>
        test(s"parse ${typeName}") {
          DataTypeParser.parse(typeName.name)
        }
      }
  }

  test("parse generic types") {
    DataType
      .knownGenericTypeNames
      .foreach { typeName =>
        test(s"parse ${typeName}") {
          DataTypeParser.parse(typeName.name) shouldMatch { case g: GenericType =>
            g.typeName shouldBe typeName
          }
        }
      }
  }

  test("parse timestamp") {
    DataTypeParser.parse("timestamp") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    DataTypeParser.parse("timestamp with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true)
    DataTypeParser.parse("timestamp(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, true, Some(IntConstant(0)))
  }

  test("parse timestamp without time zone") {
    DataTypeParser.parse("timestamp without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, None)
    DataTypeParser.parse("timestamp(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIMESTAMP, false, Some(IntConstant(1)))
  }

  test("parse time") {
    DataTypeParser.parse("time") shouldBe DataType.TimestampType(TimestampField.TIME, false, None)
    DataTypeParser.parse("time with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, true)
    DataTypeParser.parse("time(0) with time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, true, Some(IntConstant(0)))
    DataTypeParser.parse("time without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, None)
    DataTypeParser.parse("time(1) without time zone") shouldBe
      DataType.TimestampType(TimestampField.TIME, false, Some(IntConstant(1)))
  }

  test("parse any") {
    DataTypeParser.parse("any") shouldBe DataType.AnyType
  }

  test("parse array") {
    DataTypeParser.parse("array(int)") shouldBe ArrayType(DataType.IntType)
  }

  test("parse map") {
    DataTypeParser.parse("map(string,any)") shouldBe MapType(DataType.StringType, DataType.AnyType)
  }

end DataTypeParserTest

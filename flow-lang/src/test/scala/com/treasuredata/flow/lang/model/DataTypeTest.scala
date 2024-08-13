package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.compiler.Name
import wvlet.airspec.AirSpec

class DataTypeTest extends AirSpec:
  test("isNumeric") {
    DataType.IntType.isNumeric shouldBe true
    DataType.LongType.isNumeric shouldBe true
    DataType.FloatType.isNumeric shouldBe true
    DataType.DoubleType.isNumeric shouldBe true
    DataType.StringType.isNumeric shouldBe false
    DataType.BooleanType.isNumeric shouldBe false
    DataType.ArrayType(DataType.IntType).isNumeric shouldBe false
    DataType.MapType(DataType.IntType, DataType.StringType).isNumeric shouldBe false
  }

  test("isNumeric of NamedType") {
    DataType.NamedType(Name.termName("count(x)"), DataType.IntType).isNumeric shouldBe true
    DataType.NamedType(Name.termName("sum(x)"), DataType.LongType).isNumeric shouldBe true
    DataType.NamedType(Name.termName("avg(x)"), DataType.DoubleType).isNumeric shouldBe true
    DataType.NamedType(Name.termName("arbitrary(x)"), DataType.StringType).isNumeric shouldBe false
  }

  test("isNumeric of DecimalType") {
    DataType.parse("decimal(10,2)").isNumeric shouldBe true
    DataType.parse("decimal(10,0)").isNumeric shouldBe true
    DataType.parse("decimal(10,0)").isNumeric shouldBe true
  }

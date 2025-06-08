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
package wvlet.lang.catalog

import wvlet.airframe.codec.MessageCodec
import wvlet.airspec.AirSpec
import wvlet.lang.catalog.SQLFunction.FunctionType
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType

class DataTypeCodecTest extends AirSpec:

  test("serialize and deserialize DataType") {
    val testCases = List(
      DataType.IntType,
      DataType.StringType,
      DataType.DoubleType,
      DataType.ArrayType(DataType.IntType),
      DataType.MapType(DataType.StringType, DataType.IntType),
      DataType.DecimalType.of(18, 3),
      // DataType.ArrayType(DataType.DecimalType.of(18, 3)) // This can't be parsed back from string
    )

    testCases.foreach { dataType =>
      // Test direct codec
      val json = DataTypeCodec.toJson(dataType)
      debug(s"DataType: $dataType -> JSON: $json")
      val decoded = DataTypeCodec.fromJson(json)
      decoded shouldBe dataType
    }
  }

  test("serialize and deserialize SQLFunction with DataType") {
    val function = SQLFunction(
      name = "sum",
      functionType = FunctionType.AGGREGATE,
      args = Seq(DataType.DecimalType.of(18, 3)),
      returnType = DataType.DecimalType.of(38, 3)
    )

    val json = CatalogSerializer.serializeFunctions(List(function))
    val decoded = CatalogSerializer.deserializeFunctions(json)
    
    decoded.size shouldBe 1
    val decodedFunc = decoded.head
    decodedFunc.name shouldBe "sum"
    decodedFunc.functionType shouldBe FunctionType.AGGREGATE
    decodedFunc.args.size shouldBe 1
    decodedFunc.args.head.toString shouldBe "decimal(18,3)"
    decodedFunc.returnType.toString shouldBe "decimal(38,3)"
  }

  test("handle complex nested DataTypes") {
    val complexType = DataType.ArrayType(
      DataType.MapType(
        DataType.StringType,
        DataType.ArrayType(DataType.IntType)
      )
    )
    val json = DataTypeCodec.toJson(complexType)
    val decoded = DataTypeCodec.fromJson(json)
    decoded.toString shouldBe complexType.toString
  }

  test("fallback to GenericType for unparseable types") {
    // These type strings can't be parsed by DataType.parse()
    val unparseableTypes = List(
      "array(decimal(18,3))",
      "struct(a:int,b:string)"
    )
    
    unparseableTypes.foreach { typeStr =>
      val json = s""""$typeStr""""
      val decoded = DataTypeCodec.fromJson(json)
      decoded.isInstanceOf[DataType.GenericType] shouldBe true
      decoded.toString shouldBe typeStr
    }
  }

end DataTypeCodecTest
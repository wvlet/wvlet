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
package wvlet.lang.model

import wvlet.lang.compiler.Name
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

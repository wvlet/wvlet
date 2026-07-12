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

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableName
import wvlet.uni.test.UniTest
import wvlet.uni.weaver.Weaver
import wvlet.uni.weaver.codec.PrimitiveWeaver.given

class DataTypeWeaverTest extends UniTest:

  private val weaver = summon[Weaver[DataType]]

  private def roundTrip(typeExpr: String): Unit =
    val dt       = DataType.parse(typeExpr)
    val json     = weaver.toJson(dt)
    val restored = weaver.fromJson(json)
    restored shouldBe dt

  test("round-trip primitive types") {
    roundTrip("long")
    roundTrip("int")
    roundTrip("string")
    roundTrip("boolean")
    roundTrip("double")
    roundTrip("date")
    roundTrip("json")
    roundTrip("null")
  }

  test("round-trip parameterized types") {
    roundTrip("decimal(10,2)")
    roundTrip("decimal(38,0)")
    roundTrip("varchar(10)")
    roundTrip("char(3)")
  }

  test("round-trip nested types") {
    roundTrip("array(string)")
    roundTrip("array(array(int))")
    roundTrip("array(decimal(10,2))")
    roundTrip("map(string,int)")
    roundTrip("map(string,decimal(10,2))")
    roundTrip("array(timestamp with time zone)")
  }

  test("round-trip timestamp types") {
    roundTrip("timestamp")
    roundTrip("timestamp(0) with time zone")
    roundTrip("timestamp with time zone")
    roundTrip("timestamp without time zone")
    roundTrip("time with time zone")
  }

  test("round-trip TableDef through the derived weaver as in ConnectorCatalog") {
    // Mirrors the JSON cache codec in ConnectorCatalog (#1891)
    val tableDefCodec = summon[Weaver[List[Catalog.TableDef]]]

    val defs = List(
      Catalog.TableDef(
        tableName = TableName(Some("memory"), Some("sales"), "orders"),
        columns = List(
          Catalog.TableColumn("order_id", DataType.parse("long")),
          Catalog.TableColumn("price", DataType.parse("decimal(10,2)")),
          Catalog.TableColumn("tags", DataType.parse("array(string)")),
          Catalog.TableColumn("created_at", DataType.parse("timestamp with time zone"))
        )
      )
    )

    val json     = tableDefCodec.toJson(defs)
    val restored = tableDefCodec.fromJson(json)
    restored.size shouldBe 1
    restored.head.tableName shouldBe defs.head.tableName
    restored.head.columns.map(_.name) shouldBe defs.head.columns.map(_.name)
    // The essential #1891 assertion: column data types survive the round-trip
    restored.head.columns.map(_.dataType) shouldBe defs.head.columns.map(_.dataType)
  }

end DataTypeWeaverTest

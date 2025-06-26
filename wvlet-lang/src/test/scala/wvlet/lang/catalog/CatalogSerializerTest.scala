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

import wvlet.airspec.AirSpec
import wvlet.lang.model.DataType

class CatalogSerializerTest extends AirSpec:

  test("serialize and deserialize table definitions") {
    val tables = List(
      Catalog.TableDef(
        tableName = Catalog.TableName(Some("catalog"), Some("schema"), "table1"),
        columns = List(
          Catalog.TableColumn("id", DataType.LongType),
          Catalog.TableColumn("name", DataType.StringType),
          Catalog.TableColumn("data", DataType.JsonType)
        ),
        description = "Test table 1"
      ),
      Catalog.TableDef(
        tableName = Catalog.TableName(None, Some("schema"), "table2"),
        columns = List(
          Catalog.TableColumn("value", DataType.DoubleType),
          Catalog.TableColumn(
            "timestamp",
            DataType.TimestampType(DataType.TimestampField.TIMESTAMP, false)
          )
        )
      )
    )

    val json         = CatalogSerializer.serializeTables(tables)
    val deserialized = CatalogSerializer.deserializeTables(json)

    deserialized.size shouldBe 2
    deserialized(0).name shouldBe "table1"
    deserialized(0).tableName.catalog shouldBe Some("catalog")
    deserialized(0).tableName.schema shouldBe Some("schema")
    deserialized(0).columns.size shouldBe 3
    deserialized(0).description shouldBe "Test table 1"

    deserialized(1).name shouldBe "table2"
    deserialized(1).tableName.catalog shouldBe None
    deserialized(1).columns.map(_.name) shouldBe List("value", "timestamp")
  }

  test("serialize and deserialize SQL functions") {
    val functions = List(
      SQLFunction(
        name = "sum",
        functionType = SQLFunction.FunctionType.AGGREGATE,
        args = List(DataType.DoubleType),
        returnType = DataType.DoubleType
      ),
      SQLFunction(
        name = "substring",
        functionType = SQLFunction.FunctionType.SCALAR,
        args = List(DataType.StringType, DataType.IntType, DataType.IntType),
        returnType = DataType.StringType
      )
    )

    val json         = CatalogSerializer.serializeFunctions(functions)
    val deserialized = CatalogSerializer.deserializeFunctions(json)

    deserialized.size shouldBe 2
    deserialized(0).name shouldBe "sum"
    deserialized(0).functionType shouldBe SQLFunction.FunctionType.AGGREGATE
    deserialized(0).args shouldBe List(DataType.DoubleType)
    deserialized(0).returnType shouldBe DataType.DoubleType

    deserialized(1).name shouldBe "substring"
    deserialized(1).functionType shouldBe SQLFunction.FunctionType.SCALAR
    deserialized(1).args.size shouldBe 3
  }

  test("serialize and deserialize schemas") {
    val schemas = List(
      Catalog.TableSchema(
        catalog = Some("test_catalog"),
        name = "main",
        description = "Main schema",
        properties = Map("key1" -> "value1")
      ),
      Catalog.TableSchema(catalog = None, name = "analytics", description = "Analytics schema")
    )

    val json         = CatalogSerializer.serializeSchemas(schemas)
    val deserialized = CatalogSerializer.deserializeSchemas(json)

    deserialized.size shouldBe 2
    deserialized(0).catalog shouldBe Some("test_catalog")
    deserialized(0).name shouldBe "main"
    deserialized(0).description shouldBe "Main schema"
    deserialized(0).properties shouldBe Map("key1" -> "value1")

    deserialized(1).catalog shouldBe None
    deserialized(1).name shouldBe "analytics"
  }

  test("serialize and deserialize complete catalog metadata") {
    val metadata = CatalogSerializer.CatalogMetadata(
      catalogName = "test_catalog",
      dbType = "DuckDB",
      schemas = List(Catalog.TableSchema(Some("test_catalog"), "main", "Main schema")),
      tables = Map(
        "main" ->
          List(
            Catalog.TableDef(
              tableName = Catalog.TableName(Some("test_catalog"), Some("main"), "users"),
              columns = List(
                Catalog.TableColumn("id", DataType.LongType),
                Catalog.TableColumn("name", DataType.StringType)
              )
            )
          )
      ),
      functions = List(
        SQLFunction(
          "count",
          SQLFunction.FunctionType.AGGREGATE,
          List(DataType.AnyType),
          DataType.LongType
        )
      ),
      version = "1.0"
    )

    val json         = CatalogSerializer.serializeCatalog(metadata)
    val deserialized = CatalogSerializer.deserializeCatalog(json)

    deserialized.catalogName shouldBe "test_catalog"
    deserialized.dbType shouldBe "DuckDB"
    deserialized.version shouldBe "1.0"
    deserialized.schemas.size shouldBe 1
    deserialized.tables.size shouldBe 1
    deserialized.tables("main").size shouldBe 1
    deserialized.functions.size shouldBe 1
  }

end CatalogSerializerTest

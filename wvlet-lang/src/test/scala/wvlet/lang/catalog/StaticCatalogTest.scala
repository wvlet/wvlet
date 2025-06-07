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
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.lang.compiler.DBType
import wvlet.lang.model.DataType
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class StaticCatalogTest extends AirSpec:

  test("create and query static catalog") {
    val catalog = StaticCatalog(
      catalogName = "test_catalog",
      dbType = DBType.DuckDB,
      schemas = List(
        Catalog.TableSchema(Some("test_catalog"), "main", "Main schema"),
        Catalog.TableSchema(Some("test_catalog"), "analytics", "Analytics schema")
      ),
      tablesBySchema = Map(
        "main" ->
          List(
            Catalog.TableDef(
              tableName = Catalog.TableName(Some("test_catalog"), Some("main"), "users"),
              columns = List(
                Catalog.TableColumn("id", DataType.LongType),
                Catalog.TableColumn("name", DataType.StringType),
                Catalog.TableColumn("email", DataType.StringType)
              )
            ),
            Catalog.TableDef(
              tableName = Catalog.TableName(Some("test_catalog"), Some("main"), "orders"),
              columns = List(
                Catalog.TableColumn("order_id", DataType.LongType),
                Catalog.TableColumn("user_id", DataType.LongType),
                Catalog.TableColumn("amount", DataType.DoubleType)
              )
            )
          ),
        "analytics" ->
          List(
            Catalog.TableDef(
              tableName = Catalog.TableName(Some("test_catalog"), Some("analytics"), "metrics"),
              columns = List(
                Catalog.TableColumn("metric_name", DataType.StringType),
                Catalog.TableColumn("value", DataType.DoubleType),
                Catalog.TableColumn(
                  "timestamp",
                  DataType.TimestampType(DataType.TimestampField.TIMESTAMP, false)
                )
              )
            )
          )
      ),
      functions = List(
        SQLFunction(
          "sum",
          SQLFunction.FunctionType.AGGREGATE,
          List(DataType.AnyType),
          DataType.DoubleType
        ),
        SQLFunction(
          "concat",
          SQLFunction.FunctionType.SCALAR,
          List(DataType.StringType, DataType.StringType),
          DataType.StringType
        )
      )
    )

    // Test catalog properties
    catalog.catalogName shouldBe "test_catalog"
    catalog.dbType shouldBe DBType.DuckDB

    // Test schema operations
    catalog.listSchemaNames shouldBe Seq("main", "analytics")
    catalog.schemaExists("main") shouldBe true
    catalog.schemaExists("nonexistent") shouldBe false

    val mainSchema = catalog.getSchema("main")
    mainSchema.name shouldBe "main"
    mainSchema.description shouldBe "Main schema"

    // Test table operations
    catalog.listTableNames("main") shouldBe Seq("users", "orders")
    catalog.listTableNames("analytics") shouldBe Seq("metrics")

    catalog.tableExists("main", "users") shouldBe true
    catalog.tableExists("main", "nonexistent") shouldBe false

    val usersTable = catalog.getTable("main", "users")
    usersTable.name shouldBe "users"
    usersTable.columns.size shouldBe 3
    usersTable.columns.map(_.name) shouldBe Seq("id", "name", "email")

    // Test findTable
    catalog.findTable("main", "users").isDefined shouldBe true
    catalog.findTable("main", "nonexistent") shouldBe None

    // Test functions
    catalog.listFunctions.size shouldBe 2
    catalog.listFunctions.map(_.name) shouldBe Seq("sum", "concat")
  }

  test("throw exceptions for unsupported operations") {
    val catalog = StaticCatalog(
      catalogName = "test",
      dbType = DBType.DuckDB,
      schemas = List.empty,
      tablesBySchema = Map.empty,
      functions = List.empty
    )

    intercept[WvletLangException] {
      catalog.createSchema(
        Catalog.TableSchema(None, "new_schema"),
        Catalog.CreateMode.CREATE_IF_NOT_EXISTS
      )
    }

    intercept[WvletLangException] {
      catalog.createTable(
        Catalog.TableDef(Catalog.TableName(None, None, "new_table"), List.empty),
        Catalog.CreateMode.CREATE_IF_NOT_EXISTS
      )
    }

    intercept[WvletLangException] {
      catalog.updateColumns("schema", "table", List.empty)
    }
  }

  test("throw exceptions for missing resources") {
    val catalog = StaticCatalog(
      catalogName = "test",
      dbType = DBType.DuckDB,
      schemas = List(Catalog.TableSchema(None, "main")),
      tablesBySchema = Map("main" -> List.empty),
      functions = List.empty
    )

    intercept[WvletLangException] {
      catalog.getSchema("nonexistent")
    }

    intercept[WvletLangException] {
      catalog.getTable("main", "nonexistent")
    }
  }

end StaticCatalogTest

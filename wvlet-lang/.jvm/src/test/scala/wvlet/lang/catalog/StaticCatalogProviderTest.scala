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
import wvlet.lang.compiler.DBType
import wvlet.lang.model.DataType
import java.nio.file.{Files, Path}
import scala.util.Using

class StaticCatalogProviderTest extends AirSpec:

  private def withTempDirectory[T](f: Path => T): T =
    val tempDir = Files.createTempDirectory("static-catalog-test")
    try
      f(tempDir)
    finally
      // Clean up
      def deleteRecursively(path: Path): Unit =
        if Files.isDirectory(path) then
          Files.list(path).forEach(deleteRecursively)
        Files.deleteIfExists(path)
      deleteRecursively(tempDir)

  test("load static catalog from directory") {
    withTempDirectory { tempDir =>
      // Create catalog directory structure
      val catalogDir = tempDir.resolve("duckdb").resolve("test_catalog")
      Files.createDirectories(catalogDir)

      // Write schemas.json
      val schemas = List(
        Catalog.TableSchema(Some("test_catalog"), "main", "Main schema"),
        Catalog.TableSchema(Some("test_catalog"), "analytics", "Analytics schema")
      )
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )

      // Write main.json (tables in main schema)
      val mainTables = List(
        Catalog.TableDef(
          tableName = Catalog.TableName(Some("test_catalog"), Some("main"), "users"),
          columns = List(
            Catalog.TableColumn("id", DataType.LongType),
            Catalog.TableColumn("name", DataType.StringType)
          )
        )
      )
      Files.writeString(
        catalogDir.resolve("main.json"),
        CatalogSerializer.serializeTables(mainTables)
      )

      // Write functions.json
      val functions = List(
        SQLFunction(
          "sum",
          SQLFunction.FunctionType.AGGREGATE,
          List(DataType.DoubleType),
          DataType.DoubleType
        )
      )
      Files.writeString(
        catalogDir.resolve("functions.json"),
        CatalogSerializer.serializeFunctions(functions)
      )

      // Load the catalog
      val loadedCatalog = StaticCatalogProvider.loadCatalog("test_catalog", DBType.DuckDB, tempDir)

      loadedCatalog.isDefined shouldBe true
      val catalog = loadedCatalog.get

      catalog.catalogName shouldBe "test_catalog"
      catalog.dbType shouldBe DBType.DuckDB
      catalog.listSchemaNames shouldBe Seq("main", "analytics")
      catalog.listTableNames("main") shouldBe Seq("users")
      catalog.listFunctions.size shouldBe 1
    }
  }

  test("handle missing catalog gracefully") {
    withTempDirectory { tempDir =>
      val loadedCatalog = StaticCatalogProvider.loadCatalog("nonexistent", DBType.DuckDB, tempDir)
      loadedCatalog shouldBe None
    }
  }

  test("load catalog without optional files") {
    withTempDirectory { tempDir =>
      // Create minimal catalog with only schemas
      val catalogDir = tempDir.resolve("duckdb").resolve("minimal")
      Files.createDirectories(catalogDir)

      val schemas = List(Catalog.TableSchema(Some("minimal"), "default", "Default schema"))
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )

      val loadedCatalog = StaticCatalogProvider.loadCatalog("minimal", DBType.DuckDB, tempDir)

      loadedCatalog.isDefined shouldBe true
      val catalog = loadedCatalog.get

      catalog.catalogName shouldBe "minimal"
      catalog.listSchemaNames shouldBe Seq("default")
      catalog.listTableNames("default") shouldBe Seq.empty
      catalog.listFunctions shouldBe Seq.empty
    }
  }

  test("list available catalogs") {
    withTempDirectory { tempDir =>
      // Create multiple catalog directories
      Files.createDirectories(tempDir.resolve("duckdb").resolve("catalog1"))
      Files.createDirectories(tempDir.resolve("duckdb").resolve("catalog2"))
      Files.createDirectories(tempDir.resolve("trino").resolve("catalog3"))

      val availableCatalogs = StaticCatalogProvider.listAvailableCatalogs(tempDir)

      availableCatalogs.size shouldBe 3
      availableCatalogs.contains(("catalog1", DBType.DuckDB)) shouldBe true
      availableCatalogs.contains(("catalog2", DBType.DuckDB)) shouldBe true
      availableCatalogs.contains(("catalog3", DBType.Trino)) shouldBe true
    }
  }

  test("handle corrupted catalog files") {
    withTempDirectory { tempDir =>
      val catalogDir = tempDir.resolve("duckdb").resolve("corrupted")
      Files.createDirectories(catalogDir)

      // Write invalid JSON
      Files.writeString(catalogDir.resolve("schemas.json"), "{ invalid json")

      val loadedCatalog = StaticCatalogProvider.loadCatalog("corrupted", DBType.DuckDB, tempDir)
      loadedCatalog shouldBe None
    }
  }

end StaticCatalogProviderTest

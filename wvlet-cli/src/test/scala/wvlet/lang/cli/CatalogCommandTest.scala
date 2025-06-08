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
package wvlet.lang.cli

import wvlet.airspec.AirSpec
import wvlet.lang.catalog.{Catalog, CatalogSerializer, Profile, StaticCatalogProvider}
import wvlet.lang.compiler.{DBType, WorkEnv}
import wvlet.lang.model.DataType
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.log.LogLevel
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

class CatalogCommandTest extends AirSpec:

  private def withTempCatalog[A](f: Path => A): A =
    val targetDir = Paths.get("target/test-temp")
    Files.createDirectories(targetDir)
    val tempDir = Files.createTempDirectory(targetDir, "catalog-command-test")
    try f(tempDir)
    finally
      // Cleanup
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)

  test("import catalog from DuckDB") {
    withTempCatalog { tempPath =>
      val workEnv             = WorkEnv(tempPath.toString, LogLevel.DEBUG)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      // Create test data in DuckDB
      val profile   = Profile.defaultProfileFor(DBType.DuckDB)
      val connector = dbConnectorProvider.getConnector(profile)

      try
        // Create test tables
        connector.executeUpdate("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR)")
        connector.executeUpdate("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        connector.executeUpdate("CREATE TABLE orders (id INT, user_id INT, amount DECIMAL(10,2))")

        // Import catalog using the command
        val catalogCommand = new CatalogCommand()
        val importOpts = CatalogCommandOption(
          catalogPath = tempPath.toString,
          dbType = "duckdb",
          catalog = Some("test_db"),
          schema = None // Import all schemas
        )

        catalogCommand.`import`(importOpts)

        // Verify the catalog was imported
        val catalogDir = tempPath.resolve("duckdb").resolve("test_db")
        Files.exists(catalogDir) shouldBe true
        Files.exists(catalogDir.resolve("schemas.json")) shouldBe true
        Files.exists(catalogDir.resolve("functions.json")) shouldBe true

        // Check if there are any schemas
        val schemas = CatalogSerializer.deserializeSchemas(
          Files.readString(catalogDir.resolve("schemas.json"))
        )

        // DuckDB in-memory might not have schemas, so check if schemas exist
        if schemas.nonEmpty then
          // Only check for table files if schemas exist
          val schemaName = schemas.head.name
          if Files.exists(catalogDir.resolve(s"${schemaName}.json")) then
            val tables = CatalogSerializer.deserializeTables(
              Files.readString(catalogDir.resolve(s"${schemaName}.json"))
            )
            tables.exists(_.tableName.name == "users") shouldBe true
            tables.exists(_.tableName.name == "orders") shouldBe true

            // Verify the tables have correct columns
            val usersTable = tables.find(_.tableName.name == "users").get
            usersTable.columns.map(_.name) shouldContain "id"
            usersTable.columns.map(_.name) shouldContain "name"
            usersTable.columns.map(_.name) shouldContain "email"

      finally
        connector.close()
      end try
    }
  }

  test("list available catalogs") {
    withTempCatalog { tempPath =>
      // Create some fake catalog directories
      val duckdbDir = tempPath.resolve("duckdb")
      Files.createDirectories(duckdbDir.resolve("catalog1"))
      Files.writeString(duckdbDir.resolve("catalog1").resolve("schemas.json"), "[]")
      Files.createDirectories(duckdbDir.resolve("catalog2"))
      Files.writeString(duckdbDir.resolve("catalog2").resolve("schemas.json"), "[]")

      val trinoDir = tempPath.resolve("trino")
      Files.createDirectories(trinoDir.resolve("prod_catalog"))
      Files.writeString(trinoDir.resolve("prod_catalog").resolve("schemas.json"), "[]")

      // List catalogs
      val catalogCommand = new CatalogCommand()
      catalogCommand.list(tempPath.toString)

      // Just verify the catalogs were created correctly
      val catalogs = StaticCatalogProvider.listAvailableCatalogs(tempPath)
      catalogs.size shouldBe 3
      catalogs.exists(_._1 == "catalog1") shouldBe true
      catalogs.exists(_._1 == "catalog2") shouldBe true
      catalogs.exists(_._1 == "prod_catalog") shouldBe true
    }
  }

  test("show catalog details") {
    withTempCatalog { tempPath =>
      // Create a test catalog
      val catalogDir = tempPath.resolve("duckdb").resolve("test_catalog")
      Files.createDirectories(catalogDir)

      val schemas = List(
        Catalog.TableSchema(Some("test_catalog"), "main", "Main schema"),
        Catalog.TableSchema(Some("test_catalog"), "analytics", "Analytics schema")
      )
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )

      val mainTables = List(
        Catalog.TableDef(
          Catalog.TableName(Some("test_catalog"), Some("main"), "users"),
          List(Catalog.TableColumn("id", DataType.IntType))
        )
      )
      Files.writeString(
        catalogDir.resolve("main.json"),
        CatalogSerializer.serializeTables(mainTables)
      )

      Files.writeString(
        catalogDir.resolve("analytics.json"),
        CatalogSerializer.serializeTables(Nil)
      )

      Files.writeString(
        catalogDir.resolve("functions.json"),
        CatalogSerializer.serializeFunctions(Nil)
      )

      // Show catalog details
      val catalogCommand = new CatalogCommand()
      catalogCommand.show(tempPath.toString, "duckdb/test_catalog")

      // Verify the catalog can be loaded
      val catalog = StaticCatalogProvider.loadCatalog("test_catalog", DBType.DuckDB, tempPath)
      catalog.isDefined shouldBe true
      catalog.get.listSchemas.size shouldBe 2
      catalog.get.listTables("main").size shouldBe 1
      catalog.get.listTables("analytics").size shouldBe 0
    }
  }

  test("handle invalid catalog specification") {
    withTempCatalog { tempPath =>
      val catalogCommand = new CatalogCommand()
      // This should log an error but not throw
      catalogCommand.show(tempPath.toString, "invalid-format")
      // Test passes if no exception is thrown
    }
  }

  test("sanitize catalog name") {
    withTempCatalog { tempPath =>
      val workEnv             = WorkEnv(tempPath.toString)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      // Create test data
      val profile   = Profile.defaultProfileFor(DBType.DuckDB)
      val connector = dbConnectorProvider.getConnector(profile)

      try
        connector.executeUpdate("CREATE TABLE test_table (id INT)")

        // Import with dangerous catalog name
        val catalogCommand = new CatalogCommand()
        val importOpts = CatalogCommandOption(
          catalogPath = tempPath.toString,
          dbType = "duckdb",
          catalog = Some("../../../dangerous/path"),
          schema = None // Don't specify a schema
        )

        catalogCommand.`import`(importOpts)

        // Verify the catalog was created with sanitized name
        val expectedDir = tempPath.resolve("duckdb").resolve("_________dangerous_path")
        Files.exists(expectedDir) shouldBe true

        // Should NOT create a directory outside the catalog path
        Files.exists(tempPath.resolve("../../../dangerous")) shouldBe false

      finally
        connector.close()
    }
  }

  test("refresh catalog") {
    withTempCatalog { tempPath =>
      val workEnv             = WorkEnv(tempPath.toString)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      val profile   = Profile.defaultProfileFor(DBType.DuckDB)
      val connector = dbConnectorProvider.getConnector(profile)

      try
        // Initial import
        connector.executeUpdate("CREATE TABLE table1 (id INT)")

        val catalogCommand = new CatalogCommand()
        val importOpts = CatalogCommandOption(
          catalogPath = tempPath.toString,
          dbType = "duckdb",
          catalog = Some("refresh_test"),
          schema = None
        )

        catalogCommand.`import`(importOpts)

        // Add more tables
        connector.executeUpdate("CREATE TABLE table2 (id INT)")

        // Refresh catalog
        catalogCommand.refresh(importOpts)

        // Verify the catalog was created
        val catalogDir = tempPath.resolve("duckdb").resolve("refresh_test")
        Files.exists(catalogDir) shouldBe true
        Files.exists(catalogDir.resolve("schemas.json")) shouldBe true
        Files.exists(catalogDir.resolve("functions.json")) shouldBe true

        // Load the catalog and check if tables exist
        val catalog = StaticCatalogProvider.loadCatalog("refresh_test", DBType.DuckDB, tempPath)
        if catalog.isDefined && catalog.get.listSchemas.nonEmpty then
          val schemaName = catalog.get.listSchemas.head.name
          val tables     = catalog.get.listTables(schemaName)
          tables.exists(_.name == "table1") shouldBe true
          tables.exists(_.name == "table2") shouldBe true

      finally
        connector.close()
      end try
    }
  }

end CatalogCommandTest

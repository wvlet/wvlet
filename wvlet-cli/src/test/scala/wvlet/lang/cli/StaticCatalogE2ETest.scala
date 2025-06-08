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
import wvlet.lang.catalog.{Catalog, CatalogSerializer, Profile, SQLFunction, StaticCatalogProvider}
import wvlet.lang.compiler.{CompilerOptions, DBType, WorkEnv}
import wvlet.lang.model.DataType
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/**
  * End-to-end test for static catalog functionality
  */
class StaticCatalogE2ETest extends AirSpec:

  private def withTempCatalog[A](f: Path => A): A =
    val targetDir = Paths.get("target/test-temp")
    Files.createDirectories(targetDir)
    val tempDir = Files.createTempDirectory(targetDir, "static-catalog-e2e-test")
    try f(tempDir)
    finally
      // Cleanup
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)

  test("end-to-end: import catalog and compile with it") {
    withTempCatalog { tempPath =>
      val workEnv             = WorkEnv(tempPath.toString)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      // Phase 1: Import catalog from a real database (in-memory DuckDB)
      val dbType      = DBType.DuckDB
      val catalogName = "mydb"

      // Create a connector and populate some test data
      val profile   = Profile.defaultProfileFor(dbType)
      val connector = dbConnectorProvider.getConnector(profile)
      try
        // Create a test table in DuckDB
        connector.executeUpdate(
          "CREATE TABLE employees (id INT, name VARCHAR, salary DECIMAL(10,2))"
        )
        connector.executeUpdate(
          "INSERT INTO employees VALUES (1, 'Alice', 50000.00), (2, 'Bob', 60000.00)"
        )

        // Import the catalog - use "memory" for in-memory DuckDB
        val catalog     = connector.getCatalog("memory", "main")
        val catalogPath = tempPath.resolve("catalog")
        val catalogDir  = catalogPath.resolve(dbType.toString.toLowerCase).resolve(catalogName)
        Files.createDirectories(catalogDir)

        // Get schemas
        val schemas = catalog.listSchemas
        // In-memory DuckDB might have 0 or more schemas
        if schemas.nonEmpty then
          schemas.exists(_.name == "main") shouldBe true

        // Get tables
        val tables = catalog.listTables("main")
        tables.exists(_.name == "employees") shouldBe true

        // Get functions
        val functions = catalog.listFunctions
        (functions.size > 100) shouldBe true // DuckDB has many built-in functions

        // Write catalog files
        val schemasToWrite =
          if schemas.isEmpty then
            List(Catalog.TableSchema(Some(catalogName), "main", "Main schema"))
          else
            schemas.toList
        Files.writeString(
          catalogDir.resolve("schemas.json"),
          CatalogSerializer.serializeSchemas(schemasToWrite)
        )
        Files.writeString(
          catalogDir.resolve("main.json"),
          CatalogSerializer.serializeTables(tables.toList)
        )
        Files.writeString(
          catalogDir.resolve("functions.json"),
          CatalogSerializer.serializeFunctions(functions.toList)
        )

      finally
        connector.close()
      end try

      // Phase 2: Load static catalog and verify
      val loadedCatalog = StaticCatalogProvider.loadCatalog(
        catalogName,
        dbType,
        tempPath.resolve("catalog")
      )
      loadedCatalog.isDefined shouldBe true

      val staticCatalog = loadedCatalog.get
      staticCatalog.catalogName shouldBe catalogName
      staticCatalog.dbType shouldBe dbType

      // Verify table is accessible
      val employeeTable = staticCatalog.findTable("main", "employees")
      employeeTable.isDefined shouldBe true
      employeeTable.get.columns.map(_.name) shouldContain "id"
      employeeTable.get.columns.map(_.name) shouldContain "name"
      employeeTable.get.columns.map(_.name) shouldContain "salary"

      // Verify functions are available
      val sumFunction = staticCatalog.listFunctions.find(_.name == "sum")
      sumFunction.isDefined shouldBe true

      // Phase 3: Compile a query using static catalog
      val queryFile = tempPath.resolve("query.wv")
      Files.writeString(
        queryFile,
        """
        |from employees
        |where salary > 55000
        |select name, sum(salary) as total_salary
      """.stripMargin
      )

      val compilerOpts = WvletCompilerOption(
        workFolder = tempPath.toString,
        file = Some("query.wv"),
        targetDBType = Some("duckdb"),
        useStaticCatalog = true,
        staticCatalogPath = Some(tempPath.resolve("catalog").toString),
        catalog = Some(catalogName)
      )

      val compiler =
        new WvletCompiler(WvletGlobalOption(), compilerOpts, workEnv, dbConnectorProvider)

      try
        // Generate SQL using static catalog
        val sql = compiler.generateSQL

        // Verify SQL contains expected elements
        sql shouldContain "employees"
        sql shouldContain "salary"
        sql shouldContain "sum("
        sql shouldContain "55000"

        // The SQL should be valid DuckDB SQL
        sql.toLowerCase shouldContain "select"
        sql.toLowerCase shouldContain "from"
        sql.toLowerCase shouldContain "where"

      finally
        compiler.close()
    }
  }

  test("static catalog with multiple schemas") {
    withTempCatalog { tempPath =>
      val catalogPath = tempPath.resolve("catalog")
      val dbType      = DBType.DuckDB
      val catalogName = "multi_schema"
      val catalogDir  = catalogPath.resolve(dbType.toString.toLowerCase).resolve(catalogName)
      Files.createDirectories(catalogDir)

      // Create catalog with multiple schemas
      val schemas = List(
        Catalog.TableSchema(Some(catalogName), "public", "Public schema"),
        Catalog.TableSchema(Some(catalogName), "analytics", "Analytics schema")
      )

      val publicTables = List(
        Catalog.TableDef(
          Catalog.TableName(Some(catalogName), Some("public"), "users"),
          List(
            Catalog.TableColumn("id", DataType.IntType),
            Catalog.TableColumn("email", DataType.StringType)
          )
        )
      )

      val analyticsTables = List(
        Catalog.TableDef(
          Catalog.TableName(Some(catalogName), Some("analytics"), "daily_stats"),
          List(
            Catalog.TableColumn("date", DataType.StringType),
            Catalog.TableColumn("user_count", DataType.LongType),
            Catalog.TableColumn("revenue", DataType.DoubleType)
          )
        )
      )

      // Write catalog files
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )
      Files.writeString(
        catalogDir.resolve("public.json"),
        CatalogSerializer.serializeTables(publicTables)
      )
      Files.writeString(
        catalogDir.resolve("analytics.json"),
        CatalogSerializer.serializeTables(analyticsTables)
      )
      Files.writeString(
        catalogDir.resolve("functions.json"),
        CatalogSerializer.serializeFunctions(Nil)
      )

      // Load and verify
      val catalog = StaticCatalogProvider.loadCatalog(catalogName, dbType, catalogPath).get

      catalog.listSchemas.size shouldBe 2
      catalog.listTables("public").size shouldBe 1
      catalog.listTables("analytics").size shouldBe 1

      catalog.findTable("public", "users").isDefined shouldBe true
      catalog.findTable("analytics", "daily_stats").isDefined shouldBe true
    }
  }

end StaticCatalogE2ETest

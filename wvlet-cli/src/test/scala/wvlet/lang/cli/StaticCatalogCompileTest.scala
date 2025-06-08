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
import wvlet.lang.catalog.{Catalog, CatalogSerializer, StaticCatalogProvider}
import wvlet.lang.compiler.{DBType, WorkEnv}
import wvlet.lang.model.DataType
import wvlet.lang.runner.connector.DBConnectorProvider
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

class StaticCatalogCompileTest extends AirSpec:

  private def withTempCatalog[A](f: Path => A): A =
    val tempDir = Files.createTempDirectory("static-catalog-test")
    try
      f(tempDir)
    finally
      // Cleanup
      Files
        .walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)

  test("compile with static catalog") {
    withTempCatalog { catalogPath =>
      // Create a simple static catalog
      val dbType      = DBType.DuckDB
      val catalogName = "test"
      val catalogDir  = catalogPath.resolve(dbType.toString.toLowerCase).resolve(catalogName)
      Files.createDirectories(catalogDir)

      // Create minimal catalog metadata
      val schemas = List(
        Catalog.TableSchema(
          catalog = Some(catalogName),
          name = "main",
          description = "Main schema"
        )
      )
      val tables = List(
        Catalog.TableDef(
          tableName = Catalog.TableName(Some(catalogName), Some("main"), "users"),
          columns = List(
            Catalog.TableColumn(name = "id", dataType = DataType.IntType),
            Catalog.TableColumn(name = "name", dataType = DataType.StringType)
          ),
          description = "Users table"
        )
      )

      // Write catalog files
      Files.writeString(catalogDir.resolve("schemas.json"), CatalogSerializer.serializeSchemas(schemas))
      Files.writeString(catalogDir.resolve("main.json"), CatalogSerializer.serializeTables(tables))
      Files.writeString(catalogDir.resolve("functions.json"), CatalogSerializer.serializeFunctions(Nil))

      // Create a test query that references the table
      val queryFile = catalogPath.resolve("test.wv")
      Files.writeString(queryFile, "from users select id, name")

      // Create compiler with static catalog
      val workEnv             = WorkEnv(catalogPath.toString)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      val compilerOpts = WvletCompilerOption(
        workFolder = catalogPath.toString,
        file = Some("test.wv"),  // Use relative path, not absolute
        targetDBType = Some("duckdb"),
        useStaticCatalog = true,
        staticCatalogPath = Some(catalogPath.toString),
        catalog = Some("test")
      )

      val compiler = new WvletCompiler(
        WvletGlobalOption(),
        compilerOpts,
        workEnv,
        dbConnectorProvider
      )

      try
        // Test that compilation succeeds with static catalog
        val sql = compiler.generateSQL
        
        // Verify the generated SQL references the table
        sql shouldContain "users"
        sql shouldContain "id"
        sql shouldContain "name"
      finally
        compiler.close()
    }
  }

  test("compile with --use-static-catalog flag defaults to ./catalog") {
    withTempCatalog { tempPath =>
      // Create catalog in the default location relative to temp path
      val catalogPath = tempPath.resolve("catalog")
      val dbType      = DBType.DuckDB
      val catalogName = "default"
      val catalogDir  = catalogPath.resolve(dbType.toString.toLowerCase).resolve(catalogName)
      Files.createDirectories(catalogDir)

      // Create minimal catalog
      val schemas = List(
        Catalog.TableSchema(
          catalog = Some(catalogName),
          name = "main",
          description = "Main schema"
        )
      )
      Files.writeString(catalogDir.resolve("schemas.json"), CatalogSerializer.serializeSchemas(schemas))
      Files.writeString(catalogDir.resolve("functions.json"), CatalogSerializer.serializeFunctions(Nil))

      // Create a simple query
      val queryFile = tempPath.resolve("test.wv")
      Files.writeString(queryFile, "from values (1, 'a') as t(id, name) select *")

      val workEnv             = WorkEnv(tempPath.toString)
      val dbConnectorProvider = DBConnectorProvider(workEnv)

      val compilerOpts = WvletCompilerOption(
        workFolder = tempPath.toString,
        file = Some("test.wv"),  // Use relative path, not absolute
        targetDBType = Some("duckdb"),
        useStaticCatalog = true
        // Note: staticCatalogPath is not specified, should default to ./catalog
      )

      val compiler = new WvletCompiler(
        WvletGlobalOption(),
        compilerOpts,
        workEnv,
        dbConnectorProvider
      )

      try
        // Should compile successfully even without tables (using VALUES clause)
        val sql = compiler.generateSQL
        sql shouldContain "1"
        sql shouldContain "'a'"
      finally
        compiler.close()
    }
  }

end StaticCatalogCompileTest
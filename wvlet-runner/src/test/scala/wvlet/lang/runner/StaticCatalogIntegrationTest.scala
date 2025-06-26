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
package wvlet.lang.runner

import wvlet.airspec.AirSpec
import wvlet.lang.catalog.{Catalog, CatalogSerializer, StaticCatalogProvider}
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, DBType, SourceFile, WorkEnv}
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.model.DataType
import wvlet.log.{LogLevel, LogSupport}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/**
  * Integration test for static catalog functionality
  */
class StaticCatalogIntegrationTest extends AirSpec with LogSupport:

  private def withTempWorkspace[A](f: Path => A): A =
    val targetDir = Paths.get("target/test-temp")
    Files.createDirectories(targetDir)
    val tempDir = Files.createTempDirectory(targetDir, "static-catalog-integration")
    try f(tempDir)
    finally
      // Cleanup
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)

  test("static catalog can be loaded and used for compilation") {
    withTempWorkspace { workspace =>
      val catalogPath = workspace.resolve("catalog")
      val catalogDir  = catalogPath.resolve("duckdb").resolve("test_catalog")
      Files.createDirectories(catalogDir)

      // Create a simple catalog with one schema and table
      val schemas = List(Catalog.TableSchema(Some("test_catalog"), "main", "Main schema"))
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )

      val tables = List(
        Catalog.TableDef(
          Catalog.TableName(Some("test_catalog"), Some("main"), "users"),
          List(
            Catalog.TableColumn("id", DataType.IntType),
            Catalog.TableColumn("name", DataType.StringType),
            Catalog.TableColumn("email", DataType.StringType)
          ),
          "User table"
        )
      )
      Files.writeString(catalogDir.resolve("main.json"), CatalogSerializer.serializeTables(tables))
      Files.writeString(
        catalogDir.resolve("functions.json"),
        CatalogSerializer.serializeFunctions(Nil)
      )

      // Load the catalog
      val catalog = StaticCatalogProvider.loadCatalog("test_catalog", DBType.DuckDB, catalogPath)
      catalog.isDefined shouldBe true

      val loadedCatalog = catalog.get
      loadedCatalog.catalogName shouldBe "test_catalog"
      loadedCatalog.listSchemas.size shouldBe 1
      loadedCatalog.listTables("main").size shouldBe 1

      val userTable = loadedCatalog.findTable("main", "users")
      userTable.isDefined shouldBe true
      userTable.get.columns.size shouldBe 3
    }
  }

  test("compiler can use static catalog to resolve queries") {
    withTempWorkspace { workspace =>
      val catalogPath = workspace.resolve("catalog")
      val catalogDir  = catalogPath.resolve("duckdb").resolve("mydb")
      Files.createDirectories(catalogDir)

      // Create catalog metadata
      val schemas = List(Catalog.TableSchema(Some("mydb"), "main", "Main schema"))
      Files.writeString(
        catalogDir.resolve("schemas.json"),
        CatalogSerializer.serializeSchemas(schemas)
      )

      val tables = List(
        Catalog.TableDef(
          Catalog.TableName(Some("mydb"), Some("main"), "products"),
          List(
            Catalog.TableColumn("id", DataType.IntType),
            Catalog.TableColumn("name", DataType.StringType),
            Catalog.TableColumn("price", DataType.DoubleType)
          )
        )
      )
      Files.writeString(catalogDir.resolve("main.json"), CatalogSerializer.serializeTables(tables))
      Files.writeString(
        catalogDir.resolve("functions.json"),
        CatalogSerializer.serializeFunctions(Nil)
      )

      // Create a query file
      val queryFile = workspace.resolve("test_query.wv")
      Files.writeString(
        queryFile,
        """from products
          |where price > 100
          |select name, price
        """.stripMargin
      )

      // Compile with static catalog
      val workEnv = WorkEnv(workspace.toString, LogLevel.INFO)
      val compilerOptions = CompilerOptions(
        sourceFolders = List(workspace.toString),
        workEnv = workEnv,
        catalog = Some("mydb"),
        schema = Some("main"),
        dbType = DBType.DuckDB,
        useStaticCatalog = true,
        staticCatalogPath = Some(catalogPath.toString)
      )

      val compiler = Compiler(compilerOptions)
      val unit     = CompilationUnit(SourceFile.fromFile(queryFile.toString), false)
      val result   = compiler.compileSingleUnit(unit)

      result.hasFailures shouldBe false

      // Generate SQL
      val sql = GenSQL.generateSQL(unit)(using result.context)
      sql shouldContain "products"
      sql shouldContain "price"
      sql shouldContain ">"
    }
  }

end StaticCatalogIntegrationTest

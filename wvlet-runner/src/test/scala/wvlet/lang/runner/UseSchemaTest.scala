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
import wvlet.lang.api.{LinePosition, StatusCode}
import wvlet.lang.api.v1.query.{QueryRequest, QuerySelection}
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.log.LogLevel

class UseSchemaTest extends AirSpec:

  test("use schema should update both context and compiler") {
    val workEnv = WorkEnv(".", LogLevel.DEBUG)
    val compilerOptions = CompilerOptions(
      workEnv = workEnv,
      catalog = Some("memory"),
      schema = Some("main")
    )
    val compiler            = Compiler(compilerOptions)
    val dbConnectorProvider = DBConnectorProvider(workEnv)
    val queryExecutor = QueryExecutor(
      dbConnectorProvider = dbConnectorProvider,
      defaultProfile = Profile.defaultDuckDBProfile,
      workEnv = workEnv
    )

    // Initial state
    compiler.getDefaultSchema shouldBe "main"

    // Compile and execute use schema command
    val useSchemaQuery = "use schema test_schema"
    val unit           = CompilationUnit.fromWvletString(useSchemaQuery)
    val compileResult  = compiler.compileSingleUnit(unit)

    compileResult.hasFailures shouldBe false

    val ctx = compileResult.context.global.getContextOf(unit)
    val result = queryExecutor.executeSelectedStatement(
      unit,
      QuerySelection.All,
      LinePosition(1, 1),
      ctx
    )

    // Verify the schema was updated in both places
    ctx.global.defaultSchema shouldBe "test_schema"
    compiler.getDefaultSchema shouldBe "test_schema"
  }

  test("use catalog.schema should update schema") {
    val workEnv = WorkEnv(".", LogLevel.DEBUG)
    val compilerOptions = CompilerOptions(
      workEnv = workEnv,
      catalog = Some("memory"),
      schema = Some("main")
    )
    val compiler            = Compiler(compilerOptions)
    val dbConnectorProvider = DBConnectorProvider(workEnv)
    val queryExecutor = QueryExecutor(
      dbConnectorProvider = dbConnectorProvider,
      defaultProfile = Profile.defaultDuckDBProfile,
      workEnv = workEnv
    )

    // Initial state
    compiler.getDefaultSchema shouldBe "main"

    // Compile and execute use schema command with catalog
    val useSchemaQuery = "use schema my_catalog.my_schema"
    val unit           = CompilationUnit.fromWvletString(useSchemaQuery)
    val compileResult  = compiler.compileSingleUnit(unit)

    compileResult.hasFailures shouldBe false

    val ctx = compileResult.context.global.getContextOf(unit)
    val result = queryExecutor.executeSelectedStatement(
      unit,
      QuerySelection.All,
      LinePosition(1, 1),
      ctx
    )

    // Verify the schema was updated (catalog switching not yet supported)
    ctx.global.defaultSchema shouldBe "my_schema"
    compiler.getDefaultSchema shouldBe "my_schema"
  }

end UseSchemaTest

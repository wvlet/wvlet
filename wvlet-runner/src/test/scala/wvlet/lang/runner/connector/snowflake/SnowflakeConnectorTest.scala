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
package wvlet.lang.runner.connector.snowflake

import wvlet.airframe.Design
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.airspec.AirSpec
import wvlet.lang.compiler.WorkEnv

/**
  * Tests for SnowflakeConnector
  *
  * Note: Integration tests require actual Snowflake credentials to run. Set the following
  * environment variables:
  *   - SNOWFLAKE_ACCOUNT
  *   - SNOWFLAKE_USER
  *   - SNOWFLAKE_PASSWORD
  *   - SNOWFLAKE_DATABASE
  *   - SNOWFLAKE_SCHEMA
  *   - SNOWFLAKE_WAREHOUSE (optional)
  *   - SNOWFLAKE_ROLE (optional)
  *
  * Integration tests will be skipped if credentials are not available.
  */
class SnowflakeConnectorTest extends AirSpec:

  private val snowflakeConfig: Option[SnowflakeConfig] =
    for
      account  <- sys.env.get("SNOWFLAKE_ACCOUNT")
      user     <- sys.env.get("SNOWFLAKE_USER")
      password <- sys.env.get("SNOWFLAKE_PASSWORD")
      database <- sys.env.get("SNOWFLAKE_DATABASE")
      schema   <- sys.env.get("SNOWFLAKE_SCHEMA")
    yield SnowflakeConfig(
      account = account,
      database = database,
      schema = schema,
      warehouse = sys.env.get("SNOWFLAKE_WAREHOUSE"),
      role = sys.env.get("SNOWFLAKE_ROLE"),
      user = Some(user),
      password = Some(password)
    )

  if snowflakeConfig.isEmpty then
    skip(
      "Snowflake credentials not available. Set SNOWFLAKE_* environment variables to run integration tests."
    )

  initDesign { d =>
    snowflakeConfig match
      case Some(config) =>
        d.bindInstance[SnowflakeConfig](config)
          .bindInstance[WorkEnv](WorkEnv())
          .bind[SnowflakeConnector]
          .toProvider { (cfg: SnowflakeConfig, env: WorkEnv) =>
            SnowflakeConnector(cfg, env)
          }
      case None =>
        d
  }

  test("should create SnowflakeConfig with proper withXXX methods") {
    val config = SnowflakeConfig(
      account = "test-account",
      database = "test-db",
      schema = "test-schema"
    )

    val updated = config
      .withWarehouse("test-warehouse")
      .withRole("test-role")
      .withUser("test-user")
      .withPassword("test-password")

    updated.warehouse shouldBe Some("test-warehouse")
    updated.role shouldBe Some("test-role")
    updated.user shouldBe Some("test-user")
    updated.password shouldBe Some("test-password")

    val cleared = updated.noWarehouse().noRole().noUser().noPassword()
    cleared.warehouse shouldBe None
    cleared.role shouldBe None
    cleared.user shouldBe None
    cleared.password shouldBe None
  }

  test("should connect to Snowflake and execute basic queries") {
    (connector: SnowflakeConnector, config: SnowflakeConfig) =>
      // Test basic query
      connector.runQuery("SELECT 1 as test_col") { rs =>
        rs.next() shouldBe true
        rs.getInt("test_col") shouldBe 1
      }

      // Test catalog listing
      val catalogs = connector.getCatalogNames
      debug(s"Available catalogs: ${catalogs.mkString(", ")}")
      catalogs shouldNotBe empty

      // Test schema listing
      val schemas = connector.listSchemas(config.database)
      debug(s"Available schemas in ${config.database}: ${schemas.map(_.name).mkString(", ")}")
      schemas shouldNotBe empty

      // Test table listing
      val tables = connector.listTables(config.database, config.schema)
      debug(
        s"Available tables in ${config.database}.${config.schema}: ${tables
            .map(_.name)
            .mkString(", ")}"
      )

      // Test function listing
      test("list functions") {
        val functions = connector.listFunctions(config.database)
        debug(s"Found ${functions.size} functions")
        functions shouldNotBe empty
      }
  }

end SnowflakeConnectorTest

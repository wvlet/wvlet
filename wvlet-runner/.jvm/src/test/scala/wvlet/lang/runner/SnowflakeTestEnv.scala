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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.connector.snowflake.SnowflakeConfig

/**
  * Snowflake credentials for the tests that need a real account, from environment variables:
  *
  *   - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE
  *   - One of SNOWFLAKE_PRIVATE_KEY_FILE (key pair, with SNOWFLAKE_PRIVATE_KEY_PWD for an encrypted
  *     key), SNOWFLAKE_TOKEN (programmatic access token), or SNOWFLAKE_PASSWORD
  *   - Optional: SNOWFLAKE_SCHEMA (default PUBLIC), SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
  */
object SnowflakeTestEnv:
  val missingMessage: String =
    "Snowflake credentials are not set. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE, " +
      "and one of SNOWFLAKE_PRIVATE_KEY_FILE, SNOWFLAKE_TOKEN, or SNOWFLAKE_PASSWORD"

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  /** The configuration from the environment, or None when a required variable is missing */
  val config: Option[SnowflakeConfig] =
    for
      account  <- env("SNOWFLAKE_ACCOUNT")
      user     <- env("SNOWFLAKE_USER")
      database <- env("SNOWFLAKE_DATABASE")
      config = SnowflakeConfig(
        account = account,
        database = database,
        schema = env("SNOWFLAKE_SCHEMA").getOrElse("PUBLIC"),
        warehouse = env("SNOWFLAKE_WAREHOUSE"),
        role = env("SNOWFLAKE_ROLE"),
        user = Some(user),
        password = env("SNOWFLAKE_PASSWORD"),
        privateKeyFile = env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        privateKeyPassword = env("SNOWFLAKE_PRIVATE_KEY_PWD"),
        token = env("SNOWFLAKE_TOKEN")
      )
      if config.privateKeyFile.isDefined || config.token.isDefined || config.password.isDefined
    yield config

  /** The configuration as a profile connector, read by SnowflakeConnectorFactory */
  def connectorConfig(c: SnowflakeConfig): ConnectorConfig = ConnectorConfig(
    name = "snowflake",
    `type` = "snowflake",
    user = c.user,
    password = c.password,
    host = Some(c.account),
    catalog = Some(c.database),
    schema = Some(c.schema),
    properties =
      Seq(
        c.warehouse.map("warehouse" -> _),
        c.role.map("role" -> _),
        c.privateKeyFile.map("private_key_file" -> _),
        c.privateKeyPassword.map("private_key_pwd" -> _),
        c.token.map("token" -> _)
      ).flatten.toMap
  )

end SnowflakeTestEnv

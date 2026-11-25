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
import wvlet.lang.catalog.Profile

/**
  * Run the standard-library specs (spec/basic/stdlib_*.wv) against a real Snowflake account, so
  * that the `in snowflake` variants are checked by execution. CI has no Snowflake account: run it
  * locally with your own account before changing a Snowflake variant (see design/testing.md):
  *
  * {{{
  * SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... SNOWFLAKE_DATABASE=... \
  *   ./sbt "runnerJVM/testOnly *SnowflakeStdLibSpec"
  * }}}
  *
  * SNOWFLAKE_SCHEMA (default PUBLIC), SNOWFLAKE_WAREHOUSE, and SNOWFLAKE_ROLE are optional. The
  * specs query literal tables only, so they create no objects. Without the credentials, every test
  * is skipped
  */
class SnowflakeStdLibSpec extends ConnectorSpecRunner("spec/basic", _.startsWith("stdlib_")):

  private def env(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

  private val config: Option[(String, String, String, String)] =
    for
      account  <- env("SNOWFLAKE_ACCOUNT")
      user     <- env("SNOWFLAKE_USER")
      password <- env("SNOWFLAKE_PASSWORD")
      database <- env("SNOWFLAKE_DATABASE")
    yield (account, user, password, database)

  config match
    case None =>
      skip(
        "Snowflake credentials are not set. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, " +
          "SNOWFLAKE_PASSWORD, and SNOWFLAKE_DATABASE to run the stdlib specs on Snowflake"
      )
    case Some((account, user, password, database)) =>
      val schema = env("SNOWFLAKE_SCHEMA").getOrElse("PUBLIC")
      runSpecs(
        Profile(
          name = s"snowflake@${account}",
          connectors = Seq(
            ConnectorConfig(
              name = "snowflake",
              `type` = "snowflake",
              user = Some(user),
              password = Some(password),
              host = Some(account),
              catalog = Some(database),
              schema = Some(schema),
              properties =
                env("SNOWFLAKE_WAREHOUSE").map("warehouse" -> _).toMap ++
                  env("SNOWFLAKE_ROLE").map("role" -> _).toMap
            )
          )
        ),
        catalog = database,
        schema = schema
      )

end SnowflakeStdLibSpec

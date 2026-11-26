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

import wvlet.lang.catalog.Profile

/**
  * Run the standard-library specs (spec/basic/stdlib_*.wv) against a real Snowflake account, so
  * that the `in snowflake` variants are checked by execution. CI has no Snowflake account: run it
  * locally with your own account before changing a Snowflake variant (see design/stdlib.md):
  *
  * {{{
  * SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_DATABASE=... \
  *   SNOWFLAKE_PRIVATE_KEY_FILE=~/.snowflake/rsa_key.p8 \
  *   ./sbt "runnerJVM/testOnly *SnowflakeStdLibSpec"
  * }}}
  *
  * The variables are listed in [[SnowflakeTestEnv]]. The specs query literal tables only, so they
  * create no objects. Without the credentials, every test is skipped
  */
class SnowflakeStdLibSpec extends ConnectorSpecRunner("spec/basic", _.startsWith("stdlib_")):

  SnowflakeTestEnv.config match
    case None =>
      skip(SnowflakeTestEnv.missingMessage)
    case Some(config) =>
      runSpecs(
        Profile(
          name = s"snowflake@${config.account}",
          connectors = Seq(SnowflakeTestEnv.connectorConfig(config))
        ),
        catalog = config.database,
        schema = config.schema
      )

end SnowflakeStdLibSpec

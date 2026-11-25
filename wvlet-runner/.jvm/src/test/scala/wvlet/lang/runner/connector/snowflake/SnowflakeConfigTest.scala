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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.snowflake.SnowflakeConfig
import wvlet.lang.connector.snowflake.SnowflakeConnector
import wvlet.lang.connector.snowflake.SnowflakeConnectorFactory
import wvlet.uni.test.UniTest

/**
  * Snowflake authentication settings, checked without an account: which JDBC properties each
  * credential produces, and how a profile's connector properties reach the configuration
  */
class SnowflakeConfigTest extends UniTest:
  private val base = SnowflakeConfig(account = "acct", database = "db", schema = "PUBLIC").withUser(
    "svc"
  )

  private def props(c: SnowflakeConfig): Map[String, String] =
    val p = c.jdbcProperties
    p.stringPropertyNames().toArray.map(_.toString).map(k => k -> p.getProperty(k)).toMap

  test("use key-pair authentication for a private key file") {
    val p = props(base.withPrivateKeyFile("/keys/rsa_key.p8").withPrivateKeyPassword("pass"))
    p("authenticator") shouldBe "SNOWFLAKE_JWT"
    p("private_key_file") shouldBe "/keys/rsa_key.p8"
    p("private_key_pwd") shouldBe "pass"
    p.contains("password") shouldBe false
  }

  test("send a programmatic access token as the password with its authenticator") {
    val p = props(base.withToken("pat-secret"))
    p("authenticator") shouldBe "PROGRAMMATIC_ACCESS_TOKEN"
    p("password") shouldBe "pat-secret"
  }

  test("prefer a private key over a token and a password") {
    val p = props(base.withPrivateKeyFile("/keys/k.p8").withToken("t").withPassword("pw"))
    p("authenticator") shouldBe "SNOWFLAKE_JWT"
    p.contains("password") shouldBe false
  }

  test("send a plain password without an authenticator") {
    val p = props(base.withPassword("pw"))
    p("password") shouldBe "pw"
    p.contains("authenticator") shouldBe false
    p("user") shouldBe "svc"
    p("db") shouldBe "db"
    p("schema") shouldBe "PUBLIC"
  }

  test("read the key-pair and token settings from the profile's connector properties") {
    val connector = SnowflakeConnectorFactory.create(
      ConnectorConfig(
        name = "snowflake",
        `type` = "snowflake",
        user = Some("svc"),
        host = Some("acct"),
        catalog = Some("db"),
        properties = Map(
          "private_key_file" -> "/keys/k.p8",
          "private_key_pwd"  -> "pass",
          "token"            -> "t"
        )
      ),
      WorkEnv()
    )
    val c = connector.asInstanceOf[SnowflakeConnector].config
    c.privateKeyFile shouldBe Some("/keys/k.p8")
    c.privateKeyPassword shouldBe Some("pass")
    c.token shouldBe Some("t")
  }

end SnowflakeConfigTest

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
package wvlet.lang.runner.connector

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.ConnectorFactoryRegistry
import wvlet.lang.connector.duckdb.DuckDBConnector
import wvlet.lang.connector.duckdb.DuckDBConnectorFactory
import wvlet.lang.connector.duckdb.GenericConnector
import wvlet.lang.connector.slack.SlackConnectorFactory
import wvlet.lang.connector.trino.TrinoConnectorFactory
import wvlet.lang.runner.engine.WvletEngineConnectorFactory
import wvlet.uni.test.UniTest
import wvlet.uni.test.defined

class ConnectorProviderTest extends UniTest:

  private def newProvider(): ConnectorProvider = ConnectorProvider(WorkEnv())

  test("should create a connector by its type string") {
    val provider = newProvider()
    try
      val connector = provider.getConnector(ConnectorConfig(name = "db", `type` = "duckdb"))
      connector shouldMatch { case _: DuckDBConnector =>
      }
      connector.name shouldBe "db"
      connector.connectorType shouldBe "duckdb"
      connector.engine shouldBe defined
    finally
      provider.close()
  }

  test("should fall back to the generic connector for unknown types") {
    val provider = newProvider()
    try
      provider.isAvailable("hive") shouldBe true
      val connector = provider.getConnector(ConnectorConfig(name = "h", `type` = "hive"))
      connector shouldMatch { case _: GenericConnector =>
      }
    finally
      provider.close()
  }

  test("should fail when no factory matches and no generic fallback is registered") {
    val provider = ConnectorProvider(WorkEnv(), factories = Seq(DuckDBConnectorFactory))
    try
      val exception = intercept[WvletLangException] {
        provider.getConnector(ConnectorConfig(name = "s", `type` = "slack"))
      }
      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "No connector for type 'slack'"
    finally
      provider.close()
  }

  test("should discover every connector module on the classpath") {
    ConnectorFactoryRegistry.factories.map(_.connectorType).toSet shouldBe
      Set("duckdb", "generic", "trino", "snowflake", "slack", "wvlet")
  }

  // The lite CLI leaves out the DuckDB and Snowflake modules
  private def liteProvider(): ConnectorProvider = ConnectorProvider(
    WorkEnv(),
    factories = Seq(TrinoConnectorFactory, SlackConnectorFactory, WvletEngineConnectorFactory)
  )

  test("should tell how to install a connector whose module is missing") {
    val provider = liteProvider()
    try
      provider.isAvailable("duckdb") shouldBe false
      provider.isAvailable("hive") shouldBe false
      provider.isAvailable("wvlet") shouldBe true
      for tpe <- Seq("duckdb", "generic", "snowflake") do
        val exception = intercept[WvletLangException] {
          provider.getConnector(ConnectorConfig(name = "db", `type` = tpe))
        }
        exception.statusCode shouldBe StatusCode.CONNECTOR_NOT_INSTALLED
        exception.message shouldContain s"The ${tpe} connector (for 'db') is not installed"
        exception.message shouldContain "wvlet-cli-full"
    finally
      provider.close()
  }

  test("should not fall back to DuckDB for unknown types without the DuckDB module") {
    val provider = liteProvider()
    try
      val exception = intercept[WvletLangException] {
        provider.getConnector(ConnectorConfig(name = "h", `type` = "hive"))
      }
      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "Available: slack, trino, wvlet"
    finally
      provider.close()
  }

  test("should cache connectors by connector config value") {
    val provider = newProvider()
    try
      val config = ConnectorConfig(name = "db", `type` = "duckdb", catalog = Some("memory"))
      val c1     = provider.getConnector(config)
      val c2     = provider.getConnector(config.copy())
      c1 shouldBeTheSameInstanceAs c2

      // A differently configured connector gets its own instance
      val c3 = provider.getConnector(config.copy(schema = Some("other")))
      (c3 eq c1) shouldBe false
    finally
      provider.close()
  }

  test("should keep distinct connectors for same-named profiles with different configs") {
    // Both default profiles are named "local"; the config-keyed cache must not conflate them
    val provider = newProvider()
    try
      val duckdb  = provider.getConnector(Profile.defaultDuckDBProfile)
      val generic = provider.getConnector(Profile.defaultGenericProfile)
      (duckdb eq generic) shouldBe false
      duckdb shouldMatch { case _: DuckDBConnector =>
      }
      generic shouldMatch { case _: GenericConnector =>
      }
    finally
      provider.close()
  }

  test("should resolve a profile's default engine") {
    val provider = newProvider()
    try
      val profile = Profile(
        name = "multi",
        connectors = Seq(
          ConnectorConfig(name = "first", `type` = "generic"),
          ConnectorConfig(name = "second", `type` = "duckdb", default = true)
        )
      )
      val connector = provider.getConnector(profile)
      connector.name shouldBe "second"
      connector shouldMatch { case _: DuckDBConnector =>
      }
    finally
      provider.close()
  }

end ConnectorProviderTest

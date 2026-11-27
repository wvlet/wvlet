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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.connector.slack.SlackConnectorFactory
import wvlet.lang.connector.trino.TrinoConnectorFactory
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.engine.WvletEngineConnectorFactory
import wvlet.uni.control.Control
import wvlet.uni.test.UniTest

/** The CLI without the optional DuckDB and Snowflake connector modules (the lite distribution) */
class WvletLiteDistributionTest extends UniTest:

  private def withLiteCompiler[U](opt: WvletCliCompileOption, defaultProfile: Profile)(
      body: WvletCompiler => U
  ): U =
    val workEnv  = WorkEnv()
    val provider = ConnectorProvider(
      workEnv,
      factories = Seq(TrinoConnectorFactory, SlackConnectorFactory, WvletEngineConnectorFactory)
    )
    Control.withResource(
      WvletCompiler(
        WvletCliGlobalOption(),
        opt,
        workEnv,
        provider,
        opt.resolveProfile(defaultProfile)
      )
    )(body)

  test("compile to DuckDB SQL without the DuckDB connector") {
    withLiteCompiler(
      WvletCliCompileOption(query = Some("from [[1, 'a']] as t(id, name) select name")),
      Profile.defaultDuckDBProfile
    ) { compiler =>
      compiler.generateSQL shouldContain "select name"
    }
  }

  test("run a query on the wvlet engine without the DuckDB connector") {
    withLiteCompiler(
      WvletCliCompileOption(query = Some("from [[1, 'a']] as t(id, name) select name")),
      Profile.defaultWvletProfile
    ) { compiler =>
      compiler.run()
    }
  }

  test("report how to install DuckDB when running on it without the connector") {
    val e = intercept[WvletLangException] {
      withLiteCompiler(
        WvletCliCompileOption(query = Some("select 1"), targetDBType = Some("duckdb")),
        Profile.defaultWvletProfile
      ) { compiler =>
        compiler.run()
      }
    }
    e.statusCode shouldBe StatusCode.CONNECTOR_NOT_INSTALLED
    e.message shouldContain "wvlet-cli-full"
  }

end WvletLiteDistributionTest

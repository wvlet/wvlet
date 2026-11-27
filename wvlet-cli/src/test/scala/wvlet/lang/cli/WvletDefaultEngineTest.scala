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
import wvlet.uni.test.UniTest

/** Without -t or --profile, queries run on the in-process wvlet engine; -t duckdb opts in */
class WvletDefaultEngineTest extends UniTest:

  private def replProfile(opt: WvletREPLOption): Profile = WvletREPLMain(
    WvletCliGlobalOption(),
    opt
  ).replDesign(isInteractive = false).run[Profile, Profile](identity)

  test("run a query on the wvlet engine by default") {
    // The engine runs no SQL statements, so a DuckDB SQL statement fails instead of reaching DuckDB
    val e = intercept[WvletLangException] {
      WvletMain.main(Array("run", "execute sql\"select 42\""))
    }
    e.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
  }

  test("run DuckDB SQL with -t duckdb") {
    // Succeeds only on DuckDB: the same statement fails with NOT_IMPLEMENTED on the engine
    WvletMain.main(Array("run", "-t", "duckdb", "execute sql\"select 42\""))
  }

  test("start the REPL on the wvlet engine by default") {
    replProfile(WvletREPLOption()).defaultEngine.`type` shouldBe "wvlet"
  }

  test("start the REPL on DuckDB with -t duckdb") {
    replProfile(WvletREPLOption(targetDBType = Some("duckdb"))).defaultEngine.`type` shouldBe
      "duckdb"
  }

  test("reject -t together with --profile in the REPL") {
    val e = intercept[WvletLangException] {
      replProfile(WvletREPLOption(profile = Some("p"), targetDBType = Some("duckdb")))
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

end WvletDefaultEngineTest

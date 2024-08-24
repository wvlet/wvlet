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

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.InMemoryCatalog
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airspec.AirSpec

class DuckDBExecutorTest extends AirSpec:

  private val duckDB = QueryExecutor(DuckDBConnector(prepareTPCH = false))

  override def afterAll: Unit = duckDB.close()

  test("weblog") {
    pendingUntil("Stabilizing the new parser")
    duckDB.executeSingleSpec("examples/cdp_behavior", "behavior.wv")
  }

end DuckDBExecutorTest

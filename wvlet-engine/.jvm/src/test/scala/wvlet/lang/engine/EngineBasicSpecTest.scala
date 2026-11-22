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
package wvlet.lang.engine

import wvlet.uni.test.UniTest

/** Queries in spec/basic on the engine must return the same rows as DuckDB */
class EngineBasicSpecTest extends UniTest:
  /**
    * Specs whose results depend on DuckDB itself rather than on query semantics: DuckDB's system
    * catalogs (`system`, `temp`) in SHOW CATALOGS / SHOW SCHEMAS / wv_schemas(), the
    * `duckdb_functions()` table function, and the `inet` type. This list must only shrink
    */
  private val knownGaps: Set[String] = Set(
    "execute.wv",
    "from_table_function.wv",
    "information_schema.wv",
    "show-catalogs.wv",
    "show-schemas.wv"
  )

  private val conformance = EngineConformance("spec/basic", Nil, _ => ())

  override def afterAll: Unit = conformance.close()

  for unit <- conformance.units do
    test(unit.sourceFile.fileName) {
      if knownGaps.contains(unit.sourceFile.fileName) then
        ignore("Depends on DuckDB-specific catalogs, functions, or types")
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }

end EngineBasicSpecTest

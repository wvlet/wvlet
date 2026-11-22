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
    * Specs using features the engine doesn't support yet (pivot, unnest, ASOF joins, tuple IN,
    * lambdas, SHOW, some functions). This list must only shrink
    */
  private val knownGaps: Set[String] = Set(
    "agg_col_type.wv",
    "array_access.wv",
    "array_agg.wv",
    "count.wv",
    "engine-native-func.wv",
    "execute.wv",
    "from_table_function.wv",
    "information_schema.wv",
    "join-asof.wv",
    "lambda.wv",
    "map.wv",
    "map_value.wv",
    "method_chain_agg.wv",
    "partial-query-nested.wv",
    "pivot-agg-in.wv",
    "pivot-agg.wv",
    "pivot-group-by.wv",
    "pivot-order.wv",
    "pivot.wv",
    "sample.wv",
    "show-catalogs.wv",
    "show-modelsl.wv",
    "show-schemas.wv",
    "show-tables.wv",
    "stdlib_array.wv",
    "stdlib_datetime.wv",
    "stdlib_json.wv",
    "stdlib_map.wv",
    "stdlib_math.wv",
    "stdlib_misc.wv",
    "stdlib_range.wv",
    "stdlib_string.wv",
    "stdlib_window.wv",
    "table_val_asof_join.wv",
    "table_val_join.wv",
    "triple-quote.wv",
    "tuple-in-subquery.wv",
    "tuple-in.wv",
    "tuple-not-in-subquery.wv",
    "tuple-not-in.wv",
    "ulid.wv",
    "unnest-cross-join.wv",
    "unnest.wv",
    "unpivot.wv",
    "window-chain.wv",
    "window.wv",
    "with-values.wv"
  )

  private val conformance = EngineConformance("spec/basic", Nil, _ => ())

  override def afterAll: Unit = conformance.close()

  for unit <- conformance.units do
    test(unit.sourceFile.fileName) {
      if knownGaps.contains(unit.sourceFile.fileName) then
        ignore("Not supported by the engine yet")
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }

end EngineBasicSpecTest

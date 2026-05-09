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
package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.analyzer.duckdb.DuckDB
import wvlet.lang.model.RelationType

/**
  * File schema inference. Dispatches by file extension:
  *   - `*.json` / `*.json.gz` → [[JSONAnalyzer]] (cross-platform — uses uni.io for read + uni.json
  *     for parsing, no DuckDB needed)
  *   - everything else (parquet, csv, …) → [[DuckDB]] (JVM uses JDBC; JS/Native throw until
  *     `@duckdb/node-api` and `libduckdb` bindings land)
  *
  * Naming: kept as `DuckDBAnalyzer` to avoid churn in the single call site
  * (`TypeResolver.resolveLocalFileScan`). The non-DuckDB JSON dispatch is an internal
  * implementation detail.
  */
object DuckDBAnalyzer:
  def guessSchema(path: String): RelationType =
    if path.endsWith(".json") || path.endsWith(".json.gz") then
      JSONAnalyzer.analyzeJSONFile(path)
    else
      DuckDB.schemaOf(path)

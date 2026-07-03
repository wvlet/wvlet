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
  * (`RelationRefResolver.resolveDataFileRef`). The non-DuckDB JSON dispatch is an internal
  * implementation detail.
  */
object DuckDBAnalyzer:

  /**
    * Infer the relation type (column names + data types) of the file at `path` for use as the shape
    * of a `from '<path>'` clause. Routes JSON files through [[JSONAnalyzer]] and everything else
    * through the [[DuckDB]] facade. The native Scala Native backend uses the `libduckdb` C API; the
    * JVM backend uses the DuckDB JDBC driver.
    *
    * Returns `EmptyRelationType` if the file does not exist (per the backend's own pre-check) so
    * that the typer can surface a cleaner downstream error.
    *
    * @param path
    *   path to a local file (parquet, csv, json, …)
    * @return
    *   inferred `RelationType` for the file, or `EmptyRelationType` if the file is missing
    */
  def guessSchema(path: String): RelationType =
    if path.endsWith(".json") || path.endsWith(".json.gz") then
      JSONAnalyzer.analyzeJSONFile(path)
    else
      DuckDB.schemaOf(path)

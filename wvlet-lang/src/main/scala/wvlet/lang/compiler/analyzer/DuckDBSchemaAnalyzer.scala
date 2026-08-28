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
  *   - JSON-family files (`.json`, `.jsonl`, `.ndjson`, plain or `.gz`) → [[JSONAnalyzer]]
  *     (cross-platform — uses uni.io for read + uni.json for parsing, no DuckDB needed)
  *   - everything else (parquet, csv, tsv, `.zst`, …) → [[DuckDB]] (JVM uses JDBC; JS/Native use
  *     the `libduckdb` C API)
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
  def guessSchema(path: String): RelationType = guessSchema(path, DataFilePath.parse(path))

  /** Same as [[guessSchema]] for a path whose extension has already been classified */
  def guessSchema(path: String, dataFile: Option[DataFilePath]): RelationType =
    dataFile match
      case Some(f) if f.canUseJsonAnalyzer =>
        JSONAnalyzer.analyzeJSONFile(path, f)
      case _ =>
        DuckDB.schemaOf(path)

end DuckDBAnalyzer

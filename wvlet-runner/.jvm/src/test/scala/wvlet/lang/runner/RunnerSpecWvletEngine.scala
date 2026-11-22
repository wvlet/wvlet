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

import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile

/** Profiles running specs on the in-process wvlet engine (connector type `wvlet`) */
object WvletEngineProfiles:
  def of(properties: Map[String, Any] = Map.empty): Profile = Profile(
    name = "wvlet-engine",
    connectors = Seq(ConnectorConfig(name = "wvlet", `type` = "wvlet", properties = properties))
  )

/** spec/tpch on the wvlet engine, with the TPC-H tables registered from the data folder */
class RunnerSpecWvletEngineTPCH
    extends RunnerSpec(
      "spec/tpch",
      ignoredSpec = Map(
        "q1-test.wv" ->
          "avg(double) differs from DuckDB in the last digits (floating-point summation order)"
      ),
      engineProfile = Some(WvletEngineProfiles.of(Map("dataDir" -> "data")))
    )

/** spec/basic on the wvlet engine, except for specs using features the engine lacks */
class RunnerSpecWvletEngineBasic
    extends RunnerSpec(
      "spec/basic",
      ignoredSpec = RunnerSpecWvletEngineBasic.knownGaps,
      engineProfile = Some(WvletEngineProfiles.of())
    )

object RunnerSpecWvletEngineBasic:
  /**
    * Specs using features the engine can't run yet, with the missing feature. This list must only
    * shrink
    */
  val knownGaps: Map[String, String] = Map(
    "agg_col_type.wv"               -> "Aggregate function max_by placement",
    "append-to-file.wv"             -> "save to <file>",
    "append-to-new-table.wv"        -> "append to",
    "append.wv"                     -> "append to",
    "attach-database.wv"            -> "Database attachment",
    "count.wv"                      -> "Result differs from DuckDB",
    "create-table-sql-semantics.wv" -> "append to",
    "ddl-basic.wv"                  -> "DDL / SQL statements",
    "delete-all.wv"                 -> "delete",
    "delete.wv"                     -> "delete",
    "drop-schema-cascade.wv"        -> "DDL / SQL statements",
    "engine-native-func.wv"         -> "Function levenshtein",
    "execute.wv"                    -> "DDL / SQL statements",
    "flow-params.wv"                -> "RunFlow",
    "flow-run.wv"                   -> "RunFlow",
    "for-loop.wv"                   -> "append to",
    "from_table_function.wv"        -> "TableFunctionCall",
    "information_schema.wv"         -> "information_schema",
    "join-asof.wv"                  -> "ASOF joins",
    "lambda.wv"                     -> "LambdaExpr",
    "map.wv"                        -> "Map values",
    "method_chain_agg.wv"           -> "Aggregate function sum placement",
    "model-nested-ref.wv"           -> "Aggregate function sum placement",
    "partial-query-nested.wv"       -> "Result differs from DuckDB",
    "pivot-agg-in.wv"               -> "Pivot",
    "pivot-agg.wv"                  -> "Pivot",
    "pivot-group-by.wv"             -> "Pivot",
    "pivot-order.wv"                -> "Pivot",
    "pivot.wv"                      -> "Pivot",
    "reshape-and-views.wv"          -> "DDL / SQL statements",
    "save-to-file.wv"               -> "save to <file>",
    "save-with-options.wv"          -> "save to <file>",
    "show-catalogs.wv"              -> "Show",
    "show-modelsl.wv"               -> "Show",
    "show-schemas.wv"               -> "Show",
    "show-tables.wv"                -> "Show",
    "stdlib_array.wv"               -> "Function list_position",
    "stdlib_datetime.wv"            -> "Unknown extract field: isodow",
    "stdlib_json.wv"                -> "Function json_extract_string",
    "stdlib_map.wv"                 -> "Map values",
    "stdlib_math.wv"                -> "Function isnan",
    "stdlib_misc.wv"                -> "Function make_timestamp",
    "stdlib_range.wv"               -> "Function range",
    "stdlib_string.wv"              -> "Function levenshtein",
    "stdlib_window.wv"              -> "sql interpolation in window expressions",
    "string-concat.wv"              -> "Result differs from DuckDB",
    "table-constraints.wv"          -> "DDL / SQL statements",
    "table-defaults.wv"             -> "DDL / SQL statements",
    "table-in-binding.wv"           -> "DDL / SQL statements",
    "table-like.wv"                 -> "append to",
    // ddl/table-mixins.wv; ignoring by file name also skips the passing top-level table-mixins.wv
    "table-mixins.wv"          -> "DDL / SQL statements",
    "table-row-methods.wv"     -> "NULL in a mixed-type VALUES column is not NULL",
    "table_val_asof_join.wv"   -> "ASOF joins",
    "table_val_join.wv"        -> "Qualified columns of table-valued joins",
    "triple-quote.wv"          -> "Function hash",
    "tuple-in-subquery.wv"     -> "TupleIn",
    "tuple-in.wv"              -> "TupleIn",
    "tuple-not-in-subquery.wv" -> "TupleNotIn",
    "tuple-not-in.wv"          -> "TupleNotIn",
    "ulid.wv"                  -> "Function ulid_string",
    "unnest-cross-join.wv"     -> "TableFunctionCall",
    "unnest.wv"                -> "TableFunctionCall",
    "unpivot.wv"               -> "Unpivot",
    "update-and-upsert.wv"     -> "DDL / SQL statements",
    "use-schema.wv"            -> "Switches to the duckdb connector",
    "window-chain.wv"          -> "Result differs from DuckDB",
    "window.wv"                -> "sql interpolation in window expressions",
    "with-values.wv"           -> "ASOF joins",
    "table-value-constant.wv"  -> "Need to support table value constant"
  )

end RunnerSpecWvletEngineBasic

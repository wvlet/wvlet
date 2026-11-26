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

/**
  * spec/tpch on the wvlet engine over TPC-H tables generated while they're scanned (`prepareTPCH`),
  * with no data files. Values differ from dbgen's, so only that queries run is checked
  */
class RunnerSpecWvletEngineTPCHGen
    extends RunnerSpec(
      "spec/tpch",
      ignoredSpec = Map("q1-test.wv" -> "Asserts dbgen's answer; generated values differ"),
      engineProfile = Some(WvletEngineProfiles.of(Map("prepareTPCH" -> true)))
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
    "attach-database.wv"      -> "Database attachment",
    "execute.wv"              -> "SQL statements",
    "flow-params.wv"          -> "RunFlow",
    "flow-run.wv"             -> "RunFlow",
    "for-loop.wv"             -> "range table function",
    "from_table_function.wv"  -> "TableFunctionCall",
    "map.wv"                  -> "Result differs from DuckDB",
    "reshape-and-views.wv"    -> "alter table and views",
    "save-with-options.wv"    -> "parquet_metadata table function",
    "string-concat.wv"        -> "Result differs from DuckDB",
    "table-in-binding.wv"     -> "Tables bound to another catalog",
    "unpivot.wv"              -> "Row order differs from DuckDB",
    "update-and-upsert.wv"    -> "update",
    "use-schema.wv"           -> "Switches to the duckdb connector",
    "table-value-constant.wv" -> "Need to support table value constant"
  )

end RunnerSpecWvletEngineBasic

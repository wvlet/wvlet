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

import wvlet.lang.engine.plan.EngineFunctions
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import scala.jdk.CollectionConverters.*

/**
  * Compatibility gate for DuckDB function names in the engine: reads the generated DuckDB catalog
  * and counts the names the engine binds ([[EngineFunctions.isSupported]]). Out-of-scope functions
  * are excluded explicitly. The primary measure is [[EngineStdlibCoverageCheck]]: the stdlib is
  * Wvlet's engine-neutral function contract, and DuckDB names are only what its DuckDB variants
  * expand to.
  *
  * `minCovered` pins the current count. Raise it when porting functions; never lower it. The full
  * report is written to target/engine-function-coverage.txt.
  */
class EngineFunctionCoverageCheck extends UniTest:

  private val catalogPath = Path.of("wvlet-stdlib/module/standard/duckdb/functions.wv")

  /** Covered catalog functions must not drop below this count */
  private val minCovered = 199

  /** Function families out of scope for the engine: collations, spatial, DuckDB-specific types */
  private val excludedPrefixes: Seq[String] = Seq(
    "collate_",
    "icu_collate_",
    "st_",
    "union_",
    "enum_",
    "variant_"
  )

  /**
    * DuckDB internals: connection and session state, plan serialization, logs, sort keys, aggregate
    * states, type introspection, and sequences
    */
  private val excludedNames: Set[String] = Set(
    "alias",
    "bitstring_byte_comparable",
    "can_cast_implicitly",
    "cast_to_type",
    "combine",
    "combine_aggr",
    "constant_or_null",
    "create_sort_key",
    "current_connection_id",
    "current_database",
    "current_query",
    "current_query_id",
    "current_schema",
    "current_schemas",
    "current_setting",
    "current_transaction_id",
    "currval",
    "finalize",
    "get_type",
    "getvariable",
    "hive_partition_component",
    "icu_sort_key",
    "in_search_path",
    "invoke",
    "is_histogram_other_bin",
    "json_deserialize_sql",
    "json_serialize_plan",
    "json_serialize_sql",
    "make_type",
    "nextval",
    "parse_duckdb_log_message",
    "remap_struct",
    "replace_type",
    "setval",
    "sleep_ms",
    "stats",
    "timetz_byte_comparable",
    "to_aggregate_state",
    "txid_current",
    "unpivot_list",
    "vector_type",
    "version",
    "write_log"
  )

  private def isExcluded(name: String): Boolean =
    excludedNames.contains(name) || excludedPrefixes.exists(name.startsWith)

  private lazy val catalogFunctions: Seq[String] =
    val Def = """^def\s+([A-Za-z_][A-Za-z0-9_]*).*\bin duckdb\b.*""".r
    Files
      .readAllLines(catalogPath)
      .asScala
      .collect { case Def(name) =>
        name.toLowerCase
      }
      .distinct
      .sorted
      .toSeq

  private def family(name: String): String =
    val i = name.indexOf('_')
    if i > 0 then
      name.substring(0, i)
    else
      name

  test("the catalog lists DuckDB functions") {
    catalogFunctions.size shouldMatch {
      case n: Int if n > 500 =>
    }
  }

  test("exclusions name catalog functions the engine does not bind") {
    excludedNames.filterNot(catalogFunctions.contains).toSeq.sorted shouldBe Seq.empty
    catalogFunctions.filter(n => isExcluded(n) && EngineFunctions.isSupported(n)) shouldBe Seq.empty
  }

  test("engine coverage of DuckDB functions must not regress") {
    val (excluded, inScope) = catalogFunctions.partition(isExcluded)
    val (covered, missing)  = inScope.partition(EngineFunctions.isSupported)
    val ratio               = covered.size.toDouble / inScope.size
    val summary             =
      f"covered ${covered.size}/${inScope.size} (${ratio *
          100}%.1f%%) in-scope DuckDB functions; ${excluded.size} excluded"
    val byFamily = missing
      .groupBy(family)
      .toSeq
      .sortBy((f, names) => (-names.size, f))
      .map((f, names) => s"  ${f} (${names.size}): ${names.mkString(", ")}")
    val report = (
      Seq(summary, s"missing (${missing.size}):") ++ byFamily ++
        Seq(s"covered (${covered.size}): ${covered.mkString(", ")}")
    ).mkString("\n")
    info(report)
    val out = Path.of("target/engine-function-coverage.txt")
    Files.createDirectories(out.getParent)
    Files.writeString(out, report + "\n")
    if covered.size < minCovered then
      fail(s"Engine function coverage regressed: ${summary} (expected >= ${minCovered})")
  }

end EngineFunctionCoverageCheck

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.catalog

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableName

import scala.collection.mutable

/**
  * The built-in `tpch` and `tpcds` catalogs: `tpch.sf1.lineitem` or `tpcds.tiny.store_sales` names
  * a generated table ([[TpchGen]], [[TpcdsGen]]) with no setup, as Trino's tpch and tpcds
  * connectors do. The schema names the scale factor: `tiny` is 0.01, and `sf<n>` is n (`sf1`,
  * `sf10`). Rows are generated only while a query scans them, so naming a large scale factor costs
  * nothing until then.
  */
object BenchmarkCatalogs:
  /** Catalog name of the TPC-H tables */
  val Tpch = "tpch"

  /** Catalog name of the TPC-DS tables */
  val Tpcds = "tpcds"

  // Sources by (catalog, scale factor), so every query over a scale factor shares one source per
  // table
  private val cache = mutable.Map.empty[(String, Double), Map[String, TableSource]]

  /** The scale factor a schema name stands for: `tiny` is 0.01, `sf<n>` is n */
  def scaleFactorOf(schema: String): Option[Double] =
    schema.toLowerCase match
      case "tiny" =>
        Some(0.01)
      case s if s.startsWith("sf") =>
        s.drop(2).toDoubleOption.filter(_ > 0)
      case _ =>
        None

  private def generate(catalog: String, sf: Double): Seq[TableSource] =
    catalog match
      case Tpch =>
        TpchGen.sources(sf)
      case _ =>
        TpcdsGen.sources(sf)

  // The tables of a catalog at a scale factor, by lower-case name, generated once
  private def tablesOf(catalog: String, sf: Double): Map[String, TableSource] = cache.synchronized(
    cache.getOrElseUpdate(
      (catalog, sf),
      generate(catalog, sf).map(t => t.name.toLowerCase -> t).toMap
    )
  )

  private def isBenchmark(catalog: String): Boolean =
    val c = catalog.toLowerCase
    c == Tpch || c == Tpcds

  /**
    * The generated table that `fullName` (`catalog.schema.table`) names, or None when it isn't a
    * built-in benchmark table. Throws INVALID_ARGUMENT for a scale factor out of the generator's
    * range
    */
  def find(fullName: String): Option[TableSource] =
    fullName.toLowerCase.split("\\.").toList match
      case catalog :: schema :: table :: Nil if isBenchmark(catalog) =>
        scaleFactorOf(schema).flatMap(sf => tablesOf(catalog, sf).get(table))
      case _ =>
        None

  /**
    * The sorted table names of a built-in benchmark catalog's schema (`tpch`, `tiny`), or None when
    * the catalog isn't a benchmark or the schema isn't a scale factor. For `show tables`: every
    * scale factor has the same tables, so no schema list is needed
    */
  def tableNames(catalog: String, schema: String): Option[Seq[String]] =
    if isBenchmark(catalog) && scaleFactorOf(schema).isDefined then
      Some(tablesOf(catalog.toLowerCase, 0.01).keys.toSeq.sorted)
    else
      None

  /**
    * The compile-time definition of a built-in benchmark table, so the typer knows its column
    * types. Any scale-factor schema resolves here; the engine checks the scale factor's range when
    * the query runs
    */
  def tableDef(name: TableName): Option[Catalog.TableDef] =
    for
      catalog <- name.catalog.map(_.toLowerCase).filter(isBenchmark)
      schema  <- name.schema
      _       <- scaleFactorOf(schema)
      // Column types don't depend on the scale factor, so the tiny tables give them
      source <- tablesOf(catalog, 0.01).get(name.name.toLowerCase)
    yield Catalog.TableDef(
      TableName(Some(catalog), Some(schema), source.name),
      source.schema.fields.map(f => Catalog.TableColumn(f.name, f.ptype.toDataType))
    )

end BenchmarkCatalogs

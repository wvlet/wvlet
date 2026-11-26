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
package wvlet.lang.engine.catalog

/**
  * The primary keys the TPC-H and TPC-DS specifications declare, and the business keys unique in
  * every row (TPC-DS `c_customer_id`: customer, unlike item or store, is not a slowly changing
  * dimension), for tables loaded from their generated files ([[EngineCatalog.registerParquet]]) and
  * the generated TPC-H and TPC-DS tables ([[TpchGen]], [[TpcdsGen]]). Join estimates read them as
  * [[TableSource.uniqueKeys]]
  */
object BenchmarkKeys:
  private def key(cols: String*): Seq[Seq[String]] = Seq(cols.toSeq)

  private val tpchKeys: Map[String, Seq[Seq[String]]] = Map(
    "part"     -> key("p_partkey"),
    "supplier" -> key("s_suppkey"),
    "partsupp" -> key("ps_partkey", "ps_suppkey"),
    "customer" -> key("c_custkey"),
    "orders"   -> key("o_orderkey"),
    "lineitem" -> key("l_orderkey", "l_linenumber"),
    "nation"   -> key("n_nationkey"),
    "region"   -> key("r_regionkey")
  )

  private val tpcdsKeys: Map[String, Seq[Seq[String]]] = Map(
    "call_center"            -> key("cc_call_center_sk"),
    "catalog_page"           -> key("cp_catalog_page_sk"),
    "catalog_returns"        -> key("cr_item_sk", "cr_order_number"),
    "catalog_sales"          -> key("cs_item_sk", "cs_order_number"),
    "customer"               -> (key("c_customer_sk") ++ key("c_customer_id")),
    "customer_address"       -> key("ca_address_sk"),
    "customer_demographics"  -> key("cd_demo_sk"),
    "date_dim"               -> key("d_date_sk"),
    "household_demographics" -> key("hd_demo_sk"),
    "income_band"            -> key("ib_income_band_sk"),
    "inventory"              -> key("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"),
    "item"                   -> key("i_item_sk"),
    "promotion"              -> key("p_promo_sk"),
    "reason"                 -> key("r_reason_sk"),
    "ship_mode"              -> key("sm_ship_mode_sk"),
    "store"                  -> key("s_store_sk"),
    "store_returns"          -> key("sr_item_sk", "sr_ticket_number"),
    "store_sales"            -> key("ss_item_sk", "ss_ticket_number"),
    "time_dim"               -> key("t_time_sk"),
    "warehouse"              -> key("w_warehouse_sk"),
    "web_page"               -> key("wp_web_page_sk"),
    "web_returns"            -> key("wr_item_sk", "wr_order_number"),
    "web_sales"              -> key("ws_item_sk", "ws_order_number"),
    "web_site"               -> key("web_site_sk")
  )

  /** The primary key of a TPC-H table, or Nil for another name */
  def tpch(table: String): Seq[Seq[String]] = tpchKeys.getOrElse(table.toLowerCase, Nil)

  /** The unique keys of a TPC-DS table, the primary key first, or Nil for another name */
  def tpcds(table: String): Seq[Seq[String]] = tpcdsKeys.getOrElse(table.toLowerCase, Nil)

  /**
    * The unique keys of a table of `benchmark`, the primary key first: TPC-DS when its name
    * contains `tpcds`, TPC-H when it contains `tpch`, and Nil otherwise
    */
  def of(benchmark: String, table: String): Seq[Seq[String]] =
    val b = benchmark.toLowerCase
    if b.contains("tpcds") then
      tpcds(table)
    else if b.contains("tpch") then
      tpch(table)
    else
      Nil

end BenchmarkKeys

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

import wvlet.lang.compiler.*
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.uni.test.UniTest

/**
  * Inner joins are ordered by estimated output rows, using distinct counts from Parquet statistics
  * (TPC-DS data at sf=0.1)
  */
class EngineJoinOrderTest extends UniTest:
  private val (dataDir, tables) = EngineTPCDSTest.prepareData(EngineTPCDSTest.scaleFactor)
  private val engine            = WvletEngine()
  tables.foreach(t => engine.catalog.registerParquet(t, s"${dataDir}/${t}.parquet"))
  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def explain(query: String): String =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    engine.explain(q)

  /** Tables in the order the plan scans them */
  private def scanOrder(plan: String): Seq[String] =
    "Scan\\[([a-z_]+)\\]".r.findAllMatchIn(plan).map(_.group(1)).toSeq

  test("estimate distinct values from integer ranges and string columns") {
    val item = ParquetSource("item", s"${dataDir}/item.parquet")
    val rows = item.estimatedRows.get.toDouble
    // A surrogate key: one value per row
    item.estimatedDistinct("i_item_sk") shouldBe Some(rows)
    // Strings are counted by reading the column
    val colors = item.estimatedDistinct("i_color").get
    (colors > 50 && colors < 100) shouldBe true
    item.estimatedDistinct("no_such_column") shouldBe None
  }

  test("join a many-to-many key after the joins that reduce rows") {
    // q24: s_zip = ca_zip matches many addresses per store, so customer_address must join after
    // customer, which keys it by ca_address_sk
    val plan = explain("""from store_sales, store_returns, store, item, customer, customer_address
        |where ss_ticket_number = sr_ticket_number
        |  and ss_item_sk = sr_item_sk
        |  and ss_customer_sk = c_customer_sk
        |  and ss_item_sk = i_item_sk
        |  and ss_store_sk = s_store_sk
        |  and c_current_addr_sk = ca_address_sk
        |  and s_zip = ca_zip
        |  and s_market_id = 8
        |select c_last_name, ss_net_paid""".stripMargin)
    val order = scanOrder(plan)
    debug(plan)
    order.head shouldBe "store_sales"
    order.last shouldBe "customer_address"
  }

  test("join the input with a selective equality filter first") {
    // d_year = 2000 keeps one of ~200 years of date_dim, while s_gmt_offset = -5 is a default guess
    val plan = explain("""from store_sales, date_dim, store
        |where ss_sold_date_sk = d_date_sk
        |  and ss_store_sk = s_store_sk
        |  and d_year = 2000
        |  and s_gmt_offset = -5
        |select s_store_name, ss_sales_price""".stripMargin)
    scanOrder(plan) shouldBe Seq("store_sales", "date_dim", "store")
  }

  test("probe with a large input instead of building on it") {
    // customer_demographics is the largest input, but joining it to web_returns leaves few rows:
    // web_sales probes those rows rather than becoming a hash table
    val plan = explain("""from customer_demographics, web_returns, web_sales
        |where cd_demo_sk = wr_refunded_cdemo_sk
        |  and wr_item_sk = ws_item_sk
        |  and wr_order_number = ws_order_number
        |select cd_gender, ws_net_profit""".stripMargin)
    debug(plan)
    scanOrder(plan) shouldBe Seq("web_sales", "customer_demographics", "web_returns")
  }

end EngineJoinOrderTest

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
package wvlet.lang.ui.playground

import wvlet.uni.test.UniTest

/** The Lineage tab's graph of tables, seeds, models, and queries */
class ModelLineageTest extends UniTest:
  private val engine = PlaygroundEngine(0.001)

  private def lineageOf(example: String): ModelLineage =
    val out = engine.run(Examples.find(example).get.text)
    out.error shouldBe None
    out.lineage

  test("should connect layered models from source tables to the query") {
    val l = lineageOf("12_dbt_models/01_layered_models.wv")
    l.edges shouldContain LineageEdge("table:customer", "model:stg_customers")
    l.edges shouldContain LineageEdge("model:stg_orders", "model:int_customer_orders")
    l.edges shouldContain LineageEdge("table:nation", "model:int_customer_orders")
    l.edges shouldContain LineageEdge("model:dim_customers", "query:1")
    l.depth("table:customer") shouldBe 0
    l.depth("model:stg_customers") shouldBe 1
    l.depth("model:int_customer_orders") shouldBe 2
    l.depth("model:dim_customers") shouldBe 3
    l.depth("query:1") shouldBe 4
    l.related("model:stg_orders") shouldContain "query:1"
    l.related("model:stg_orders").contains("model:stg_customers") shouldBe false
    // Nothing is materialized, so wvlet build has nothing to build
    l.buildOrder shouldBe Nil
  }

  test("should show materializations and the build order") {
    val l   = lineageOf("12_dbt_models/02_materialized.wv")
    val stg = l.nodes.find(_.id == "model:stg_lineitem").get
    stg.materialized shouldBe true
    stg.detail shouldBe "view"
    l.nodes.find(_.id == "model:fct_monthly_revenue").get.detail shouldBe "table in marts"
    l.nodes.find(_.id == "model:shipped_lines").get.detail shouldBe "ephemeral"
    l.buildOrder shouldBe Seq("stg_lineitem", "fct_monthly_revenue")
  }

  test("should show seeds and parameterized models") {
    val seeds = lineageOf("12_dbt_models/04_seeds.wv")
    seeds.nodes.find(_.id == "seed:region_owner").get.kind shouldBe LineageKind.Seed
    seeds.edges shouldContain LineageEdge("seed:region_owner", "query:1")

    val macros = lineageOf("12_dbt_models/03_macros.wv")
    macros.nodes.find(_.id == "model:revenue_by").get.label shouldBe "revenue_by(year)"
    macros.edges shouldContain LineageEdge("model:revenue_by", "query:1")
  }

  test("should leave out with aliases and keep a node per query") {
    val l = lineageOf("06_queries/01_with.wv")
    l.nodes.map(_.id) shouldBe Seq("query:1", "table:orders")
    val tests = lineageOf("12_dbt_models/05_data_tests.wv")
    tests.nodes.count(_.kind == LineageKind.Query) shouldBe 3
  }

  test("should not show scalar vals as seeds") {
    // val min_balance = 9500 and val segment = 'MACHINERY' are values, not tables
    val l = lineageOf("06_queries/04_val_constants.wv")
    l.nodes.map(_.id) shouldBe Seq("query:1", "table:customer")
  }

  test("should mark the statement that ran") {
    val out = engine.run(Examples.find("12_dbt_models/01_layered_models.wv").get.text)
    out.statementLine shouldBe out.lineage.nodes.find(_.id == "query:1").get.line
  }

end ModelLineageTest

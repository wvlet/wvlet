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

import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.Catalog.TableName
import wvlet.uni.test.UniTest

/** The built-in `tpch` and `tpcds` catalogs */
class BenchmarkCatalogsTest extends UniTest:
  test("read the scale factor from the schema name as Trino does") {
    BenchmarkCatalogs.scaleFactorOf("tiny") shouldBe Some(0.01)
    BenchmarkCatalogs.scaleFactorOf("sf1") shouldBe Some(1.0)
    BenchmarkCatalogs.scaleFactorOf("SF100") shouldBe Some(100.0)
    BenchmarkCatalogs.scaleFactorOf("sf0") shouldBe None
    BenchmarkCatalogs.scaleFactorOf("main") shouldBe None
  }

  test("find a generated table by catalog.schema.table") {
    BenchmarkCatalogs.find("tpch.sf1.orders").flatMap(_.estimatedRows) shouldBe Some(1500000L)
    BenchmarkCatalogs.find("TPCDS.tiny.Customer").flatMap(_.estimatedRows) shouldBe Some(1000L)
    // One source per table and scale factor
    BenchmarkCatalogs.find("tpch.tiny.orders").get shouldBeTheSameInstanceAs
      BenchmarkCatalogs.find("tpch.tiny.orders").get
    BenchmarkCatalogs.find("tpch.tiny.store_sales") shouldBe None
    BenchmarkCatalogs.find("tpch.main.lineitem") shouldBe None
    BenchmarkCatalogs.find("tiny.lineitem") shouldBe None
    BenchmarkCatalogs.find("duckdb.tiny.lineitem") shouldBe None
  }

  test("reject a scale factor the generator doesn't support") {
    val e = intercept[WvletLangException] {
      BenchmarkCatalogs.find("tpcds.sf100.store_sales")
    }
    e.getMessage shouldContain "TPC-DS scale factor"
  }

  test("type the tables of any scale factor for the compiler") {
    val ds = BenchmarkCatalogs.tableDef(TableName(Some("tpcds"), Some("sf1"), "customer")).get
    ds.columns.map(_.name) shouldContain "c_customer_sk"
    val h = BenchmarkCatalogs.tableDef(TableName(Some("tpch"), Some("sf1"), "customer")).get
    h.columns.map(_.name) shouldContain "c_custkey"
    BenchmarkCatalogs.tableDef(TableName(None, Some("sf1"), "customer")) shouldBe None
    BenchmarkCatalogs.tableDef(TableName(Some("tpch"), Some("main"), "customer")) shouldBe None
  }

  test("resolve benchmark tables after registered ones") {
    val c         = EngineCatalog()
    val benchmark = BenchmarkCatalogs.find("tpch.tiny.nation")
    c.lookup("nation", "tpch.tiny.nation") shouldBe benchmark
    // A registered bare-named table doesn't hide the qualified benchmark table
    val region = TpchGen.sources(0.01).find(_.name == "region").get
    val nation = MemTable("nation", region.schema, Nil)
    c.register(nation)
    c.lookup("nation", "tpch.tiny.nation") shouldBe benchmark
    c.lookup("nation", "nation") shouldBe Some(nation)
    // A table registered under the qualified name comes first
    val mine = MemTable("tpch.tiny.nation", region.schema, Nil)
    c.register(mine)
    c.lookup("nation", "tpch.tiny.nation") shouldBe Some(mine)
    c.withNameKey(identity).lookup("nation", "tpch.tiny.nation") shouldBe Some(mine)
  }

end BenchmarkCatalogsTest

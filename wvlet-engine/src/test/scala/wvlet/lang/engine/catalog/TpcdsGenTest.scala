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
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.ValueOps
import wvlet.uni.test.UniTest

/** TPC-DS tables the specification defines completely, generated on every platform */
class TpcdsGenTest extends UniTest:
  private val sources = TpcdsGen.sources(0.1).map(s => s.name -> s).toMap

  // Sales, returns, and inventory at SF 0.01 (28,810 store sales lines in dsdgen)
  private val facts =
    TpcdsGen
      .sources(0.01)
      .filter(s => TpcdsFacts.schemas.contains(s.name))
      .map(s => s.name -> s)
      .toMap

  private def rowsOf(name: String): IndexedSeq[IndexedSeq[Any]] =
    sources(name)
      .scan(ScanSpec())
      .flatMap(b => Iterator.range(0, b.size).map(b.row(_).toIndexedSeq))
      .toIndexedSeq

  private lazy val dates = rowsOf("date_dim")

  private def date(y: Int, m: Int, d: Int): IndexedSeq[Any] = dates(
    ValueOps.daysFromCivil(y, m, d) - TpcdsGen.FirstEpochDay
  )

  test("should scale row counts as dsdgen does") {
    val counts: Map[String, Long] =
      sources
        .view
        .filterKeys(TpcdsGen.tableNames.take(5).contains)
        .mapValues(_.estimatedRows.get)
        .toMap
    counts shouldBe
      Map(
        "date_dim"               -> 73049L,
        "time_dim"               -> 86400L,
        "customer_demographics"  -> 192080L,
        "household_demographics" -> 7200L,
        "income_band"            -> 20L
      )
    dates.size shouldBe 73049
    def cd(sf: Double) =
      TpcdsGen.sources(sf).find(_.name == "customer_demographics").get.estimatedRows
    cd(0.01) shouldBe Some(19208L)
    cd(1) shouldBe Some(1920800L)
    cd(10) shouldBe Some(1920800L)
    intercept[WvletLangException] {
      TpcdsGen.sources(0)
    }
  }

  test("should encode business keys as dsdgen does") {
    TpcdsGen.businessKey(1) shouldBe "AAAAAAAABAAAAAAA"
    TpcdsGen.businessKey(3600) shouldBe "AAAAAAAAABOAAAAA"
    TpcdsGen.businessKey(2415022) shouldBe "AAAAAAAAOKJNECAA"
  }

  test("should generate dsdgen's calendar") {
    // Rows as DuckDB's dsdgen writes them, except d_last_dom (index 20)
    date(2000, 1, 1).patch(20, Nil, 1) shouldBe
      IndexedSeq[Any](
        2451545L,
        "AAAAAAAAJFIGFCAA",
        ValueOps.daysFromCivil(2000, 1, 1),
        1200L,
        5218L,
        401L,
        2000L,
        6L,
        1L,
        1L,
        1L,
        2000L,
        401L,
        5218L,
        "Saturday",
        "2000Q1",
        "Y",
        "Y",
        "N",
        2451545L,
        2451180L,
        2451453L,
        "N",
        "N",
        "N",
        "N",
        "N"
      )
    // dsdgen counts 1900 as a leap year: 1900-01-02 is a Monday, and 1900-03-01 a Thursday
    date(1900, 1, 2)(14) shouldBe "Monday"
    date(1900, 2, 28)(14) shouldBe "Tuesday"
    date(1900, 3, 1)(14) shouldBe "Thursday"
    date(2100, 1, 1)(14) shouldBe "Thursday"
    // Holidays are days 1, 186, 360, and 366 of the year
    dates.filter(r => r(6) == 2001L && r(16) == "Y").map(r => (r(8), r(9))) shouldBe
      Seq((1L, 1L), (7L, 5L), (12L, 26L))
    dates.count(_(16) == "Y") shouldBe 650
    // Friday and Saturday are the weekend
    dates.filter(_(17) == "Y").map(_(14).toString).distinct.sorted shouldBe
      Seq("Friday", "Saturday")
    // Same day last year and last quarter, at the month's end when it's shorter
    date(2000, 2, 29)(21) shouldBe 2451238L
    date(1999, 12, 31)(22) shouldBe 2451452L
    // dsdgen counts Apr 1 of a non-leap year in Q1, and 2003 as the current year
    date(2001, 4, 1)(10) shouldBe 1L
    date(2000, 4, 1)(10) shouldBe 2L
    dates.count(_(27) == "Y") shouldBe 365
  }

  test("should generate times of day with shifts and meals") {
    val t = rowsOf("time_dim")
    t.size shouldBe 86400
    t(25200) shouldBe
      IndexedSeq[Any](
        25200L,
        "AAAAAAAABHCGAAAA",
        25200L,
        7L,
        0L,
        0L,
        "AM",
        "first",
        "morning",
        "breakfast"
      )
    t(0)(9) shouldBe null
    val meals: Map[String, Int] = t.groupBy(r => String.valueOf(r(9))).view.mapValues(_.size).toMap
    meals shouldBe Map("null" -> 50400, "breakfast" -> 14400, "lunch" -> 10800, "dinner" -> 10800)
  }

  test("should enumerate the demographics combinations") {
    val cd = rowsOf("customer_demographics")
    cd(31360) shouldBe IndexedSeq[Any](31361L, "M", "M", "Primary", 4500L, "High Risk", 5L, 0L, 0)
    cd.last(0) shouldBe 192080L
    val hd = rowsOf("household_demographics")
    hd(999) shouldBe IndexedSeq[Any](1000L, 1L, "1001-5000", 8L, 0)
    hd.map(_(4).asInstanceOf[Int]).distinct.sorted shouldBe Seq(-1, 0, 1, 2, 3, 4)
    val ib = rowsOf("income_band")
    ib.take(2) shouldBe Seq(IndexedSeq[Any](1L, 0L, 10000), IndexedSeq[Any](2L, 10001L, 20000))
  }

  test("should scan split parts to the same rows as the whole scan") {
    Seq("date_dim", "customer_demographics").foreach { name =>
      val s     = sources(name)
      val parts = s.scanSplits(ScanSpec(columns = Some(Seq(s.schema.names(0))))).get
      parts.rows shouldBe
        IndexedSeq.tabulate(parts.parts.size)(b =>
          math.min(TpcdsGen.blockRows.toLong, s.estimatedRows.get - b * TpcdsGen.blockRows)
        )
      parts.parts.flatMap(_()).map(_.size).sum.toLong shouldBe s.estimatedRows.get
    }
    sources("income_band").scanSplits(ScanSpec()) shouldBe None
  }

  test("should bound every column by its range and distinct estimates") {
    // Fact tables are checked at a smaller scale below
    (
      sources.values.filterNot(s => TpcdsFacts.schemas.contains(s.name)) ++
        facts.values.filter(s => Seq("store_returns", "inventory").contains(s.name))
    ).foreach { s =>
      val all = values(s, s.schema.names*)
      s.schema
        .names
        .zipWithIndex
        .foreach { (c, i) =>
          val vs = all.map(_(i))
          s.columnRange(c)
            .foreach { (lo, hi) =>
              val ls = vs
                .filter(_ != null)
                .map {
                  case x: Long =>
                    x
                  case x: Int =>
                    x.toLong
                  case x =>
                    fail(s"Unexpected value of ${c}: ${x}")
                }
              (c, ls.min >= lo && ls.max <= hi) shouldBe (c, true)
            }
          s.partRanges(c)
            .foreach { parts =>
              // Each part's range is exactly that of its block's rows
              var start = 0
              parts.foreach { p =>
                val block = vs
                  .slice(start, start + p.rows.toInt)
                  .map {
                    case x: Long =>
                      x
                    case x: Int =>
                      x.toLong
                    case x =>
                      fail(s"Unexpected value of ${c}: ${x}")
                  }
                (c, start, block.min, block.max) shouldBe (c, start, p.min, p.max)
                start += p.rows.toInt
              }
              start shouldBe all.size
            }
          s.estimatedDistinct(c)
            .foreach { d =>
              (c, vs.filter(_ != null).distinct.size <= d) shouldBe (c, true)
            }
        }
    }
  }

  test("should scale dimension row counts as dsdgen does") {
    // Counts DuckDB's dsdgen writes at each scale factor
    val expected = Map(
      0.01 -> Seq(180, 1000, 500, 1, 1, 1, 1, 1, 3, 11718, 1, 20),
      0.05 -> Seq(900, 5000, 2500, 1, 1, 1, 1, 3, 15, 11718, 3, 20),
      0.1  -> Seq(1800, 10000, 5000, 1, 1, 1, 3, 6, 30, 11718, 7, 20),
      0.5  -> Seq(9000, 50000, 25000, 6, 2, 3, 15, 30, 150, 11718, 37, 20),
      1.0  -> Seq(18000, 100000, 50000, 12, 5, 6, 30, 60, 300, 11718, 75, 20),
      2.0  -> Seq(26000, 144000, 72000, 22, 5, 8, 30, 74, 322, 11718, 75, 20),
      3.0  -> Seq(36000, 188000, 94000, 32, 6, 10, 32, 90, 344, 11718, 75, 20),
      5.0  -> Seq(54000, 277000, 138000, 52, 7, 14, 34, 122, 388, 11718, 75, 20),
      10.0 -> Seq(102000, 500000, 250000, 102, 10, 24, 42, 200, 500, 11718, 75, 20)
    )
    val tables = Seq(
      "item",
      "customer",
      "customer_address",
      "store",
      "warehouse",
      "call_center",
      "web_site",
      "web_page",
      "promotion",
      "catalog_page",
      "reason",
      "ship_mode"
    )
    expected.foreach { (sf, counts) =>
      (sf, tables.map(TpcdsDimensions.rows(_, sf))) shouldBe (sf, counts)
    }
  }

  private lazy val sf1 = TpcdsGen.sources(1).map(s => s.name -> s).toMap

  private def values(s: TpcdsSource, columns: String*): IndexedSeq[IndexedSeq[Any]] =
    s.scan(ScanSpec(columns = Some(columns)))
      .flatMap(b => Iterator.range(0, b.size).map(b.row(_).toIndexedSeq))
      .toIndexedSeq

  test("should generate the values the TPC-DS queries filter on") {
    val items = values(sf1("item"), "i_category", "i_class").toSet
    Seq(
      "Books"       -> "computers",
      "Books"       -> "reference",
      "Books"       -> "self-help",
      "Electronics" -> "personal",
      "Electronics" -> "portable",
      "Electronics" -> "stereo",
      "Sports"      -> "football",
      "Men"         -> "accessories",
      "Men"         -> "pants",
      "Men"         -> "shirts",
      "Women"       -> "dresses",
      "Women"       -> "fragrances",
      "Women"       -> "maternity",
      "Music"       -> "classical",
      "Jewelry"     -> "birdal"
    ).foreach { (cat, cls) =>
      (cat, cls, items.contains(IndexedSeq(cat, cls))) shouldBe (cat, cls, true)
    }
    val brands = values(sf1("item"), "i_brand").flatten.toSet
    TpcdsDimensions.queryBrands.values.flatten.foreach(b => (b, brands(b)) shouldBe (b, true))
    val counties = values(sf1("customer_address"), "ca_county").flatten.toSet
    Seq("Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County")
      .foreach(c => (c, counties(c)) shouldBe (c, true))
    values(sf1("store"), "s_store_name").flatten shouldContain "ese"
    values(sf1("web_site"), "web_company_name").flatten shouldContain "pri"
    val carriers = values(sf1("ship_mode"), "sm_carrier").flatten
    carriers shouldContain "DHL"
    carriers shouldContain "BARIAN"
    val addresses = values(sf1("customer_address"), "ca_state", "ca_city", "ca_zip")
    val states    = addresses.map(_(0)).toSet
    Seq(
      "AR",
      "CA",
      "CO",
      "CT",
      "GA",
      "IA",
      "IL",
      "IN",
      "KY",
      "LA",
      "MN",
      "MS",
      "ND",
      "NJ",
      "NM",
      "OH",
      "OK",
      "OR",
      "TX",
      "VA",
      "WA",
      "WI"
    ).foreach(st => (st, states(st)) shouldBe (st, true))
    addresses.map(_(1)) shouldContain "Edgewood"
    val zips = addresses.map(_(2)).toSet
    TpcdsDimensions.queryZips.foreach(z => (z, zips(z)) shouldBe (z, true))
  }

  test("should point foreign keys at existing rows") {
    val customers = values(sf1("customer"), "c_current_addr_sk", "c_current_cdemo_sk")
    customers.forall(r =>
      r(0) == null || r(0).asInstanceOf[Long] <= sf1("customer_address").estimatedRows.get
    ) shouldBe true
    customers.forall(r =>
      r(1) == null || r(1).asInstanceOf[Long] <= sf1("customer_demographics").estimatedRows.get
    ) shouldBe true
    values(sf1("promotion"), "p_item_sk").forall(r =>
      r(0) == null || r(0).asInstanceOf[Long] <= sf1("item").estimatedRows.get
    ) shouldBe true
  }

  test("should estimate the business ids of a partial revision group") {
    // 22 stores at SF 2: 3 groups of 6 rows (9 ids), then 4 rows holding 3 more ids
    val stores = TpcdsGen.sources(2).find(_.name == "store").get
    val ids    = values(stores, "s_store_id").map(_(0)).distinct.size
    ids shouldBe 12
    stores.estimatedDistinct("s_store_id") shouldBe Some(12.0)
  }

  test("should keep half the revisions of slowly changing dimensions current") {
    val items = values(sf1("item"), "i_item_id", "i_rec_end_date")
    items.count(_(1) == null) shouldBe items.size / 2
    items.map(_(0)).distinct.size shouldBe items.size / 2
    // Every business id has exactly one current revision
    items.groupBy(_(0)).values.forall(_.count(_(1) == null) == 1) shouldBe true
  }

  private def factRows(name: String, columns: String*): IndexedSeq[IndexedSeq[Any]] = values(
    facts(name),
    columns*
  )

  private def long(v: Any): Long = v.asInstanceOf[Long]

  test("should generate dsdgen's fact table sizes") {
    // dsdgen's counts at SF 0.01; the generated ones are random around them except inventory's
    val expected = Map(
      "store_sales"     -> 28810L,
      "store_returns"   -> 2829L,
      "catalog_sales"   -> 14313L,
      "catalog_returns" -> 1377L,
      "web_sales"       -> 7212L,
      "web_returns"     -> 685L
    )
    expected.foreach { (t, n) =>
      val actual = facts(t).scan(ScanSpec(columns = Some(Nil))).map(_.size.toLong).sum
      // About 10% of a few thousand lines are returned, so allow 10%
      (t, math.abs(actual - n).toDouble / n < 0.1) shouldBe (t, true)
      (t, math.abs(facts(t).estimatedRows.get - n).toDouble / n < 0.1) shouldBe (t, true)
    }
    facts("inventory").scan(ScanSpec(columns = Some(Nil))).map(_.size).sum shouldBe 23490
    facts("inventory").estimatedRows shouldBe Some(23490L)
  }

  test("should share a ticket's customer, store, date, and time over distinct items") {
    val lines = factRows(
      "store_sales",
      "ss_ticket_number",
      "ss_customer_sk",
      "ss_store_sk",
      "ss_sold_date_sk",
      "ss_sold_time_sk",
      "ss_item_sk",
      "ss_cdemo_sk",
      "ss_hdemo_sk",
      "ss_addr_sk"
    )
    val tickets = lines.groupBy(_(0))
    tickets.size shouldBe 2400
    tickets.values.forall(t => t.size >= 8 && t.size <= 16) shouldBe true
    (Seq(1, 2, 3, 4) ++ Seq(6, 7, 8)).foreach { c =>
      (c, tickets.values.forall(_.map(_(c)).filter(_ != null).distinct.size <= 1)) shouldBe
        (c, true)
    }
    tickets.values.forall(t => t.map(_(5)).distinct.size == t.size) shouldBe true
    val orders = factRows("catalog_sales", "cs_order_number", "cs_sold_date_sk", "cs_ship_date_sk")
    orders.groupBy(_(0)).values.forall(o => o.size >= 4 && o.size <= 14) shouldBe true
    orders
      .filter(r => r(1) != null && r(2) != null)
      .forall(r => long(r(2)) - long(r(1)) >= 2 && long(r(2)) - long(r(1)) <= 90) shouldBe true
  }

  test("should derive sales amounts from quantity and prices") {
    val rows = factRows(
      "store_sales",
      "ss_quantity",
      "ss_wholesale_cost",
      "ss_list_price",
      "ss_sales_price",
      "ss_ext_sales_price",
      "ss_coupon_amt",
      "ss_net_paid",
      "ss_net_profit",
      "ss_ext_wholesale_cost",
      "ss_ext_discount_amt"
    ).filter(!_.contains(null))
    def d(v: Any): BigDecimal = BigDecimal(v.asInstanceOf[java.math.BigDecimal])
    rows.nonEmpty shouldBe true
    rows.forall(r => d(r(4)) == d(r(3)) * long(r(0))) shouldBe true
    rows.forall(r => d(r(6)) == d(r(4)) - d(r(5))) shouldBe true
    rows.forall(r => d(r(7)) == d(r(6)) - d(r(8))) shouldBe true
    rows.forall(r => d(r(9)) == d(r(5))) shouldBe true
    rows.forall(r => d(r(2)) >= d(r(1)) && d(r(2)) <= d(r(1)) * 2 && d(r(3)) <= d(r(2))) shouldBe
      true
  }

  test("should return lines of their sale") {
    val sold =
      factRows(
        "store_sales",
        "ss_ticket_number",
        "ss_item_sk",
        "ss_quantity",
        "ss_sales_price",
        "ss_sold_date_sk"
      ).map(r => (r(0), r(1)) -> r).toMap
    val returns = factRows(
      "store_returns",
      "sr_ticket_number",
      "sr_item_sk",
      "sr_return_quantity",
      "sr_return_amt",
      "sr_returned_date_sk",
      "sr_fee",
      "sr_return_ship_cost",
      "sr_return_tax",
      "sr_net_loss"
    )
    def d(v: Any): BigDecimal = BigDecimal(v.asInstanceOf[java.math.BigDecimal])
    returns.forall(r => sold.contains((r(0), r(1)))) shouldBe true
    val complete = returns.filter(r => !r.contains(null) && !sold((r(0), r(1))).contains(null))
    complete.nonEmpty shouldBe true
    complete.forall { r =>
      val s = sold((r(0), r(1)))
      long(r(2)) <= long(s(2)) && d(r(3)) == d(s(3)) * long(r(2)) && long(r(4)) - long(s(4)) >= 4 &&
      long(r(4)) - long(s(4)) <= 180 && d(r(8)) == d(r(5)) + d(r(6)) + d(r(7))
    } shouldBe true
  }

  test("should scan fact parts to the same rows as the whole scan") {
    val s     = facts("web_returns")
    val whole = values(s, s.schema.names*)
    val parts =
      s.scanSplits(ScanSpec()) match
        case Some(sp) =>
          sp.parts.flatMap(_()).flatMap(b => Iterator.range(0, b.size).map(b.row(_).toIndexedSeq))
        case None =>
          whole
    parts shouldBe whole
    val inv = facts("inventory")
    inv.scanSplits(ScanSpec()).get.rows shouldBe inv.partRanges("inv_date_sk").get.map(_.rows)
    // Estimates cover the inventory columns too
    inv.columnRange("inv_item_sk") shouldBe Some((1L, 180L))
    inv.estimatedDistinct("inv_date_sk") shouldBe Some(261.0)
    facts("store_sales").columnRange("ss_item_sk") shouldBe Some((1L, 180L))
  }

  test("should generate orders with fewer items than lines per order") {
    // SF 0.0005 has 9 items, fewer than a store ticket's 8 to 16 lines
    val tiny  = TpcdsGen.sources(0.0005).find(_.name == "store_sales").get
    val lines = values(tiny, "ss_ticket_number", "ss_item_sk").groupBy(_(0))
    lines.values.forall(t => t.size <= 9 && t.map(_(1)).distinct.size == t.size) shouldBe true
  }

end TpcdsGenTest

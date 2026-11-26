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

import wvlet.lang.engine.vector.*

import scala.util.Random

/**
  * The TPC-DS fact tables: the sales of the three channels with their returns, and the weekly
  * inventory, generated block by block while they're scanned.
  *
  * Their shape follows DuckDB's dsdgen:
  *   - Store tickets (240,000 per scale factor) of 8 to 16 lines of distinct items share the
  *     customer, store, date, and time. Catalog orders (160,000) have 4 to 14 lines shipped 2 to 90
  *     days later, web orders (60,000) 8 to 16 lines shipped 1 to 120 days later.
  *   - Sales dates span 1998-01-02 to 2003-01-02, with dsdgen's seasons: August to October about
  *     2.2 times as many sales a day as February to July, November and December 3.5 times.
  *   - Prices: list price is the wholesale cost times 1 to 2 (store) or 1 to 3 (catalog, web), the
  *     sales price a share of it, and the extended amounts, tax, coupon, net paid, and net profit
  *     derive from them as dsdgen's do (`ss_ext_discount_amt` equals the coupon, as in dsdgen).
  *   - About 10% of the lines are returned, matching their sale's (ticket or order, item), with a
  *     return quantity up to the sold one and a net loss of fee + shipping + tax.
  *   - Inventory holds, every week from 1998-01-01 (261 weeks), every warehouse's quantity of every
  *     item revision current that week.
  *   - Each table's share of NULLs in non-key columns.
  *
  * Generating a block of returns generates its block of sales too.
  */
private[catalog] object TpcdsFacts:

  val tableNames: Seq[String] = Seq(
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "inventory"
  )

  private val I64 = PType.Int64
  private val Dec = PType.decimal(7, 2)

  private def cols(names: String, t: PType): Seq[(String, PType)] = names
    .split(' ')
    .toSeq
    .map(_ -> t)

  private def schema(fields: Seq[(String, PType)]): Schema = GenColumns.schema(fields*)

  val schemas: Map[String, Schema] = Map(
    "store_sales" ->
      schema(
        cols(
          "ss_sold_date_sk ss_sold_time_sk ss_item_sk ss_customer_sk ss_cdemo_sk ss_hdemo_sk " +
            "ss_addr_sk ss_store_sk ss_promo_sk ss_ticket_number ss_quantity",
          I64
        ) ++
          cols(
            "ss_wholesale_cost ss_list_price ss_sales_price ss_ext_discount_amt " +
              "ss_ext_sales_price ss_ext_wholesale_cost ss_ext_list_price ss_ext_tax " +
              "ss_coupon_amt ss_net_paid ss_net_paid_inc_tax ss_net_profit",
            Dec
          )
      ),
    "store_returns" ->
      schema(
        cols(
          "sr_returned_date_sk sr_return_time_sk sr_item_sk sr_customer_sk sr_cdemo_sk " +
            "sr_hdemo_sk sr_addr_sk sr_store_sk sr_reason_sk sr_ticket_number sr_return_quantity",
          I64
        ) ++
          cols(
            "sr_return_amt sr_return_tax sr_return_amt_inc_tax sr_fee sr_return_ship_cost " +
              "sr_refunded_cash sr_reversed_charge sr_store_credit sr_net_loss",
            Dec
          )
      ),
    "catalog_sales" ->
      schema(
        cols(
          "cs_sold_date_sk cs_sold_time_sk cs_ship_date_sk cs_bill_customer_sk cs_bill_cdemo_sk " +
            "cs_bill_hdemo_sk cs_bill_addr_sk cs_ship_customer_sk cs_ship_cdemo_sk " +
            "cs_ship_hdemo_sk cs_ship_addr_sk cs_call_center_sk cs_catalog_page_sk " +
            "cs_ship_mode_sk cs_warehouse_sk cs_item_sk cs_promo_sk cs_order_number cs_quantity",
          I64
        ) ++
          cols(
            "cs_wholesale_cost cs_list_price cs_sales_price cs_ext_discount_amt " +
              "cs_ext_sales_price cs_ext_wholesale_cost cs_ext_list_price cs_ext_tax " +
              "cs_coupon_amt cs_ext_ship_cost cs_net_paid cs_net_paid_inc_tax " +
              "cs_net_paid_inc_ship cs_net_paid_inc_ship_tax cs_net_profit",
            Dec
          )
      ),
    "catalog_returns" ->
      schema(
        cols(
          "cr_returned_date_sk cr_returned_time_sk cr_item_sk cr_refunded_customer_sk " +
            "cr_refunded_cdemo_sk cr_refunded_hdemo_sk cr_refunded_addr_sk " +
            "cr_returning_customer_sk cr_returning_cdemo_sk cr_returning_hdemo_sk " +
            "cr_returning_addr_sk cr_call_center_sk cr_catalog_page_sk cr_ship_mode_sk " +
            "cr_warehouse_sk cr_reason_sk cr_order_number cr_return_quantity",
          I64
        ) ++
          cols(
            "cr_return_amount cr_return_tax cr_return_amt_inc_tax cr_fee cr_return_ship_cost " +
              "cr_refunded_cash cr_reversed_charge cr_store_credit cr_net_loss",
            Dec
          )
      ),
    "web_sales" ->
      schema(
        cols(
          "ws_sold_date_sk ws_sold_time_sk ws_ship_date_sk ws_item_sk ws_bill_customer_sk " +
            "ws_bill_cdemo_sk ws_bill_hdemo_sk ws_bill_addr_sk ws_ship_customer_sk " +
            "ws_ship_cdemo_sk ws_ship_hdemo_sk ws_ship_addr_sk ws_web_page_sk ws_web_site_sk " +
            "ws_ship_mode_sk ws_warehouse_sk ws_promo_sk ws_order_number ws_quantity",
          I64
        ) ++
          cols(
            "ws_wholesale_cost ws_list_price ws_sales_price ws_ext_discount_amt " +
              "ws_ext_sales_price ws_ext_wholesale_cost ws_ext_list_price ws_ext_tax " +
              "ws_coupon_amt ws_ext_ship_cost ws_net_paid ws_net_paid_inc_tax " +
              "ws_net_paid_inc_ship ws_net_paid_inc_ship_tax ws_net_profit",
            Dec
          )
      ),
    "web_returns" ->
      schema(
        cols(
          "wr_returned_date_sk wr_returned_time_sk wr_item_sk wr_refunded_customer_sk " +
            "wr_refunded_cdemo_sk wr_refunded_hdemo_sk wr_refunded_addr_sk " +
            "wr_returning_customer_sk wr_returning_cdemo_sk wr_returning_hdemo_sk " +
            "wr_returning_addr_sk wr_web_page_sk wr_reason_sk wr_order_number wr_return_quantity",
          I64
        ) ++
          cols(
            "wr_return_amt wr_return_tax wr_return_amt_inc_tax wr_fee wr_return_ship_cost " +
              "wr_refunded_cash wr_reversed_charge wr_account_credit wr_net_loss",
            Dec
          )
      ),
    "inventory" ->
      schema(
        cols("inv_date_sk inv_item_sk inv_warehouse_sk", I64) ++
          Seq("inv_quantity_on_hand" -> PType.Int32)
      )
  )

  /** Columns never NULL in dsdgen's output; the others are NULL at the table's share */
  val nonNull: Set[String] = Set(
    "ss_item_sk",
    "ss_ticket_number",
    "sr_item_sk",
    "sr_ticket_number",
    "cs_item_sk",
    "cs_order_number",
    "cs_net_paid_inc_ship",
    "cs_net_paid_inc_ship_tax",
    "cs_net_profit",
    "cr_returned_date_sk",
    "cr_returned_time_sk",
    "cr_item_sk",
    "cr_order_number",
    "ws_item_sk",
    "ws_order_number",
    "ws_net_paid_inc_ship",
    "ws_net_paid_inc_ship_tax",
    "ws_net_profit",
    "wr_item_sk",
    "wr_order_number",
    "inv_date_sk",
    "inv_item_sk",
    "inv_warehouse_sk"
  )

  val nullShare: Map[String, Double] = Map(
    "store_sales"     -> 0.045,
    "store_returns"   -> 0.035,
    "catalog_sales"   -> 0.005,
    "catalog_returns" -> 0.02,
    "web_sales"       -> 0.0003,
    "web_returns"     -> 0.044,
    "inventory"       -> 0.05
  )

  /** The first sales date, 1998-01-02, and the number of sales days to 2003-01-02 */
  val FirstSaleSk: Long = 2450816L
  val SaleDays: Int     = 1827

  /** The first inventory week, 1998-01-01, and the number of weeks */
  val FirstInventorySk: Long = 2450815L
  val InventoryWeeks: Int    = 261

  // Cumulative sales-day weights by month: dsdgen sells more in autumn and before Christmas
  private lazy val dayWeights: Array[Double] =
    val monthWeight = Array(1.2, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.1, 2.3, 2.2, 3.4, 3.5)
    val firstEpoch  = (FirstSaleSk - TpcdsGen.FirstDateSk + TpcdsGen.FirstEpochDay).toInt
    val cum         = new Array[Double](SaleDays)
    var total       = 0.0
    var i           = 0
    while i < SaleDays do
      val (_, m, _) = ValueOps.civilFromDays(firstEpoch + i)
      total += monthWeight(m - 1)
      cum(i) = total
      i += 1
    cum

  /** A sales date key drawn with dsdgen's seasons */
  def saleDateSk(r: Random): Long =
    val w  = dayWeights
    val x  = r.nextDouble() * w(SaleDays - 1)
    var lo = 0
    var hi = SaleDays - 1
    while lo < hi do
      val mid = (lo + hi) >>> 1
      if w(mid) < x then
        lo = mid + 1
      else
        hi = mid
    FirstSaleSk + lo

end TpcdsFacts

/** The fact tables at a scale factor, over the dimension sizes of `gen` */
private[catalog] class TpcdsFactGen(gen: TpcdsGen):
  import TpcdsFacts.*

  private val sf = gen.sf

  private def dim(table: String): Long = gen.rows(table).toLong

  private val numTickets      = math.max(1L, math.round(240000 * sf)).toInt
  private val numCatalogOrder = math.max(1L, math.round(160000 * sf)).toInt
  private val numWebOrder     = math.max(1L, math.round(60000 * sf)).toInt

  // Orders (tickets) per block
  private val blockOrders = TpcdsGen.blockRows

  private def orders(table: String): Int =
    table match
      case "store_sales" | "store_returns" =>
        numTickets
      case "catalog_sales" | "catalog_returns" =>
        numCatalogOrder
      case _ =>
        numWebOrder

  /** The number of blocks: orders for sales and returns, weeks for inventory */
  def numBlocks(table: String): Int =
    if table == "inventory" then
      InventoryWeeks
    else
      (orders(table) + blockOrders - 1) / blockOrders

  private def blockOrderRange(table: String, b: Int): (Int, Int) =
    val start = b * blockOrders
    (start, math.min(blockOrders, orders(table) - start))

  // Average lines per order (tickets of 8 to 16 lines, catalog orders of 4 to 14)
  private def linesPerOrder(table: String): Double =
    if table.startsWith("catalog") then
      9.0
    else
      12.0

  /** The rows of `table`: estimated, except inventory's */
  def estimatedRows(table: String): Long =
    table match
      case "inventory" =>
        (0 until InventoryWeeks).map(inventoryRows).sum
      case t if t.endsWith("_returns") =>
        (orders(t) * linesPerOrder(t) * 0.1).toLong
      case t =>
        (orders(t) * linesPerOrder(t)).toLong

  /** The rows of block `b`, known without generating only for inventory */
  def blockRowCount(table: String, b: Int): Option[Long] =
    if table == "inventory" then
      Some(inventoryRows(b))
    else
      None

  // Inventory: an item's revision k is current at a week by its place k % 6 in the revision
  // groups, so the rows of a week count items by that place
  private val numItems      = gen.rows("item")
  private val numWarehouses = gen.rows("warehouse")

  private def weekEpoch(w: Int): Int =
    (FirstInventorySk + 7L * w - TpcdsGen.FirstDateSk + TpcdsGen.FirstEpochDay).toInt

  private val revisionsByPlace: IndexedSeq[TpcdsDimensions.Revision] = (0 until 6).map(
    TpcdsDimensions.revision(_, TpcdsDimensions.itemBase)
  )

  private def current(place: Int, epochDay: Int): Boolean =
    val rv = revisionsByPlace(place)
    rv.start <= epochDay && (rv.end == Int.MinValue || epochDay <= rv.end)

  private lazy val inventoryRowCounts: Array[Long] =
    Array.tabulate(InventoryWeeks) { w =>
      val e = weekEpoch(w)
      (0 until 6).filter(current(_, e)).map(p => (numItems - p + 5) / 6).sum.toLong * numWarehouses
    }

  private def inventoryRows(w: Int): Long = inventoryRowCounts(w)

  // Values of a block, as Longs (money in cents), with Long.MinValue for NULL
  private final class Cols(table: String, n: Int, r: Random):
    private val names = schemas(table).names
    private val share = nullShare(table)
    val data          = Array.fill(names.size)(new Array[Long](n))

    def apply(name: String): Array[Long] = data(names.indexOf(name))

    def batch: Batch =
      val vecs = names
        .indices
        .map { c =>
          val v     = data(c)
          val t     = schemas(table)(c).ptype
          val nulls =
            if nonNull.contains(names(c)) then
              null
            else
              val m = Array.tabulate(n)(i => v(i) == Long.MinValue || r.nextDouble() < share)
              if m.contains(true) then
                m
              else
                null
          t match
            case PType.Int32 =>
              IntVec(t, v.map(_.toInt), nulls)
            case _ =>
              LongVec(t, v, nulls)
        }
      Batch(vecs.toArray, n)

  end Cols

  private def rnd(table: String, b: Int, salt: Int): Random = Random(
    19980102L + tableNames.indexOf(table) * 1000003L + b * 2654435761L + salt * 7919L
  )

  private def between(r: Random, lo: Int, hi: Int): Int = lo + r.nextInt(hi - lo + 1)

  private def key(r: Random, n: Long): Long = 1 + (r.nextDouble() * n).toLong.min(n - 1)

  private def cents(r: Random, lo: Long, hi: Long): Long =
    lo + (r.nextDouble() * (hi - lo + 1)).toLong.min(hi - lo)

  // Store keys of the current store revisions: places 0, 2, and 5 of each group of 6
  private val currentStores: Array[Long] =
    val all = (0 until dim("store").toInt).filter(k => Set(0, 2, 5).contains(k % 6)).map(_ + 1L)
    if all.isEmpty then
      Array(1L)
    else
      all.toArray

  /** The sales lines of a block and the indexes of the returned lines */
  private final case class Lines(
      n: Int,
      order: Array[Long],
      item: Array[Long],
      date: Array[Long],
      time: Array[Long],
      shipDate: Array[Long],
      customer: Array[Long],
      shipCustomer: Array[Long],
      quantity: Array[Long],
      wholesale: Array[Long],
      list: Array[Long],
      sales: Array[Long],
      coupon: Array[Long],
      taxRate: Array[Long],
      shipCost: Array[Long],
      perOrder: Array[Long],  // the store, call center, or warehouse of the order
      perOrder2: Array[Long], // the catalog order's warehouse, the web order's page
      cdemo: Array[Long],
      hdemo: Array[Long],
      addr: Array[Long],
      shipCdemo: Array[Long],
      shipHdemo: Array[Long],
      shipAddr: Array[Long],
      returned: Array[Int]
  )

  // Draw the lines of the block's orders. As in dsdgen, an order (ticket) keeps its customer, the
  // customer's demographics and address, date, time, and store, call center, or warehouse; each
  // line has a distinct item, its prices, promotion, and ship date. A web order's lines ship to
  // different customers
  private def lines(channel: String, b: Int): Lines =
    val table          = s"${channel}_sales"
    val (start, count) = blockOrderRange(table, b)
    val r              = rnd(table, b, 0)
    val (minL, maxL)   =
      channel match
        case "catalog" =>
          (4, 14)
        case _ =>
          (8, 16)
    // Items are distinct within an order, so an order has at most as many lines as there are
    // items (below scale factor 0.001)
    val perOrderLines = Array.fill(count)(math.min(between(r, minL, maxL), numItems))
    val n             = perOrderLines.sum
    def longs         = new Array[Long](n)
    val l             = Lines(
      n,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      longs,
      Array.empty
    )
    val numCd        = gen.numCustomerDemographics.toLong
    val numAddr      = dim("customer_address")
    val numCustomers = dim("customer")
    val markup       =
      if channel == "store" then
        100
      else
        200
    var row = 0
    var o   = 0
    while o < count do
      val date = saleDateSk(r)
      val time =
        if channel == "store" then
          between(r, 28800, 75599).toLong
        else
          r.nextInt(86400).toLong
      val customer = key(r, numCustomers)
      val cdemo    = key(r, numCd)
      val hdemo    = key(r, 7200)
      val addr     = key(r, numAddr)
      // A catalog order ships to its buyer 88% of the time
      val catalogShip =
        if channel == "catalog" && r.nextInt(100) >= 88 then
          Array(key(r, numCustomers), key(r, numCd), key(r, 7200), key(r, numAddr))
        else
          Array(customer, cdemo, hdemo, addr)
      val perOrder2 =
        channel match
          case "catalog" =>
            key(r, dim("warehouse"))
          case "web" =>
            key(r, dim("web_page"))
          case _ =>
            0L
      val perOrder =
        channel match
          case "store" =>
            currentStores(r.nextInt(currentStores.length))
          case "catalog" =>
            key(r, dim("call_center"))
          case _ =>
            key(r, dim("warehouse"))
      val lines = perOrderLines(o)
      val items = new Array[Long](lines)
      var j     = 0
      while j < lines do
        var item = key(r, numItems)
        // Distinct items within an order
        var k = 0
        while k < j do
          if items(k) == item then
            item = key(r, numItems)
            k = 0
          else
            k += 1
        items(j) = item
        val qty       = between(r, 1, 100).toLong
        val wholesale = cents(r, 100, 10000)
        val list      = wholesale * (100 + r.nextInt(markup + 1)) / 100
        val sales     = list * r.nextInt(101) / 100
        l.order(row) = start + o + 1L
        l.item(row) = item
        l.date(row) = date
        l.time(row) = time
        l.shipDate(row) =
          channel match
            case "catalog" =>
              date + between(r, 2, 90)
            case "web" =>
              date + between(r, 1, 120)
            case _ =>
              Long.MinValue
        l.customer(row) = customer
        l.cdemo(row) = cdemo
        l.hdemo(row) = hdemo
        l.addr(row) = addr
        // A web order's lines ship to its buyer 8% of the time
        val ship =
          if channel == "web" && r.nextInt(100) >= 8 then
            Array(key(r, numCustomers), key(r, numCd), key(r, 7200), key(r, numAddr))
          else
            catalogShip
        l.shipCustomer(row) = ship(0)
        l.shipCdemo(row) = ship(1)
        l.shipHdemo(row) = ship(2)
        l.shipAddr(row) = ship(3)
        l.perOrder2(row) = perOrder2
        l.quantity(row) = qty
        l.wholesale(row) = wholesale
        l.list(row) = list
        l.sales(row) = sales
        l.coupon(row) =
          if r.nextInt(100) < 19 then
            qty * sales * r.nextInt(101) / 100
          else
            0L
        l.taxRate(row) = r.nextInt(10).toLong
        l.shipCost(row) = qty * list * r.nextInt(51) / 100
        l.perOrder(row) = perOrder
        row += 1
        j += 1
      end while
      o += 1
    end while
    val returned = (0 until n).filter(_ => r.nextInt(100) < 10).toArray
    l.copy(returned = returned)

  end lines

  /** Generate block `b` of `table` */
  def block(table: String, b: Int): Batch =
    table match
      case "inventory" =>
        inventory(b)
      case t if t.endsWith("_sales") =>
        sales(t.stripSuffix("_sales"), b)
      case t =>
        returns(t.stripSuffix("_returns"), b)

  private def sales(channel: String, b: Int): Batch =
    val table = s"${channel}_sales"
    val l     = lines(channel, b)
    val r     = rnd(table, b, 1)
    val c     = Cols(table, l.n, r)
    val p     =
      channel.head match
        case 's' =>
          "ss_"
        case 'c' =>
          "cs_"
        case _ =>
          "ws_"
    def fill(name: String)(f: Int => Long): Unit =
      val a = c(p + name)
      var i = 0
      while i < l.n do
        a(i) = f(i)
        i += 1
    val ext     = (i: Int) => l.quantity(i) * l.sales(i)
    val netPaid = (i: Int) => ext(i) - l.coupon(i)
    val tax     = (i: Int) => netPaid(i) * l.taxRate(i) / 100
    fill("sold_date_sk")(l.date)
    fill("sold_time_sk")(l.time)
    fill("item_sk")(l.item)
    fill("quantity")(l.quantity)
    fill("wholesale_cost")(l.wholesale)
    fill("list_price")(l.list)
    fill("sales_price")(l.sales)
    fill("ext_sales_price")(ext)
    fill("ext_wholesale_cost")(i => l.quantity(i) * l.wholesale(i))
    fill("ext_list_price")(i => l.quantity(i) * l.list(i))
    fill("ext_tax")(tax)
    fill("coupon_amt")(l.coupon)
    fill("net_paid")(netPaid)
    fill("net_paid_inc_tax")(i => netPaid(i) + tax(i))
    fill("net_profit")(i => netPaid(i) - l.quantity(i) * l.wholesale(i))
    fill("promo_sk")(_ => key(r, dim("promotion")))
    channel match
      case "store" =>
        fill("ticket_number")(l.order)
        fill("customer_sk")(l.customer)
        fill("cdemo_sk")(l.cdemo)
        fill("hdemo_sk")(l.hdemo)
        fill("addr_sk")(l.addr)
        fill("store_sk")(l.perOrder)
        // dsdgen's store discount is the coupon
        fill("ext_discount_amt")(l.coupon)
      case _ =>
        fill("order_number")(l.order)
        fill("ship_date_sk")(l.shipDate)
        fill("bill_customer_sk")(l.customer)
        fill("ship_customer_sk")(l.shipCustomer)
        fill("bill_cdemo_sk")(l.cdemo)
        fill("bill_hdemo_sk")(l.hdemo)
        fill("bill_addr_sk")(l.addr)
        fill("ship_cdemo_sk")(l.shipCdemo)
        fill("ship_hdemo_sk")(l.shipHdemo)
        fill("ship_addr_sk")(l.shipAddr)
        fill("ship_mode_sk")(_ => key(r, dim("ship_mode")))
        fill("ext_discount_amt")(i => l.quantity(i) * l.list(i) - ext(i))
        fill("ext_ship_cost")(l.shipCost)
        fill("net_paid_inc_ship")(i => netPaid(i) + l.shipCost(i))
        fill("net_paid_inc_ship_tax")(i => netPaid(i) + l.shipCost(i) + tax(i))
        if channel == "catalog" then
          fill("call_center_sk")(l.perOrder)
          fill("catalog_page_sk")(_ => key(r, dim("catalog_page")))
          fill("warehouse_sk")(l.perOrder2)
        else
          fill("warehouse_sk")(l.perOrder)
          fill("web_page_sk")(l.perOrder2)
          fill("web_site_sk")(_ => key(r, dim("web_site")))
    end match
    c.batch

  end sales

  private def returns(channel: String, b: Int): Batch =
    val table = s"${channel}_returns"
    val l     = lines(channel, b)
    val rs    = l.returned
    val n     = rs.length
    val r     = rnd(table, b, 1)
    val c     = Cols(table, n, r)
    val p     =
      channel.head match
        case 's' =>
          "sr_"
        case 'c' =>
          "cr_"
        case _ =>
          "wr_"
    def fill(name: String)(f: Int => Long): Unit =
      val a = c(p + name)
      var i = 0
      while i < n do
        a(i) = f(i)
        i += 1
    val numCd   = gen.numCustomerDemographics.toLong
    val numAddr = dim("customer_address")
    val numCust = dim("customer")
    val qty     = Array.tabulate(n)(i => between(r, 1, l.quantity(rs(i)).toInt).toLong)
    val amt     = Array.tabulate(n)(i => qty(i) * l.sales(rs(i)))
    val tax     = Array.tabulate(n)(i => amt(i) * l.taxRate(rs(i)) / 100)
    val fee     = Array.fill(n)(cents(r, 50, 10000))
    val ship    = Array.tabulate(n)(i => qty(i) * l.list(rs(i)) * r.nextInt(51) / 100)
    // The amount with tax is refunded in cash, reversed, and credited in random shares
    val cash   = Array.tabulate(n)(i => (amt(i) + tax(i)) * r.nextInt(101) / 100)
    val charge = Array.tabulate(n)(i => (amt(i) + tax(i) - cash(i)) * r.nextInt(101) / 100)
    val (dateFrom, offsets) =
      channel match
        case "store" =>
          (l.date, (4, 180))
        case "catalog" =>
          (l.shipDate, (4, 180))
        case _ =>
          (l.shipDate, (2, 240))
    fill("returned_date_sk")(i => dateFrom(rs(i)) + between(r, offsets._1, offsets._2))
    fill(
      if channel == "store" then
        "return_time_sk"
      else
        "returned_time_sk"
    )(_ =>
      if channel == "store" then
        between(r, 28799, 61199).toLong
      else
        r.nextInt(86400).toLong
    )
    fill("item_sk")(i => l.item(rs(i)))
    fill("reason_sk")(_ => key(r, dim("reason")))
    fill("return_quantity")(qty(_))
    fill(
      if channel == "catalog" then
        "return_amount"
      else
        "return_amt"
    )(amt(_))
    fill("return_tax")(tax(_))
    fill("return_amt_inc_tax")(i => amt(i) + tax(i))
    fill("fee")(fee(_))
    fill("return_ship_cost")(ship(_))
    fill("refunded_cash")(cash(_))
    fill("reversed_charge")(charge(_))
    fill(
      if channel == "web" then
        "account_credit"
      else
        "store_credit"
    )(i => amt(i) + tax(i) - cash(i) - charge(i))
    fill("net_loss")(i => fee(i) + ship(i) + tax(i))
    channel match
      case "store" =>
        fill("ticket_number")(i => l.order(rs(i)))
        // Most returns are by the buyer, at the store of the sale
        fill("customer_sk")(i =>
          if r.nextInt(100) < 73 then
            l.customer(rs(i))
          else
            key(r, numCust)
        )
        fill("cdemo_sk")(_ => key(r, numCd))
        fill("hdemo_sk")(_ => key(r, 7200))
        fill("addr_sk")(_ => key(r, numAddr))
        // Returned at the store of the sale
        fill("store_sk")(i => l.perOrder(rs(i)))
      case _ =>
        fill("order_number")(i => l.order(rs(i)))
        // Catalog refunds go to the bill customer, and returns come from someone else 92% of the
        // time. Web returns come from anyone, refunded to them 93% of the time and otherwise to
        // the line's ship customer
        val returning =
          Array.tabulate(n) { i =>
            if channel == "catalog" && r.nextInt(100) < 8 then
              l.customer(rs(i))
            else
              key(r, numCust)
          }
        val refunded =
          Array.tabulate(n) { i =>
            if channel == "catalog" then
              l.customer(rs(i))
            else if r.nextInt(100) < 93 then
              returning(i)
            else
              l.shipCustomer(rs(i))
          }
        fill("refunded_customer_sk")(refunded(_))
        fill("returning_customer_sk")(returning(_))
        if channel == "catalog" then
          fill("refunded_cdemo_sk")(i => l.cdemo(rs(i)))
          fill("refunded_hdemo_sk")(i => l.hdemo(rs(i)))
          fill("refunded_addr_sk")(i => l.addr(rs(i)))
        else
          fill("refunded_cdemo_sk")(_ => key(r, numCd))
          fill("refunded_hdemo_sk")(_ => key(r, 7200))
          fill("refunded_addr_sk")(_ => key(r, numAddr))
        fill("returning_cdemo_sk")(_ => key(r, numCd))
        fill("returning_hdemo_sk")(_ => key(r, 7200))
        fill("returning_addr_sk")(_ => key(r, numAddr))
        if channel == "catalog" then
          fill("call_center_sk")(i => l.perOrder(rs(i)))
          fill("catalog_page_sk")(_ => key(r, dim("catalog_page")))
          fill("ship_mode_sk")(_ => key(r, dim("ship_mode")))
          fill("warehouse_sk")(i => l.perOrder2(rs(i)))
        else
          fill("web_page_sk")(i => l.perOrder2(rs(i)))
    end match
    c.batch

  end returns

  private def inventory(w: Int): Batch =
    val table = "inventory"
    val n     = inventoryRows(w).toInt
    val r     = rnd(table, w, 0)
    val c     = Cols(table, n, r)
    val e     = weekEpoch(w)
    val date  = c("inv_date_sk")
    val item  = c("inv_item_sk")
    val wh    = c("inv_warehouse_sk")
    val qoh   = c("inv_quantity_on_hand")
    var row   = 0
    var k     = 0
    while k < numItems do
      if current(k % 6, e) then
        var h = 1
        while h <= numWarehouses do
          date(row) = FirstInventorySk + 7L * w
          item(row) = k + 1L
          wh(row) = h
          qoh(row) = r.nextInt(1001)
          row += 1
          h += 1
      k += 1
    c.batch

  end inventory

  // A column's name without its table prefix (ss_, cr_, inv_, ...)
  private def unprefixed(column: String): String = column.substring(column.indexOf('_') + 1)

  /** The range of a fact column's values, from the specification of the generator */
  def columnRange(table: String, column: String): Option[(Long, Long)] =
    val name = unprefixed(column)
    name match
      case "sold_date_sk" =>
        Some((FirstSaleSk, FirstSaleSk + SaleDays - 1))
      case "date_sk" if table == "inventory" =>
        Some((FirstInventorySk, FirstInventorySk + 7L * (InventoryWeeks - 1)))
      case "item_sk" =>
        Some((1L, numItems.toLong))
      case "ticket_number" | "order_number" =>
        Some((1L, orders(table).toLong))
      case "quantity" | "return_quantity" =>
        Some((1L, 100L))
      case "warehouse_sk" =>
        Some((1L, numWarehouses.toLong))
      case "customer_sk" | "bill_customer_sk" | "ship_customer_sk" | "refunded_customer_sk" |
          "returning_customer_sk" =>
        Some((1L, dim("customer")))
      case "store_sk" =>
        Some((1L, dim("store")))
      case _ =>
        None

  /** Ranges of the order numbers in each block, which number orders block by block */
  def partRanges(table: String, column: String): Option[IndexedSeq[ColumnPartRange]] =
    val name = unprefixed(column)
    if (name == "ticket_number" || name == "order_number") && !table.endsWith("_returns") then
      Some(
        IndexedSeq.tabulate(numBlocks(table)) { b =>
          val (start, count) = blockOrderRange(table, b)
          ColumnPartRange(start + 1L, start.toLong + count, (count * linesPerOrder(table)).toLong)
        }
      )
    else if table == "inventory" && column == "inv_date_sk" then
      Some(
        IndexedSeq.tabulate(InventoryWeeks) { w =>
          val d = FirstInventorySk + 7L * w
          ColumnPartRange(d, d, inventoryRows(w))
        }
      )
    else
      None

  /** Distinct values of a fact column, for join ordering */
  def estimatedDistinct(table: String, column: String): Option[Double] =
    val rows              = estimatedRows(table).toDouble
    val name              = unprefixed(column)
    val n: Option[Double] =
      name match
        case "sold_date_sk" | "returned_date_sk" =>
          Some(SaleDays)
        case "date_sk" if table == "inventory" =>
          Some(InventoryWeeks)
        case "item_sk" =>
          Some(numItems)
        case "ticket_number" | "order_number" =>
          Some(orders(table))
        case "quantity" | "return_quantity" =>
          Some(100)
        case "warehouse_sk" =>
          Some(numWarehouses)
        case "store_sk" =>
          Some(currentStores.length)
        case "customer_sk" | "bill_customer_sk" | "ship_customer_sk" | "refunded_customer_sk" |
            "returning_customer_sk" =>
          Some(dim("customer"))
        case "promo_sk" =>
          Some(dim("promotion"))
        case "reason_sk" =>
          Some(dim("reason"))
        case "call_center_sk" =>
          Some(dim("call_center"))
        case "ship_mode_sk" =>
          Some(dim("ship_mode"))
        case "web_site_sk" =>
          Some(dim("web_site"))
        case "web_page_sk" =>
          Some(dim("web_page"))
        case "catalog_page_sk" =>
          Some(dim("catalog_page"))
        case "hdemo_sk" | "bill_hdemo_sk" | "ship_hdemo_sk" | "refunded_hdemo_sk" |
            "returning_hdemo_sk" =>
          Some(7200)
        case _ =>
          None
    n.map(math.min(_, rows))

  end estimatedDistinct

end TpcdsFactGen

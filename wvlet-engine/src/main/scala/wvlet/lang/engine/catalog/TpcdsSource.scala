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

/**
  * A TPC-DS table generated while it's scanned ([[TpcdsGen]]), block by block. The tables are fixed
  * formulas of the row index, so row counts, value ranges (exact per block for columns that grow
  * with the index), and distinct counts need no generated data.
  */
class TpcdsSource private[catalog] (val table: String, gen: TpcdsGen) extends GeneratedSource:
  import TpcdsGen.*

  val name: String   = table
  val schema: Schema = schemas(table)

  private val rows = gen.rows(table)

  protected val numBlocks: Int = gen.numBlocks(table)

  override def estimatedRows: Option[Long] = Some(rows.toLong)

  protected def generateBlock(b: Int): Batch = gen.block(table, b)

  protected def blockRowCount(b: Int): Option[Long] = Some(gen.blockSize(table, b).toLong)

  private def epochDay(i: Int): Int = FirstEpochDay + i
  private def year(i: Int): Long    = ValueOps.civilFromDays(epochDay(i))._1.toLong

  // Columns whose value never decreases with the row index i, as a function of i
  private def ascending(column: String): Option[Int => Long] =
    column match
      case "d_date_sk" =>
        Some(FirstDateSk + _)
      case "d_date" =>
        Some(epochDay(_).toLong)
      case "d_year" | "d_fy_year" =>
        Some(year)
      case "d_month_seq" =>
        Some { i =>
          val (y, m, _) = ValueOps.civilFromDays(epochDay(i))
          (y - 1900) * 12L + m - 1
        }
      case "d_week_seq" | "d_fy_week_seq" =>
        Some(i => (i + 7L) / 7)
      case "t_time_sk" | "t_time" =>
        Some(_.toLong)
      case "t_hour" =>
        Some(_ / 3600L)
      case "cd_demo_sk" | "hd_demo_sk" | "ib_income_band_sk" | "i_item_sk" | "c_customer_sk" |
          "ca_address_sk" | "s_store_sk" | "w_warehouse_sk" | "cc_call_center_sk" | "web_site_sk" |
          "wp_web_page_sk" | "p_promo_sk" | "cp_catalog_page_sk" | "r_reason_sk" |
          "sm_ship_mode_sk" =>
        Some(_ + 1L)
      case _ =>
        None

  // Ranges of columns that don't grow with the row index
  private def fixedRange(column: String): Option[(Long, Long)] =
    column match
      case "d_dow" =>
        Some((0L, 6L))
      case "d_moy" =>
        Some((1L, 12L))
      case "d_dom" =>
        Some((1L, 31L))
      case "d_qoy" =>
        Some((1L, 4L))
      case "t_minute" | "t_second" =>
        Some((0L, 59L))
      case "hd_income_band_sk" =>
        Some((1L, 20L))
      case "hd_dep_count" =>
        Some((0L, 9L))
      case "hd_vehicle_count" =>
        Some((-1L, 4L))
      case "i_category_id" =>
        Some((1L, TpcdsDimensions.categories.size.toLong))
      case "i_class_id" =>
        Some((1L, TpcdsDimensions.categories.map(_._2.size).max.toLong))
      case "i_manufact_id" =>
        Some((1L, 1000L))
      case "i_manager_id" =>
        Some((1L, 100L))
      case "c_birth_year" =>
        Some((1924L, 1992L))
      case "c_birth_month" =>
        Some((1L, 12L))
      case "c_current_hdemo_sk" =>
        Some((1L, 7200L))
      case "c_current_cdemo_sk" =>
        Some((1L, gen.numCustomerDemographics.toLong))
      case "c_current_addr_sk" =>
        Some((1L, gen.rows("customer_address").toLong))
      case _ =>
        None

  override def columnRange(column: String): Option[(Long, Long)] = ascending(column)
    .map(f => (f(0), f(rows - 1)))
    .orElse(fixedRange(column))

  override def partRanges(column: String): Option[IndexedSeq[ColumnPartRange]] = ascending(column)
    .map { f =>
      IndexedSeq.tabulate(numBlocks) { b =>
        val start = gen.blockStart(b)
        val n     = gen.blockSize(table, b)
        ColumnPartRange(f(start), f(start + n - 1), n.toLong)
      }
    }

  // The values of a customer_demographics column taking `domain` values, each for `stride` rows
  private def prefixDistinct(domain: Int, stride: Int): Double = math.min(
    domain.toDouble,
    math.ceil(rows.toDouble / stride)
  )

  override def estimatedDistinct(column: String): Option[Double] =
    val n: Option[Double] =
      column match
        case "d_date_sk" | "d_date_id" | "d_date" | "t_time_sk" | "t_time_id" | "t_time" |
            "cd_demo_sk" | "hd_demo_sk" | "ib_income_band_sk" | "ib_lower_bound" |
            "ib_upper_bound" | "i_item_sk" | "c_customer_sk" | "c_customer_id" | "ca_address_sk" |
            "ca_address_id" | "s_store_sk" | "w_warehouse_sk" | "w_warehouse_id" |
            "cc_call_center_sk" | "web_site_sk" | "wp_web_page_sk" | "p_promo_sk" | "p_promo_id" |
            "cp_catalog_page_sk" | "cp_catalog_page_id" | "r_reason_sk" | "r_reason_id" |
            "r_reason_desc" | "sm_ship_mode_sk" | "sm_ship_mode_id" | "sm_carrier" =>
          Some(rows)
        // Business ids of slowly changing dimensions: 3 for every 6 rows, in revisions of 1, 2, and 3
        case "i_item_id" | "s_store_id" | "cc_call_center_id" | "web_site_id" | "wp_web_page_id" =>
          Some(3 * (rows / 6) + IndexedSeq(0, 1, 2, 2, 3, 3)(rows % 6))
        case "i_category" | "i_category_id" =>
          Some(TpcdsDimensions.categories.size)
        case "i_class" =>
          Some(TpcdsDimensions.categories.map(_._2.size).sum)
        case "i_class_id" =>
          Some(TpcdsDimensions.categories.map(_._2.size).max)
        case "i_manufact_id" | "i_manufact" =>
          Some(1000)
        case "i_manager_id" =>
          Some(100)
        case "i_color" =>
          Some(TpcdsDimensions.colors.size)
        case "i_size" =>
          Some(TpcdsDimensions.sizes.size)
        case "i_units" =>
          Some(TpcdsDimensions.units.size)
        case "ca_state" =>
          Some(TpcdsDimensions.states.size)
        case "ca_city" =>
          Some(TpcdsDimensions.cities.size)
        case "ca_county" =>
          Some(TpcdsDimensions.counties.size)
        case "ca_zip" =>
          Some(4000)
        case "c_birth_country" =>
          Some(TpcdsDimensions.countries.size)
        case "c_birth_year" =>
          Some(69)
        case "c_birth_month" =>
          Some(12)
        case "c_current_hdemo_sk" =>
          Some(7200)
        case "c_current_addr_sk" =>
          Some(gen.rows("customer_address"))
        case "c_current_cdemo_sk" =>
          Some(gen.numCustomerDemographics)
        case "c_preferred_cust_flag" | "p_channel_dmail" | "wp_autogen_flag" =>
          Some(2)
        case "d_year" | "d_fy_year" =>
          Some(201)
        case "d_month_seq" =>
          Some(2401)
        case "d_week_seq" | "d_fy_week_seq" =>
          Some(10436)
        case "d_moy" =>
          Some(12)
        case "d_dom" =>
          Some(31)
        case "d_qoy" =>
          Some(4)
        case "d_dow" | "d_day_name" =>
          Some(7)
        case "t_hour" =>
          Some(24)
        case "t_minute" | "t_second" =>
          Some(60)
        case "t_am_pm" | "cd_gender" | "d_holiday" | "d_weekend" | "d_following_holiday" =>
          Some(2)
        case "t_shift" | "t_meal_time" =>
          Some(3)
        case "t_sub_shift" =>
          Some(4)
        case "cd_marital_status" =>
          Some(maritalStatus.size)
        // Below scale factor 1 the table is a prefix of the cross product, where the slower
        // varying columns take fewer values
        case "cd_education_status" =>
          Some(prefixDistinct(7, 10))
        case "cd_dep_count" =>
          Some(prefixDistinct(7, 5600))
        case "cd_dep_employed_count" =>
          Some(prefixDistinct(7, 39200))
        case "cd_dep_college_count" =>
          Some(prefixDistinct(7, 274400))
        case "cd_purchase_estimate" =>
          Some(prefixDistinct(20, 70))
        case "cd_credit_rating" =>
          Some(prefixDistinct(4, 1400))
        case "hd_income_band_sk" =>
          Some(20)
        case "hd_buy_potential" | "hd_vehicle_count" =>
          Some(6)
        case "hd_dep_count" =>
          Some(10)
        case _ =>
          None
    n.map(math.min(_, rows.toDouble))

  end estimatedDistinct

  // Tables of the same name at the same scale factor hold the same rows
  /** The primary key of the table in the TPC-DS specification */
  override def uniqueKeys: Seq[Seq[String]] = BenchmarkKeys.tpcds(table)

  override def key: Any = ("tpcds", gen.sf, table)

end TpcdsSource

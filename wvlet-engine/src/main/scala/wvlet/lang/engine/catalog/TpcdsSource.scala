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
      case "cd_demo_sk" | "hd_demo_sk" | "ib_income_band_sk" =>
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

  override def estimatedDistinct(column: String): Option[Double] =
    val n: Option[Double] =
      column match
        case "d_date_sk" | "d_date_id" | "d_date" | "t_time_sk" | "t_time_id" | "t_time" |
            "cd_demo_sk" | "hd_demo_sk" | "ib_income_band_sk" | "ib_lower_bound" |
            "ib_upper_bound" =>
          Some(rows)
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
        case "t_sub_shift" | "cd_credit_rating" =>
          Some(4)
        case "cd_marital_status" =>
          Some(maritalStatus.size)
        case "cd_education_status" | "cd_dep_count" | "cd_dep_employed_count" |
            "cd_dep_college_count" =>
          Some(7)
        case "cd_purchase_estimate" | "hd_income_band_sk" =>
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
  override def key: Any = ("tpcds", gen.sf, table)

end TpcdsSource

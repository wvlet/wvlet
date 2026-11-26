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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*
import GenColumns.*

/**
  * Generates TPC-DS tables in memory, on every platform, with no DuckDB or data files, block by
  * block while they're scanned ([[TpcdsSource]]).
  *
  * The tables here are the ones the TPC-DS specification defines completely, so their rows equal
  * those DuckDB's `dsdgen` writes, including dsdgen's calendar (it treats 1900 as a leap year,
  * which shifts the weekdays of January and February 1900 and the day-of-year holidays):
  *   - `date_dim`: every day from 1900-01-02 to 2100-01-01, keyed by Julian day number
  *   - `time_dim`: every second of a day
  *   - `customer_demographics`: the cross product of its column domains (1,920,800 rows at scale
  *     factor 1 and above, a prefix of them below)
  *   - `household_demographics`, `income_band`: fixed tables
  *
  * The one known difference is `d_last_dom`, the last day of the month: dsdgen's values don't
  * follow the calendar, and these follow it.
  *
  * Column types match the Parquet files DuckDB's `dsdgen` writes.
  */
object TpcdsGen:

  /** The generated tables */
  val tableNames: Seq[String] = Seq(
    "date_dim",
    "time_dim",
    "customer_demographics",
    "household_demographics",
    "income_band"
  )

  /** The largest scale factor [[sources]] generates */
  val maxScaleFactor: Double = 1000.0

  /** Rows per generated block */
  val blockRows: Int = 10000

  /**
    * The tables at scale factor `sf` as sources that generate their rows while they're scanned.
    *
    * @param sf
    *   the scale factor, in (0, [[maxScaleFactor]]]
    */
  def sources(sf: Double): Seq[TpcdsSource] =
    checkScaleFactor("TPC-DS", sf, maxScaleFactor)
    val gen = TpcdsGen(sf)
    tableNames.map(TpcdsSource(_, gen))

  /** Register every table at scale factor `sf` in `catalog`, generated while it's scanned */
  def register(catalog: EngineCatalog, sf: Double): Seq[TpcdsSource] =
    val ts = sources(sf)
    ts.foreach(catalog.register)
    ts

  /** The Julian day number of the first date, 1900-01-02 */
  private[catalog] val FirstDateSk: Long = 2415022L

  /** The number of days in date_dim, to 2100-01-01 */
  private[catalog] val NumDates: Int = 73049

  /** 1900-01-02 in days since 1970-01-01 */
  private[catalog] val FirstEpochDay: Int = ValueOps.daysFromCivil(1900, 1, 2)

  /** The full cross product of the customer demographics domains */
  private[catalog] val MaxCustomerDemographics: Int = 1920800

  private val I64 = PType.Int64
  private val I32 = PType.Int32
  private val Str = PType.Str

  /** The columns of each table */
  private[catalog] val schemas: Map[String, Schema] = Map(
    "date_dim" ->
      schema(
        "d_date_sk"           -> I64,
        "d_date_id"           -> Str,
        "d_date"              -> PType.Date,
        "d_month_seq"         -> I64,
        "d_week_seq"          -> I64,
        "d_quarter_seq"       -> I64,
        "d_year"              -> I64,
        "d_dow"               -> I64,
        "d_moy"               -> I64,
        "d_dom"               -> I64,
        "d_qoy"               -> I64,
        "d_fy_year"           -> I64,
        "d_fy_quarter_seq"    -> I64,
        "d_fy_week_seq"       -> I64,
        "d_day_name"          -> Str,
        "d_quarter_name"      -> Str,
        "d_holiday"           -> Str,
        "d_weekend"           -> Str,
        "d_following_holiday" -> Str,
        "d_first_dom"         -> I64,
        "d_last_dom"          -> I64,
        "d_same_day_ly"       -> I64,
        "d_same_day_lq"       -> I64,
        "d_current_day"       -> Str,
        "d_current_week"      -> Str,
        "d_current_month"     -> Str,
        "d_current_quarter"   -> Str,
        "d_current_year"      -> Str
      ),
    "time_dim" ->
      schema(
        "t_time_sk"   -> I64,
        "t_time_id"   -> Str,
        "t_time"      -> I64,
        "t_hour"      -> I64,
        "t_minute"    -> I64,
        "t_second"    -> I64,
        "t_am_pm"     -> Str,
        "t_shift"     -> Str,
        "t_sub_shift" -> Str,
        "t_meal_time" -> Str
      ),
    "customer_demographics" ->
      schema(
        "cd_demo_sk"            -> I64,
        "cd_gender"             -> Str,
        "cd_marital_status"     -> Str,
        "cd_education_status"   -> Str,
        "cd_purchase_estimate"  -> I64,
        "cd_credit_rating"      -> Str,
        "cd_dep_count"          -> I64,
        "cd_dep_employed_count" -> I64,
        "cd_dep_college_count"  -> I32
      ),
    "household_demographics" ->
      schema(
        "hd_demo_sk"        -> I64,
        "hd_income_band_sk" -> I64,
        "hd_buy_potential"  -> Str,
        "hd_dep_count"      -> I64,
        "hd_vehicle_count"  -> I32
      ),
    "income_band" ->
      schema("ib_income_band_sk" -> I64, "ib_lower_bound" -> I64, "ib_upper_bound" -> I32)
  )

  private[catalog] val dayNames = IndexedSeq(
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"
  )

  private[catalog] val genders         = IndexedSeq("M", "F")
  private[catalog] val maritalStatus   = IndexedSeq("M", "S", "D", "W", "U")
  private[catalog] val educationStatus = IndexedSeq(
    "Primary",
    "Secondary",
    "College",
    "2 yr Degree",
    "4 yr Degree",
    "Advanced Degree",
    "Unknown"
  )

  private[catalog] val creditRatings = IndexedSeq("Good", "Low Risk", "High Risk", "Unknown")
  private[catalog] val buyPotentials = IndexedSeq(
    "0-500",
    "501-1000",
    "1001-5000",
    "5001-10000",
    ">10000",
    "Unknown"
  )

  private[catalog] val vehicleCounts = IndexedSeq(0, 1, 2, 3, 4, -1)

  /**
    * The business key (`*_id`) of surrogate key `n`, as dsdgen writes it: 8 `A`s, then the hex
    * digits of n, lowest first, as the letters A to P
    */
  def businessKey(n: Long): String =
    val sb = StringBuilder("AAAAAAAA")
    var v  = n
    var i  = 0
    while i < 8 do
      sb.append(('A' + (v & 15).toInt).toChar)
      v >>>= 4
      i += 1
    sb.toString

end TpcdsGen

/** The generator at a scale factor; the tables here need no randomness */
private[catalog] class TpcdsGen(val sf: Double):
  import TpcdsGen.*

  val numCustomerDemographics: Int =
    math.min(MaxCustomerDemographics.toLong, math.round(MaxCustomerDemographics * sf)).max(1L).toInt

  /** The rows of `table` */
  def rows(table: String): Int =
    table match
      case "date_dim" =>
        NumDates
      case "time_dim" =>
        86400
      case "customer_demographics" =>
        numCustomerDemographics
      case "household_demographics" =>
        7200
      case "income_band" =>
        20
      case other =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Not a generated TPC-DS table: ${other}")

  /** The number of blocks of `table` */
  def numBlocks(table: String): Int = (rows(table) + blockRows - 1) / blockRows

  /** The first row index of block `b` */
  def blockStart(b: Int): Int = b * blockRows

  /** The rows of block `b` of `table` */
  def blockSize(table: String, b: Int): Int = math.min(blockRows, rows(table) - blockStart(b))

  /** Generate block `b` of `table`, with every column */
  def block(table: String, b: Int): Batch =
    val start = blockStart(b)
    val n     = blockSize(table, b)
    table match
      case "date_dim" =>
        dateDim(start, n)
      case "time_dim" =>
        timeDim(start, n)
      case "customer_demographics" =>
        customerDemographics(start, n)
      case "household_demographics" =>
        householdDemographics(start, n)
      case _ =>
        incomeBand(start, n)

  private def flags(n: Int)(f: Int => Boolean): Vec =
    strs(n)(i =>
      if f(i) then
        "Y"
      else
        "N"
    )

  private def batch(n: Int, columns: Vec*): Batch = Batch(columns.toArray, n)

  private def isLeap(y: Int): Boolean = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

  private def daysInMonth(y: Int, m: Int): Int =
    m match
      case 2 =>
        if isLeap(y) then
          29
        else
          28
      case 4 | 6 | 9 | 11 =>
        30
      case _ =>
        31

  private val Mar1st1900 = ValueOps.daysFromCivil(1900, 3, 1)

  // dsdgen's weekday (0 = Sunday): it counts 1900 as a leap year, so the days before 1900-03-01
  // (and its last day, 2100-01-01) are one weekday earlier than the calendar's
  private def dsdgenDow(epochDay: Int, sk: Long): Int =
    val dow = Math.floorMod(epochDay + 4, 7)
    if epochDay < Mar1st1900 || sk == FirstDateSk + NumDates - 1 then
      (dow + 6) % 7
    else
      dow

  // dsdgen's day of the year of a date, counting 1900 as a leap year
  private def dayOfYear(epochDay: Int, y: Int, m: Int): Int =
    val doy = epochDay - ValueOps.daysFromCivil(y, 1, 1) + 1
    if y == 1900 && m > 2 then
      doy + 1
    else
      doy

  private def dayOfYear(epochDay: Int): Int =
    val (y, m, _) = ValueOps.civilFromDays(epochDay)
    dayOfYear(epochDay, y, m)

  // dsdgen's holidays are days 1, 186, 360, and 366 of the year (Jan 1, Jul 4, Dec 25, Dec 31 of
  // a leap year)
  private def isHolidayDay(doy: Int): Boolean = doy == 1 || doy == 186 || doy == 360 || doy == 366

  // dsdgen's quarter of the year, from the day of the year with a leap year's quarter ends, so
  // that Apr 1, Jul 1, and Oct 1 of other years fall in the quarter before
  private def quarterOfYear(doy: Int): Int =
    if doy <= 91 then
      1
    else if doy <= 182 then
      2
    else if doy <= 274 then
      3
    else
      4

  // dsdgen's same day last quarter: the date minus the length of the calendar quarter before
  // (quarter `q` of the calendar, not dsdgen's quarterOfYear)
  private def previousQuarterDays(y: Int, q: Int): Int =
    q match
      case 1 | 4 =>
        92
      case 2 =>
        if isLeap(y) then
          91
        else
          90
      case _ =>
        91

  // The same day a year earlier, Feb 28 for Feb 29
  private def yearEarlier(y: Int, m: Int, d: Int): Int = ValueOps.daysFromCivil(
    y - 1,
    m,
    math.min(d, daysInMonth(y - 1, m))
  )

  private def dateDim(start: Int, n: Int): Batch =
    val sk                       = Array.tabulate(n)(i => FirstDateSk + start + i)
    val epoch                    = Array.tabulate(n)(i => FirstEpochDay + start + i)
    val ymd                      = epoch.map(ValueOps.civilFromDays)
    val year                     = ymd.map(_._1)
    val month                    = ymd.map(_._2)
    val day                      = ymd.map(_._3)
    val dow                      = Array.tabulate(n)(i => dsdgenDow(epoch(i), sk(i)))
    def monthSeq(i: Int): Long   = (year(i) - 1900) * 12L + month(i) - 1
    def weekSeq(i: Int): Long    = (sk(i) - FirstDateSk + 7) / 7
    def quarterSeq(i: Int): Long = (monthSeq(i) + 4) / 3
    val doy                      = Array.tabulate(n)(i => dayOfYear(epoch(i), year(i), month(i)))
    val qoy                      = doy.map(quarterOfYear)
    val holiday                  = doy.map(isHolidayDay)
    // The day before the block's first day is in the block before
    def followsHoliday(i: Int): Boolean =
      if i > 0 then
        holiday(i - 1)
      else
        isHolidayDay(dayOfYear(epoch(0) - 1))
    def toSk(epochDay: Int): Long = epochDay - FirstEpochDay + FirstDateSk
    val firstDom                  = Array.tabulate(n)(i => sk(i) - day(i) + 1)
    batch(
      n,
      LongVec(PType.Int64, sk, null),
      strs(n)(i => businessKey(sk(i))),
      IntVec(PType.Date, epoch, null),
      longs(n)(monthSeq),
      longs(n)(weekSeq),
      longs(n)(quarterSeq),
      longs(n)(year(_).toLong),
      longs(n)(dow(_).toLong),
      longs(n)(month(_).toLong),
      longs(n)(day(_).toLong),
      longs(n)(qoy(_).toLong),
      longs(n)(year(_).toLong),
      longs(n)(quarterSeq),
      longs(n)(weekSeq),
      strs(n)(i => dayNames(dow(i))),
      strs(n)(i => s"${year(i)}Q${qoy(i)}"),
      flags(n)(holiday),
      flags(n)(i => dow(i) == 5 || dow(i) == 6),
      flags(n)(followsHoliday),
      LongVec(PType.Int64, firstDom, null),
      longs(n)(i => firstDom(i) + daysInMonth(year(i), month(i)) - 1),
      longs(n)(i => toSk(yearEarlier(year(i), month(i), day(i)))),
      longs(n)(i => sk(i) - previousQuarterDays(year(i), (month(i) - 1) / 3 + 1)),
      flags(n)(_ => false),
      flags(n)(_ => false),
      flags(n)(i => year(i) == 2003 && month(i) == 1),
      // dsdgen's current quarter runs to 2003-04-01
      flags(n)(i => year(i) == 2003 && (month(i) <= 3 || (month(i) == 4 && day(i) == 1))),
      flags(n)(year(_) == 2003)
    )

  end dateDim

  private def timeDim(start: Int, n: Int): Batch =
    def hour(i: Int): Int = (start + i) / 3600
    val meal              =
      Array.tabulate(n) { i =>
        hour(i) match
          case h if h >= 6 && h <= 9 =>
            "breakfast"
          case h if h >= 12 && h <= 14 =>
            "lunch"
          case h if h >= 17 && h <= 19 =>
            "dinner"
          case _ =>
            null
      }
    batch(
      n,
      longs(n)(i => (start + i).toLong),
      strs(n)(i => businessKey(start + i + 1L)),
      longs(n)(i => (start + i).toLong),
      longs(n)(hour(_).toLong),
      longs(n)(i => (start + i) / 60 % 60L),
      longs(n)(i => (start + i) % 60L),
      strs(n)(i =>
        if hour(i) < 12 then
          "AM"
        else
          "PM"
      ),
      strs(n)(i =>
        hour(i) match
          case h if h >= 7 && h <= 14 =>
            "first"
          case h if h >= 15 && h <= 22 =>
            "second"
          case _ =>
            "third"
      ),
      strs(n)(i =>
        hour(i) match
          case h if h <= 5 =>
            "night"
          case h if h <= 11 =>
            "morning"
          case h if h <= 16 =>
            "afternoon"
          case _ =>
            "evening"
      ),
      StrVec(meal, meal.map(_ == null))
    )

  end timeDim

  // Row k is the k-th combination of the domains, the gender varying fastest
  private def customerDemographics(start: Int, n: Int): Batch =
    def k(i: Int): Int = start + i
    batch(
      n,
      longs(n)(k(_) + 1L),
      strs(n)(i => genders(k(i) % 2)),
      strs(n)(i => maritalStatus(k(i) / 2 % 5)),
      strs(n)(i => educationStatus(k(i) / 10 % 7)),
      longs(n)(i => (k(i) / 70 % 20) * 500L + 500),
      strs(n)(i => creditRatings(k(i) / 1400 % 4)),
      longs(n)(i => k(i) / 5600 % 7L),
      longs(n)(i => k(i) / 39200 % 7L),
      ints(n)(i => k(i) / 274400 % 7)
    )

  // Key sk combines income band (fastest, starting from band 2), buy potential, dependents, and
  // vehicles
  private def householdDemographics(start: Int, n: Int): Batch =
    def sk(i: Int): Int = start + i + 1
    batch(
      n,
      longs(n)(sk(_).toLong),
      longs(n)(i => sk(i) % 20 + 1L),
      strs(n)(i => buyPotentials(sk(i) / 20 % 6)),
      longs(n)(i => sk(i) / 120 % 10L),
      ints(n)(i => vehicleCounts(sk(i) / 1200 % 6))
    )

  private def incomeBand(start: Int, n: Int): Batch =
    def sk(i: Int): Int = start + i + 1
    batch(
      n,
      longs(n)(sk(_).toLong),
      longs(n)(i =>
        if sk(i) == 1 then
          0L
        else
          (sk(i) - 1) * 10000L + 1
      ),
      ints(n)(sk(_) * 10000)
    )

end TpcdsGen

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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * The TPC-DS dimension tables generated from random draws over value lists: `item`, `customer`,
  * `customer_address`, `store`, `warehouse`, `call_center`, `web_site`, `web_page`, `promotion`,
  * `catalog_page`, `reason`, and `ship_mode`.
  *
  * Row counts, key ranges, column types, null shares, and the slowly changing dimension layout
  * (business ids with 1 to 3 revisions and their `rec_start_date` / `rec_end_date`) follow DuckDB's
  * dsdgen. The values don't: the value lists are the literals the TPC-DS queries filter on (item
  * categories and their classes, states, counties, cities, carriers, the digit syllables of store
  * and company names, ...) plus generic filler, not dsdgen's distributions.
  */
private[catalog] object TpcdsDimensions:

  /** The generated tables, in dependency order */
  val tableNames: Seq[String] = Seq(
    "reason",
    "ship_mode",
    "warehouse",
    "call_center",
    "web_site",
    "web_page",
    "store",
    "promotion",
    "catalog_page",
    "customer_address",
    "customer",
    "item"
  )

  private val I64 = PType.Int64
  private val I32 = PType.Int32
  private val Str = PType.Str
  private val Dt  = PType.Date
  private val D72 = PType.decimal(7, 2)
  private val D52 = PType.decimal(5, 2)
  private val D15 = PType.decimal(15, 2)

  private def address(prefix: String): Seq[(String, PType)] = Seq(
    s"${prefix}street_number" -> Str,
    s"${prefix}street_name"   -> Str,
    s"${prefix}street_type"   -> Str,
    s"${prefix}suite_number"  -> Str,
    s"${prefix}city"          -> Str,
    s"${prefix}county"        -> Str,
    s"${prefix}state"         -> Str,
    s"${prefix}zip"           -> Str,
    s"${prefix}country"       -> Str,
    s"${prefix}gmt_offset"    -> D52
  )

  private def schema(fields: Seq[(String, PType)]): Schema = GenColumns.schema(fields*)

  /** The columns of each table, as dsdgen writes them */
  val schemas: Map[String, Schema] = Map(
    "item" ->
      schema(
        Seq(
          "i_item_sk"        -> I64,
          "i_item_id"        -> Str,
          "i_rec_start_date" -> Dt,
          "i_rec_end_date"   -> Dt,
          "i_item_desc"      -> Str,
          "i_current_price"  -> D72,
          "i_wholesale_cost" -> D72,
          "i_brand_id"       -> I64,
          "i_brand"          -> Str,
          "i_class_id"       -> I64,
          "i_class"          -> Str,
          "i_category_id"    -> I64,
          "i_category"       -> Str,
          "i_manufact_id"    -> I64,
          "i_manufact"       -> Str,
          "i_size"           -> Str,
          "i_formulation"    -> Str,
          "i_color"          -> Str,
          "i_units"          -> Str,
          "i_container"      -> Str,
          "i_manager_id"     -> I64,
          "i_product_name"   -> Str
        )
      ),
    "customer" ->
      schema(
        Seq(
          "c_customer_sk"          -> I64,
          "c_customer_id"          -> Str,
          "c_current_cdemo_sk"     -> I64,
          "c_current_hdemo_sk"     -> I64,
          "c_current_addr_sk"      -> I64,
          "c_first_shipto_date_sk" -> I64,
          "c_first_sales_date_sk"  -> I64,
          "c_salutation"           -> Str,
          "c_first_name"           -> Str,
          "c_last_name"            -> Str,
          "c_preferred_cust_flag"  -> Str,
          "c_birth_day"            -> I64,
          "c_birth_month"          -> I64,
          "c_birth_year"           -> I64,
          "c_birth_country"        -> Str,
          "c_login"                -> Str,
          "c_email_address"        -> Str,
          "c_last_review_date_sk"  -> I32
        )
      ),
    "customer_address" ->
      schema(
        Seq("ca_address_sk" -> I64, "ca_address_id" -> Str) ++ address("ca_") ++
          Seq("ca_location_type" -> Str)
      ),
    "store" ->
      schema(
        Seq(
          "s_store_sk"         -> I64,
          "s_store_id"         -> Str,
          "s_rec_start_date"   -> Dt,
          "s_rec_end_date"     -> Dt,
          "s_closed_date_sk"   -> I64,
          "s_store_name"       -> Str,
          "s_number_employees" -> I64,
          "s_floor_space"      -> I64,
          "s_hours"            -> Str,
          "s_manager"          -> Str,
          "s_market_id"        -> I64,
          "s_geography_class"  -> Str,
          "s_market_desc"      -> Str,
          "s_market_manager"   -> Str,
          "s_division_id"      -> I64,
          "s_division_name"    -> Str,
          "s_company_id"       -> I64,
          "s_company_name"     -> Str
        ) ++ address("s_") ++ Seq("s_tax_percentage" -> D52)
      ),
    "warehouse" ->
      schema(
        Seq(
          "w_warehouse_sk"    -> I64,
          "w_warehouse_id"    -> Str,
          "w_warehouse_name"  -> Str,
          "w_warehouse_sq_ft" -> I64
        ) ++ address("w_")
      ),
    "call_center" ->
      schema(
        Seq(
          "cc_call_center_sk" -> I64,
          "cc_call_center_id" -> Str,
          "cc_rec_start_date" -> Dt,
          "cc_rec_end_date"   -> Dt,
          "cc_closed_date_sk" -> I64,
          "cc_open_date_sk"   -> I64,
          "cc_name"           -> Str,
          "cc_class"          -> Str,
          "cc_employees"      -> I64,
          "cc_sq_ft"          -> I64,
          "cc_hours"          -> Str,
          "cc_manager"        -> Str,
          "cc_mkt_id"         -> I64,
          "cc_mkt_class"      -> Str,
          "cc_mkt_desc"       -> Str,
          "cc_market_manager" -> Str,
          "cc_division"       -> I64,
          "cc_division_name"  -> Str,
          "cc_company"        -> I64,
          "cc_company_name"   -> Str
        ) ++ address("cc_") ++ Seq("cc_tax_percentage" -> D52)
      ),
    "web_site" ->
      schema(
        Seq(
          "web_site_sk"        -> I64,
          "web_site_id"        -> Str,
          "web_rec_start_date" -> Dt,
          "web_rec_end_date"   -> Dt,
          "web_name"           -> Str,
          "web_open_date_sk"   -> I64,
          "web_close_date_sk"  -> I64,
          "web_class"          -> Str,
          "web_manager"        -> Str,
          "web_mkt_id"         -> I64,
          "web_mkt_class"      -> Str,
          "web_mkt_desc"       -> Str,
          "web_market_manager" -> Str,
          "web_company_id"     -> I64,
          "web_company_name"   -> Str
        ) ++ address("web_") ++ Seq("web_tax_percentage" -> D52)
      ),
    "web_page" ->
      schema(
        Seq(
          "wp_web_page_sk"      -> I64,
          "wp_web_page_id"      -> Str,
          "wp_rec_start_date"   -> Dt,
          "wp_rec_end_date"     -> Dt,
          "wp_creation_date_sk" -> I64,
          "wp_access_date_sk"   -> I64,
          "wp_autogen_flag"     -> Str,
          "wp_customer_sk"      -> I64,
          "wp_url"              -> Str,
          "wp_type"             -> Str,
          "wp_char_count"       -> I64,
          "wp_link_count"       -> I64,
          "wp_image_count"      -> I64,
          "wp_max_ad_count"     -> I32
        )
      ),
    "promotion" ->
      schema(
        Seq(
          "p_promo_sk"        -> I64,
          "p_promo_id"        -> Str,
          "p_start_date_sk"   -> I64,
          "p_end_date_sk"     -> I64,
          "p_item_sk"         -> I64,
          "p_cost"            -> D15,
          "p_response_target" -> I64,
          "p_promo_name"      -> Str,
          "p_channel_dmail"   -> Str,
          "p_channel_email"   -> Str,
          "p_channel_catalog" -> Str,
          "p_channel_tv"      -> Str,
          "p_channel_radio"   -> Str,
          "p_channel_press"   -> Str,
          "p_channel_event"   -> Str,
          "p_channel_demo"    -> Str,
          "p_channel_details" -> Str,
          "p_purpose"         -> Str,
          "p_discount_active" -> Str
        )
      ),
    "catalog_page" ->
      schema(
        Seq(
          "cp_catalog_page_sk"     -> I64,
          "cp_catalog_page_id"     -> Str,
          "cp_start_date_sk"       -> I64,
          "cp_end_date_sk"         -> I64,
          "cp_department"          -> Str,
          "cp_catalog_number"      -> I64,
          "cp_catalog_page_number" -> I64,
          "cp_description"         -> Str,
          "cp_type"                -> Str
        )
      ),
    "reason"    -> schema(Seq("r_reason_sk" -> I64, "r_reason_id" -> Str, "r_reason_desc" -> Str)),
    "ship_mode" ->
      schema(
        Seq(
          "sm_ship_mode_sk" -> I64,
          "sm_ship_mode_id" -> Str,
          "sm_type"         -> Str,
          "sm_code"         -> Str,
          "sm_carrier"      -> Str,
          "sm_contract"     -> Str
        )
      )
  )

  /**
    * Row counts at scale factor 1 and 10 (dsdgen's), the rounding unit between them, and whether
    * the count stays fixed from scale factor 1
    */
  private case class Scaling(at1: Int, at10: Int, unit: Int = 1, fixed: Boolean = false)

  private val scaling: Map[String, Scaling] = Map(
    "item"             -> Scaling(18000, 102000, unit = 2000),
    "customer"         -> Scaling(100000, 500000, unit = 1000),
    "customer_address" -> Scaling(50000, 250000, unit = 1000),
    "store"            -> Scaling(12, 102),
    "warehouse"        -> Scaling(5, 10),
    "call_center"      -> Scaling(6, 24),
    "web_site"         -> Scaling(30, 42, unit = 2),
    "web_page"         -> Scaling(60, 200, unit = 2),
    "promotion"        -> Scaling(300, 500),
    "catalog_page"     -> Scaling(11718, 11718, fixed = true),
    "reason"           -> Scaling(75, 75, fixed = true),
    "ship_mode"        -> Scaling(20, 20, fixed = true)
  )

  /**
    * The rows of `table` at scale factor `sf`, as dsdgen counts them: a share of the scale factor 1
    * count below it (at least 1 row), and linear between the scale factor 1 and 10 counts above it,
    * rounded down to the table's unit
    */
  def rows(table: String, sf: Double): Int =
    val s = scaling(table)
    if table == "ship_mode" || table == "catalog_page" then
      s.at1
    else if sf < 1 then
      math.max(1, math.floor(s.at1 * sf + 1e-9).toInt)
    else if s.fixed then
      s.at1
    else
      val v = s.at1 + (s.at10 - s.at1) * (sf - 1) / 9
      math.max(s.at1, (math.floor(v / s.unit + 1e-9) * s.unit).toInt)

  /** The share of NULLs in the non-key columns of each table, as in dsdgen's output */
  val nullShare: Map[String, Double] = Map(
    "item"             -> 0.0025,
    "customer"         -> 0.0345,
    "customer_address" -> 0.03,
    "promotion"        -> 0.02,
    "catalog_page"     -> 0.009
  )

  // Value lists: the literals of the TPC-DS queries, and generic filler

  /**
    * Digit syllables: dsdgen builds names (store, company, manufacturer, ...) from a number's
    * digits
    */
  val syllables = IndexedSeq(
    "bar",
    "ought",
    "able",
    "pri",
    "ese",
    "anti",
    "cally",
    "ation",
    "eing",
    "n st"
  )

  /** A name made of the syllables of n's digits, as `pri` for 3 or `ableought` for 21 */
  def syllableName(n: Long): String = n.toString.map(c => syllables(c - '0')).mkString

  /** Item categories (by i_category_id - 1) with the classes the queries pair them with first */
  val categories: IndexedSeq[(String, IndexedSeq[String])] =
    def classes(cat: String, named: Seq[String], total: Int): IndexedSeq[String] =
      (named ++ (named.size + 1 to total).map(i => s"${cat.toLowerCase} ${i}")).toIndexedSeq
    IndexedSeq(
      "Women"       -> classes("Women", Seq("dresses", "fragrances", "maternity"), 4),
      "Men"         -> classes("Men", Seq("accessories", "pants", "shirts"), 4),
      "Children"    -> classes("Children", Nil, 4),
      "Shoes"       -> classes("Shoes", Nil, 4),
      "Music"       -> classes("Music", Seq("classical"), 4),
      "Jewelry"     -> classes("Jewelry", Seq("birdal"), 16),
      "Home"        -> classes("Home", Nil, 16),
      "Sports"      -> classes("Sports", Seq("football"), 16),
      "Books"       -> classes("Books", Seq("computers", "reference", "self-help"), 16),
      "Electronics" -> classes("Electronics", Seq("personal", "portable", "stereo"), 16)
    )

  val sizes = IndexedSeq("petite", "small", "medium", "large", "extra large", "economy", "N/A")

  val units = IndexedSeq(
    "Each",
    "Oz",
    "Ounce",
    "Lb",
    "Pound",
    "Dram",
    "Tbl",
    "Cup",
    "Dozen",
    "Gross",
    "Bunch",
    "Bundle",
    "Box",
    "Pallet",
    "Ton",
    "N/A",
    "Carton",
    "Case",
    "Gram",
    "Unknown"
  )

  // The query literals first; the rest from the TPC-H color words
  val colors: IndexedSeq[String] =
    val named = IndexedSeq(
      "blanched",
      "brown",
      "burlywood",
      "burnished",
      "cornflower",
      "cyan",
      "deep",
      "floral",
      "forest",
      "frosted",
      "ghost",
      "honeydew",
      "indian",
      "khaki",
      "light",
      "medium",
      "midnight",
      "orange",
      "papaya",
      "peach",
      "powder",
      "purple",
      "slate",
      "snow",
      "spring"
    )
    named ++ TpchGen.colorNames.filterNot(named.contains).take(88 - named.size)

  /** (state, GMT offset) */
  val states: IndexedSeq[(String, Int)] = IndexedSeq(
    "AL" -> -6,
    "AK" -> -9,
    "AZ" -> -7,
    "AR" -> -6,
    "CA" -> -8,
    "CO" -> -7,
    "CT" -> -5,
    "DE" -> -5,
    "DC" -> -5,
    "FL" -> -5,
    "GA" -> -5,
    "HI" -> -10,
    "ID" -> -7,
    "IL" -> -6,
    "IN" -> -5,
    "IA" -> -6,
    "KS" -> -6,
    "KY" -> -5,
    "LA" -> -6,
    "ME" -> -5,
    "MD" -> -5,
    "MA" -> -5,
    "MI" -> -5,
    "MN" -> -6,
    "MS" -> -6,
    "MO" -> -6,
    "MT" -> -7,
    "NE" -> -6,
    "NV" -> -8,
    "NH" -> -5,
    "NJ" -> -5,
    "NM" -> -7,
    "NY" -> -5,
    "NC" -> -5,
    "ND" -> -6,
    "OH" -> -5,
    "OK" -> -6,
    "OR" -> -8,
    "PA" -> -5,
    "RI" -> -5,
    "SC" -> -5,
    "SD" -> -6,
    "TN" -> -6,
    "TX" -> -6,
    "UT" -> -7,
    "VT" -> -5,
    "VA" -> -5,
    "WA" -> -8,
    "WV" -> -5,
    "WI" -> -6,
    "WY" -> -7
  )

  val cities = IndexedSeq(
    "Fairview",
    "Midway",
    "Edgewood",
    "Oak Grove",
    "Five Points",
    "Riverside",
    "Pleasant Hill",
    "Centerville",
    "Mount Pleasant",
    "Georgetown",
    "Salem",
    "Greenwood",
    "Franklin",
    "Clinton",
    "Springfield",
    "Oakland",
    "Union",
    "Liberty",
    "Lakeside",
    "Glendale",
    "Marion",
    "Ashland",
    "Bethel",
    "Clifton",
    "Concord",
    "Friendship",
    "Greenville",
    "Hamilton",
    "Harmony",
    "Highland",
    "Jackson",
    "Kingston",
    "Lebanon",
    "Lincoln",
    "Madison",
    "Milford",
    "Newport",
    "Oakdale",
    "Pine Grove",
    "Plainview",
    "Riverview",
    "Shady Grove",
    "Shiloh",
    "Spring Hill",
    "Summit",
    "Sunnyside",
    "Unionville",
    "Walnut Grove",
    "Waterloo",
    "Woodland"
  )

  // Counties the queries name, then generic ones
  val counties: IndexedSeq[String] =
    IndexedSeq("Williamson County", "Bronx County", "Orange County", "Franklin Parish") ++
      cities.drop(3).map(c => s"${c} County")

  // The zip codes TPC-DS q15 and q45 look for, among others
  val queryZips = IndexedSeq(
    "85669",
    "86197",
    "88274",
    "83405",
    "86475",
    "85392",
    "85460",
    "80348",
    "81792"
  )

  val streetNames = IndexedSeq(
    "1st",
    "2nd",
    "3rd",
    "4th",
    "5th",
    "6th",
    "7th",
    "8th",
    "9th",
    "10th",
    "11th",
    "12th",
    "Main",
    "Oak",
    "Pine",
    "Maple",
    "Cedar",
    "Elm",
    "Park",
    "Lake",
    "Hill",
    "View",
    "Washington",
    "Ridge",
    "Forest",
    "Spring",
    "Sunset",
    "Valley",
    "Church",
    "Center",
    "Mill",
    "River",
    "Walnut",
    "Willow",
    "Hickory",
    "Chestnut",
    "Sycamore",
    "Birch",
    "Ash",
    "Meadow",
    "Green",
    "Woodland"
  )

  val streetTypes = IndexedSeq(
    "Ave",
    "Avenue",
    "Blvd",
    "Boulevard",
    "Cir.",
    "Circle",
    "Court",
    "Ct.",
    "Dr.",
    "Drive",
    "Ln",
    "Lane",
    "Parkway",
    "Pkwy",
    "RD",
    "Road",
    "ST",
    "Street",
    "Way",
    "Wy"
  )

  val countries = IndexedSeq(
    "UNITED STATES",
    "CANADA",
    "MEXICO",
    "BRAZIL",
    "ARGENTINA",
    "CHILE",
    "PERU",
    "COLOMBIA",
    "UNITED KINGDOM",
    "IRELAND",
    "FRANCE",
    "GERMANY",
    "ITALY",
    "SPAIN",
    "PORTUGAL",
    "NETHERLANDS",
    "BELGIUM",
    "SWITZERLAND",
    "AUSTRIA",
    "POLAND",
    "SWEDEN",
    "NORWAY",
    "DENMARK",
    "FINLAND",
    "GREECE",
    "TURKEY",
    "EGYPT",
    "MOROCCO",
    "NIGERIA",
    "KENYA",
    "SOUTH AFRICA",
    "INDIA",
    "CHINA",
    "JAPAN",
    "KOREA",
    "VIET NAM",
    "THAILAND",
    "INDONESIA",
    "PHILIPPINES",
    "AUSTRALIA",
    "NEW ZEALAND"
  )

  val salutations = IndexedSeq("Mr.", "Mrs.", "Ms.", "Miss", "Dr.", "Sir")

  // Name syllables: first and last names are two or three of them, capitalized
  private val nameSyllables = IndexedSeq(
    "ka",
    "ra",
    "li",
    "mo",
    "ne",
    "sa",
    "to",
    "vi",
    "da",
    "el",
    "an",
    "jo",
    "ma",
    "ri",
    "na",
    "le",
    "ro",
    "ta",
    "mi",
    "sha",
    "ben",
    "car",
    "der",
    "son",
    "ton",
    "ley",
    "wood",
    "ford",
    "man",
    "ber",
    "lin",
    "ger",
    "ham",
    "well",
    "field",
    "ston"
  )

  def personName(n: Int): String =
    val k  = nameSyllables.size
    val sb = StringBuilder(nameSyllables(n % k))
    sb.append(nameSyllables(n / k % k))
    if n / (k * k) % 3 == 0 then
      sb.append(nameSyllables(n / (k * k * 3) % k))
    sb.toString.capitalize

  private val words = IndexedSeq(
    "able",
    "acres",
    "bad",
    "big",
    "blue",
    "boys",
    "cards",
    "certain",
    "children",
    "clear",
    "close",
    "common",
    "dark",
    "days",
    "different",
    "early",
    "easy",
    "eyes",
    "fine",
    "free",
    "full",
    "good",
    "great",
    "hands",
    "hard",
    "high",
    "important",
    "issues",
    "large",
    "late",
    "levels",
    "little",
    "local",
    "long",
    "major",
    "men",
    "minutes",
    "national",
    "new",
    "old",
    "open",
    "parents",
    "particular",
    "people",
    "political",
    "possible",
    "present",
    "public",
    "real",
    "recent",
    "right",
    "small",
    "social",
    "special",
    "strong",
    "sure",
    "things",
    "true",
    "white",
    "whole",
    "women",
    "years",
    "young"
  )

  private val verbs = IndexedSeq(
    "make",
    "see",
    "come",
    "take",
    "know",
    "find",
    "give",
    "tell",
    "work",
    "call",
    "try",
    "feel",
    "leave",
    "put",
    "keep",
    "let",
    "begin",
    "seem",
    "help",
    "show",
    "hear",
    "play",
    "run",
    "move",
    "live",
    "hold",
    "bring",
    "write",
    "stand",
    "meet"
  )

  /** Sentences of random words, cut to a length in [minLen, maxLen] */
  def text(r: Random, minLen: Int, maxLen: Int): String =
    val target = minLen + r.nextInt(maxLen - minLen + 1)
    val sb     = StringBuilder()
    while sb.length < target do
      val sentence =
        s"${words(r.nextInt(words.size))} ${words(r.nextInt(words.size))} " +
          s"${verbs(r.nextInt(verbs.size))} ${words(r.nextInt(words.size))}."
      if sb.nonEmpty then
        sb.append(' ')
      sb.append(sentence.capitalize)
    sb.toString.take(target).trim

end TpcdsDimensions

/**
  * Columns of a block, each with dsdgen's share of NULLs unless it's a key. Values draw from `r` in
  * column order, so a block is the same for the same seed
  */
private final class DimColumns(n: Int, r: Random, nullShare: Double):
  private val out = ArrayBuffer.empty[Vec]

  // A value of Long.MinValue (or a null string) is NULL too
  private def nulls(nullable: Boolean, isNull: Int => Boolean): Array[Boolean] =
    val a =
      Array.tabulate(n)(i => isNull(i) || (nullable && nullShare > 0 && r.nextDouble() < nullShare))
    if a.contains(true) then
      a
    else
      null

  private def longsOf(t: PType, f: Int => Long, nullable: Boolean): Unit =
    val v = Array.tabulate(n)(f)
    out += LongVec(t, v, nulls(nullable, v(_) == Long.MinValue))

  def long(f: Int => Long, nullable: Boolean = true): Unit = longsOf(PType.Int64, f, nullable)

  def decimal(t: PType, f: Int => Long, nullable: Boolean = true): Unit = longsOf(t, f, nullable)

  def int(f: Int => Int, nullable: Boolean = true): Unit =
    val v = Array.tabulate(n)(f)
    out += IntVec(PType.Int32, v, nulls(nullable, v(_) == Int.MinValue))

  def date(f: Int => Int, nullable: Boolean = true): Unit =
    val v = Array.tabulate(n)(f)
    out += IntVec(PType.Date, v, nulls(nullable, v(_) == Int.MinValue))

  def str(f: Int => String, nullable: Boolean = true): Unit =
    val v = Array.tabulate(n)(f)
    val m = nulls(nullable, v(_) == null)
    if m != null then
      var i = 0
      while i < n do
        if m(i) then
          v(i) = null
        i += 1
    out += StrVec(v, m)

  def batch: Batch = Batch(out.toArray, n)

end DimColumns

/**
  * The dimension tables at a scale factor. `numCustomerDemographics` bounds the customers'
  * demographics keys
  */
private[catalog] class TpcdsDimensionGen(sf: Double, numCustomerDemographics: Int):
  import TpcdsDimensions.*
  import TpcdsGen.{FirstDateSk, FirstEpochDay, businessKey}

  private val counts: Map[String, Int] = tableNames.map(t => t -> rows(t, sf)).toMap

  def rowsOf(table: String): Int = counts(table)

  private def dateSk(epochDay: Int): Long = epochDay.toLong - FirstEpochDay + FirstDateSk

  private def epoch(y: Int, m: Int, d: Int): Int = ValueOps.daysFromCivil(y, m, d)

  private def between(r: Random, lo: Int, hi: Int): Int = lo + r.nextInt(hi - lo + 1)

  private def betweenLong(r: Random, lo: Long, hi: Long): Long =
    lo + (r.nextDouble() * (hi - lo + 1)).toLong.min(hi - lo)

  private def pick[A](r: Random, xs: IndexedSeq[A]): A = xs(r.nextInt(xs.size))

  // Slowly changing dimensions: every 6 rows hold a business id with 1 revision, one with 2, and
  // one with 3, so half the rows are current (rec_end_date NULL), as in dsdgen
  private case class Revision(id: Long, start: Int, end: Int)

  private def revision(k: Int, base: (Int, Int, Int)): Revision =
    val (y, m, d) = base
    val group     = k / 6
    val p         = k % 6
    val s0        = epoch(y, m, d)
    val s1        = epoch(y + 2, m, d) + 1
    val s2        = epoch(y + 3, m, d)
    val s3        = epoch(y + 4, m, d)
    val none      = Int.MinValue
    p match
      case 0 =>
        Revision(group * 3L + 1, s0, none)
      case 1 =>
        Revision(group * 3L + 2, s0, s2 - 1)
      case 2 =>
        Revision(group * 3L + 2, s2, none)
      case 3 =>
        Revision(group * 3L + 3, s0, s1 - 1)
      case 4 =>
        Revision(group * 3L + 3, s1, s3 - 1)
      case _ =>
        Revision(group * 3L + 3, s3, none)

  private def addressColumns(
      c: DimColumns,
      r: Random,
      n: Int,
      city: Int => String,
      county: Int => String,
      state: Int => String,
      zip: Int => String,
      gmt: Int => Int
  ): Unit =
    c.str(_ => between(r, 1, 999).toString)
    c.str(_ =>
      if r.nextBoolean() then
        s"${pick(r, streetNames)} ${pick(r, streetNames)}"
      else
        pick(r, streetNames)
    )
    c.str(_ => pick(r, streetTypes))
    c.str(_ =>
      if r.nextBoolean() then
        s"Suite ${between(r, 0, 49) * 10}"
      else
        s"Suite ${('A' + r.nextInt(26)).toChar}"
    )
    c.str(city)
    c.str(county)
    c.str(state)
    c.str(zip)
    c.str(_ => "United States")
    c.decimal(PType.decimal(5, 2), gmt(_) * 100L)

  end addressColumns

  // Stores, warehouses, call centers, and web sites are in Williamson County, TN, as in dsdgen
  private def localAddress(c: DimColumns, r: Random, n: Int, cityOf: Int => String): Unit =
    addressColumns(
      c,
      r,
      n,
      cityOf,
      _ => "Williamson County",
      _ => "TN",
      i =>
        if cityOf(i) == "Midway" then
          "31904"
        else
          "35709"
      ,
      _ => -5
    )

  private def twoCities(r: Random): String =
    if r.nextBoolean() then
      "Fairview"
    else
      "Midway"

  /** Generate the rows [start, start + n) of `table` */
  def block(table: String, start: Int, n: Int, r: Random): Batch =
    val c                = DimColumns(n, r, nullShare.getOrElse(table, 0.0))
    def sk(i: Int): Long = start + i + 1L
    table match
      case "reason" =>
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.str(i => s"reason ${sk(i)}", nullable = false)
      case "ship_mode" =>
        val types = IndexedSeq("EXPRESS", "NEXT DAY", "OVERNIGHT", "TWO DAY", "LIBRARY", "REGULAR")
        val codes = IndexedSeq("AIR", "SURFACE", "SEA", "BIKE")
        val carrier = IndexedSeq("UPS", "FEDEX", "USPS", "DHL", "BARIAN")
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.str(i => types((start + i) % types.size), nullable = false)
        c.str(i => codes(math.min((start + i) * codes.size / 20, codes.size - 1)), nullable = false)
        c.str(
          i =>
            if start + i < carrier.size then
              carrier(start + i)
            else
              s"CARRIER ${start + i + 1}"
          ,
          nullable = false
        )
        c.str(_ => (0 until between(r, 2, 20)).map(_ => ('a' + r.nextInt(26)).toChar).mkString)
      case "warehouse" =>
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.str(_ => text(r, 10, 20))
        c.long(_ => betweenLong(r, 50000, 1000000))
        val city = Array.fill(n)(twoCities(r))
        localAddress(c, r, n, city(_))
      case "call_center" =>
        val names = IndexedSeq(
          "NY Metro",
          "Mid Atlantic",
          "North Midwest",
          "California",
          "Pacific Northwest",
          "South"
        )
        val rev = Array.tabulate(n)(i => revision(start + i, (1998, 1, 1)))
        c.long(sk, nullable = false)
        c.str(i => businessKey(rev(i).id), nullable = false)
        c.date(rev(_).start, nullable = false)
        c.date(rev(_).end, nullable = false)
        c.long(_ => Long.MinValue, nullable = false)
        c.long(_ => dateSk(epoch(1998, 1, 1)) + between(r, 0, 300))
        c.str(i => names(((rev(i).id - 1) % names.size).toInt))
        c.str(_ => pick(r, IndexedSeq("small", "medium", "large")))
        c.long(_ => between(r, 1, 7).toLong)
        c.long(_ => between(r, 600, 4200).toLong)
        c.str(_ => pick(r, IndexedSeq("8AM-4PM", "8AM-8AM")))
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        c.long(_ => between(r, 1, 6).toLong)
        c.str(_ => text(r, 20, 50))
        c.str(_ => text(r, 20, 100))
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        val division = Array.fill(n)(between(r, 1, 6))
        c.long(division(_).toLong)
        c.str(i => syllableName(division(i)))
        val company = Array.fill(n)(between(r, 1, 6))
        c.long(company(_).toLong)
        c.str(i => syllableName(company(i)))
        localAddress(c, r, n, _ => "Midway")
        c.decimal(PType.decimal(5, 2), _ => between(r, 0, 12).toLong)
      case "web_site" =>
        val rev = Array.tabulate(n)(i => revision(start + i, (1997, 8, 16)))
        c.long(sk, nullable = false)
        c.str(i => businessKey(rev(i).id), nullable = false)
        c.date(rev(_).start, nullable = false)
        c.date(rev(_).end, nullable = false)
        c.str(i => s"site_${(rev(i).id - 1) / 3}")
        c.long(_ => dateSk(epoch(1997, 8, 16)) + between(r, 0, 230))
        c.long(_ =>
          if r.nextInt(6) == 0 then
            Long.MinValue
          else
            dateSk(epoch(1987, 1, 1)) + between(r, 0, 2000)
        )
        c.str(_ => "Unknown")
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        c.long(_ => between(r, 1, 6).toLong)
        c.str(_ => text(r, 20, 50))
        c.str(_ => text(r, 20, 100))
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        val company = Array.fill(n)(between(r, 1, 6))
        c.long(company(_).toLong)
        c.str(i => syllableName(company(i)))
        val city = Array.fill(n)(twoCities(r))
        localAddress(c, r, n, city(_))
        c.decimal(PType.decimal(5, 2), _ => between(r, 0, 12).toLong)
      case "web_page" =>
        val types = IndexedSeq(
          "ad",
          "dynamic",
          "feedback",
          "general",
          "order",
          "protected",
          "welcome"
        )
        val rev = Array.tabulate(n)(i => revision(start + i, (1997, 9, 3)))
        c.long(sk, nullable = false)
        c.str(i => businessKey(rev(i).id), nullable = false)
        c.date(rev(_).start, nullable = false)
        c.date(rev(_).end, nullable = false)
        c.long(_ => dateSk(epoch(1997, 12, 31)) + between(r, 0, 8))
        c.long(_ => dateSk(epoch(2002, 9, 30)) + between(r, 0, 100))
        val autogen = Array.fill(n)(r.nextBoolean())
        c.str(i =>
          if autogen(i) then
            "Y"
          else
            "N"
        )
        // Pages generated for a customer
        c.long(i =>
          if r.nextInt(100) < 35 then
            betweenLong(r, 1, rowsOf("customer"))
          else
            Long.MinValue
        )
        c.str(_ => "http://www.foo.com")
        c.str(_ => pick(r, types))
        c.long(_ => between(r, 700, 7000).toLong)
        c.long(_ => between(r, 2, 25).toLong)
        c.long(_ => between(r, 1, 7).toLong)
        c.int(_ => between(r, 0, 4))
      case "store" =>
        val rev = Array.tabulate(n)(i => revision(start + i, (1997, 3, 13)))
        c.long(sk, nullable = false)
        c.str(i => businessKey(rev(i).id), nullable = false)
        c.date(rev(_).start, nullable = false)
        c.date(rev(_).end, nullable = false)
        c.long(_ =>
          if r.nextInt(4) == 0 then
            dateSk(epoch(1998, 3, 1)) + between(r, 0, 300)
          else
            Long.MinValue
        )
        // Store names are the syllable of the business id's last digit: ought, able, pri, ese, ...
        c.str(i => syllables((rev(i).id % 10).toInt))
        c.long(_ => between(r, 200, 300).toLong)
        c.long(_ => between(r, 5000000, 10000000).toLong)
        c.str(_ => pick(r, IndexedSeq("8AM-4PM", "8AM-12AM", "8AM-8AM")))
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        c.long(_ => between(r, 1, 10).toLong)
        c.str(_ => "Unknown")
        c.str(_ => text(r, 20, 100))
        c.str(_ => s"${personName(r.nextInt(5000))} ${personName(r.nextInt(5000))}")
        c.long(_ => 1L)
        c.str(_ => "Unknown")
        c.long(_ => 1L)
        c.str(_ => "Unknown")
        val city = Array.fill(n)(twoCities(r))
        localAddress(c, r, n, city(_))
        c.decimal(PType.decimal(5, 2), _ => between(r, 0, 11).toLong)
      case "promotion" =>
        val startSk = Array.fill(n)(dateSk(epoch(1995, 12, 31)) + between(r, 0, 813))
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.long(startSk(_))
        c.long(i => startSk(i) + between(r, 1, 60))
        c.long(_ => betweenLong(r, 1, rowsOf("item")))
        c.decimal(PType.decimal(15, 2), _ => 100000L)
        c.long(_ => 1L)
        c.str(i => syllableName(sk(i) % 10))
        c.str(_ =>
          if r.nextBoolean() then
            "Y"
          else
            "N"
        )
        (0 until 7).foreach(_ => c.str(_ => "N"))
        c.str(_ => text(r, 20, 60))
        c.str(_ => "Unknown")
        c.str(_ => "N")
      case "catalog_page" =>
        // Catalogs of 108 pages, issued monthly, quarterly, or twice a year from 1998
        val pages                = 108
        def catalog(i: Int): Int = (start + i) / pages
        val types                = IndexedSeq("bi-annual", "quarterly", "monthly")
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.long(i => dateSk(epoch(1998, 1, 1)) + catalog(i) * 20L)
        c.long(i => dateSk(epoch(1998, 1, 1)) + catalog(i) * 20L + 29 + catalog(i) % 3 * 60)
        c.str(_ => "DEPARTMENT")
        c.long(catalog(_) + 1L)
        c.long(i => (start + i) % pages + 1L)
        c.str(_ => text(r, 30, 100))
        c.str(i => types(catalog(i) % types.size))
      case "customer_address" =>
        val state = Array.fill(n)(r.nextInt(states.size))
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        addressColumns(
          c,
          r,
          n,
          _ => pick(r, cities),
          _ => pick(r, counties),
          i => states(state(i))._1,
          // About 4,000 zip codes, among them those the queries look for
          _ =>
            val k = r.nextInt(4000)
            if k < queryZips.size then
              queryZips(k)
            else
              f"${(k.toLong * 2654435761L % 99381) + 601}%05d"
          ,
          i => states(state(i))._2
        )
        c.str(_ => pick(r, IndexedSeq("apartment", "condo", "single family")))
      case "customer" =>
        val first    = Array.fill(n)(personName(r.nextInt(4000)))
        val last     = Array.fill(n)(personName(4000 + r.nextInt(5000)))
        val firstBuy = Array.fill(n)(dateSk(epoch(1993, 1, 1)) + between(r, 0, 3650))
        c.long(sk, nullable = false)
        c.str(i => businessKey(sk(i)), nullable = false)
        c.long(_ => betweenLong(r, 1, numCustomerDemographics))
        c.long(_ => between(r, 1, 7200).toLong)
        c.long(_ => betweenLong(r, 1, rowsOf("customer_address")), nullable = false)
        c.long(firstBuy(_) + 30)
        c.long(firstBuy(_))
        c.str(_ => pick(r, salutations))
        c.str(first(_))
        c.str(last(_))
        c.str(_ =>
          if r.nextBoolean() then
            "Y"
          else
            "N"
        )
        c.long(_ => between(r, 1, 28).toLong)
        c.long(_ => between(r, 1, 12).toLong)
        c.long(_ => between(r, 1924, 1992).toLong)
        c.str(_ => pick(r, countries))
        c.str(_ => null, nullable = false)
        c.str(i =>
          s"${first(i)}.${last(i)}@${(0 until between(r, 8, 16))
              .map(_ => ('a' + r.nextInt(26)).toChar)
              .mkString}.${pick(r, IndexedSeq("com", "org", "edu"))}"
        )
        c.int(_ => (dateSk(epoch(2002, 1, 1)) + between(r, 0, 365)).toInt)
      case "item" =>
        val rev      = Array.tabulate(n)(i => revision(start + i, (1997, 10, 27)))
        val category = Array.fill(n)(r.nextInt(categories.size))
        val cls      = Array.tabulate(n)(i => r.nextInt(categories(category(i))._2.size))
        val brand    = Array.fill(n)(between(r, 1, 10))
        val manufact = Array.fill(n)(between(r, 1, 1000))
        val price    = Array.fill(n)(betweenLong(r, 9, 9999))
        c.long(sk, nullable = false)
        c.str(i => businessKey(rev(i).id), nullable = false)
        c.date(rev(_).start)
        c.date(rev(_).end, nullable = false)
        c.str(_ => text(r, 1, 200))
        c.decimal(PType.decimal(7, 2), price(_))
        c.decimal(PType.decimal(7, 2), i => math.max(2L, price(i) * between(r, 20, 88) / 100))
        c.long(i => (category(i) + 1) * 1000000L + (cls(i) + 1) * 1000L + brand(i))
        c.str(i => s"${syllableName(category(i) + 1)}${syllableName(cls(i) + 1)} #${brand(i)}")
        c.long(cls(_) + 1L)
        c.str(i => categories(category(i))._2(cls(i)))
        c.long(category(_) + 1L)
        c.str(i => categories(category(i))._1)
        c.long(manufact(_).toLong)
        c.str(i => syllableName(manufact(i)))
        c.str(_ => pick(r, sizes))
        c.str(_ => f"${r.nextLong().abs % 10000000000L}%010d${pick(r, colors)}".take(20))
        c.str(_ => pick(r, colors))
        c.str(_ => pick(r, units))
        c.str(_ => "Unknown")
        c.long(_ => between(r, 1, 100).toLong)
        c.str(i => syllableName(sk(i)))
    end match
    c.batch

  end block

end TpcdsDimensionGen

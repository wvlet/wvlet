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
package wvlet.lang.compiler.lsp

/**
  * A group of stdlib functions by purpose (text, dates, aggregates, ...), the way function
  * references are organized
  *
  * @param id
  *   a stable anchor, e.g. `pattern-matching`
  */
case class StdLibCategory(id: String, title: String, intro: String)

/** A category and its functions, in reference order */
case class StdLibCategoryFunctions(category: StdLibCategory, functions: List[StdLibFunctionDoc])

/**
  * Assigns each stdlib function a category. A function falls in its receiver type's category
  * (`string` → Text) unless its name puts it elsewhere (`regexp_like` → Pattern Matching, `sum` →
  * Aggregate). Every function gets one, so a new stdlib function needs no change here to be listed
  */
object StdLibCategories:
  val Text            = StdLibCategory("text", "Text", "Functions of string values.")
  val PatternMatching = StdLibCategory(
    "pattern-matching",
    "Pattern Matching",
    "Match strings against `like` patterns, prefixes, and regular expressions."
  )

  val Numeric = StdLibCategory(
    "numeric",
    "Numeric",
    "Arithmetic, rounding, and math functions of numeric values (int, long, float, double, decimal)."
  )

  val DateTime = StdLibCategory(
    "date-time",
    "Date and Time",
    "Extract parts of dates and timestamps, shift and truncate them, and format them as text."
  )

  val Conversion = StdLibCategory("conversion", "Type Conversion", "Cast a value to another type.")
  val Comparison = StdLibCategory(
    "comparison",
    "Comparison and Null Handling",
    "Range and set membership tests, and functions that handle null values."
  )

  val Aggregate = StdLibCategory(
    "aggregate",
    "Aggregate",
    "Aggregate the values of a column after `group by`, or of the whole table with `agg`."
  )

  val Statistical = StdLibCategory(
    "statistical-aggregate",
    "Statistical Aggregate",
    "Variance, correlation, and regression of a column's values, used like other aggregates."
  )

  val Window = StdLibCategory(
    "window",
    "Window",
    "Ranking and offset functions, used with an `over(...)` clause."
  )

  val Array = StdLibCategory("array", "Array", "Functions of array values.")
  val Map   = StdLibCategory("map", "Map", "Functions of map values.")
  val Json  = StdLibCategory(
    "json",
    "JSON",
    "Read values from JSON text and convert values to JSON."
  )

  val Encoding = StdLibCategory(
    "encoding",
    "Hashing, Encoding, and IDs",
    "Hash and encode values, and generate unique identifiers."
  )

  val Other = StdLibCategory("other", "Other", "Functions not in the categories above.")

  /** Categories in reference order */
  val all: List[StdLibCategory] = List(
    Text,
    PatternMatching,
    Numeric,
    DateTime,
    Conversion,
    Comparison,
    Aggregate,
    Statistical,
    Window,
    Array,
    Map,
    Json,
    Encoding,
    Other
  )

  private val numericTypes = Set("numeric", "int", "long", "float", "real", "double", "decimal")

  private val patternNames = Set(
    "like",
    "starts_with",
    "ends_with",
    "regexp_like",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_replace",
    "regexp_split"
  )

  private val encodingNames = Set(
    "md5",
    "sha1",
    "sha256",
    "to_base64",
    "from_base64",
    "to_hex",
    "from_hex",
    "url_encode",
    "url_decode",
    "uuid",
    "ulid_string"
  )

  private val dateTimeNames = Set(
    "from_unixtime",
    "try_to_timestamp",
    "within",
    "td_time_string",
    "td_interval",
    "to_unixtime",
    "to_time_zone",
    "from_time_zone"
  )

  // Conversions are `to_<type>` casts; these `to_` names do more than cast
  private val notConversion = Set(
    "to_array",
    "to_json",
    "to_base64",
    "to_hex",
    "to_unixtime",
    "to_time_zone"
  )

  private val comparisonNames = Set(
    "between",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
    "or_else",
    "null_if"
  )

  private val jsonNames = Set("json_extract", "json_extract_string", "to_json")

  private val aggregateNames = Set(
    "count",
    "count_distinct",
    "count_if",
    "count_approx_distinct",
    "arbitrary",
    "any",
    "min",
    "max",
    "min_by",
    "max_by",
    "map_agg",
    "to_array",
    "bool_and",
    "bool_or",
    "string_agg",
    "sum",
    "avg",
    "median",
    "approx_quantile",
    "bit_and",
    "bit_or",
    "bit_xor"
  )

  private val statisticalNames = Set(
    "variance",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "var_pop",
    "var_samp",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "regr_r2",
    "regr_count",
    "kurtosis",
    "skewness"
  )

  private val windowNames = Set("lag", "lead", "first_value", "last_value", "nth_value")

  private val windowFunctions = Set(
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile"
  )

  /** The category of a stdlib function */
  def categoryOf(f: StdLibFunctionDoc): StdLibCategory =
    val n = f.name
    if jsonNames(n) then
      Json
    else if encodingNames(n) then
      Encoding
    else if patternNames(n) then
      PatternMatching
    else if comparisonNames(n) then
      Comparison
    else if dateTimeNames(n) || n.startsWith("current_") then
      DateTime
    else if n == "type_of" || (n.startsWith("to_") && !notConversion(n)) then
      Conversion
    else
      f.ownerType match
        case "" if windowFunctions(n) =>
          Window
        case "" if n == "range" =>
          Array
        case "" if encodingNames(n) =>
          Encoding
        case "" =>
          Other
        case "array" if aggregateNames(n) =>
          Aggregate
        case "array" if statisticalNames(n) =>
          Statistical
        case "array" if windowNames(n) =>
          Window
        case "array" =>
          Array
        case "map" =>
          Map
        case "string" =>
          Text
        case "date" | "timestamp" =>
          DateTime
        case "json" =>
          Json
        case t if numericTypes(t) =>
          Numeric
        case _ =>
          Comparison
    end if

  end categoryOf

end StdLibCategories

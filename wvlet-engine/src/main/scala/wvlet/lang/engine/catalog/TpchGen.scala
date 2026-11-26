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

import scala.util.Random

/**
  * Generates the eight TPC-H tables in memory, on every platform (JVM, Scala.js in the browser,
  * Native), with no DuckDB or data files.
  *
  * Row counts, keys, and value domains follow the TPC-H specification (section 4.2): sparse order
  * keys, 4 suppliers per part, 1 to 7 line items per order, the fixed nations and regions, the
  * retail-price formula, and dates derived from the order date. Text columns are built from a small
  * vocabulary that keeps the words the benchmark queries search for (`green`, `special`,
  * `requests`, `Customer ... Complaints`, ...). The output is deterministic for a scale factor, but
  * the values are not those of the reference `dbgen`, so query answers differ from the official
  * ones.
  *
  * Column types match the Parquet files that DuckDB's `dbgen` writes: keys are `bigint`, money and
  * quantities `decimal(15, 2)`, and dates `date`.
  *
  * Each table is cut into blocks of [[blockRows]] rows (of that many orders with their line items
  * for `orders` and `lineitem`, and of that many parts' rows for `partsupp`), and each block draws
  * from its own seeded random stream. A block generates on its own, so [[TpchSource]] generates a
  * table while it's scanned, block by block and in parallel, and never holds the whole table.
  */
object TpchGen:

  /** The tables in dependency-free order: region, nation, supplier, part, partsupp, ... */
  val tableNames: Seq[String] = Seq(
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem"
  )

  /**
    * The largest scale factor [[tables]] materializes, so a browser tab can't be asked for
    * gigabytes
    */
  val maxScaleFactor: Double = 1.0

  /** The largest scale factor [[sources]] generates: 1.5 billion orders still fit Int indexes */
  val maxStreamingScaleFactor: Double = 1000.0

  /** Rows (orders for lineitem, parts for partsupp) per generated block */
  val blockRows: Int = 10000

  /**
    * The tables at scale factor `sf` as sources that generate their rows while they're scanned.
    *
    * @param sf
    *   the scale factor, in (0, [[maxStreamingScaleFactor]]]
    */
  def sources(sf: Double): Seq[TpchSource] =
    checkScaleFactor("TPC-H", sf, maxStreamingScaleFactor)
    val gen = TpchGen(sf)
    tableNames.map(TpchSource(_, gen))

  /**
    * Generate every table at scale factor `sf` in memory (0.01 is 60K line items; 1.0 is 6M).
    *
    * @param sf
    *   the scale factor, in (0, [[maxScaleFactor]]]
    */
  def tables(sf: Double): Seq[MemTable] =
    checkScaleFactor("TPC-H", sf, maxScaleFactor)
    val gen = TpchGen(sf)
    // Orders and line items come from one pass per block, not one per table
    val (orders, lineitems) = Seq.tabulate(gen.numBlocks("orders"))(gen.ordersAndLineitems).unzip
    tableNames.map { t =>
      val s = TpchSource(t, gen)
      t match
        case "orders" =>
          s.materialize(orders)
        case "lineitem" =>
          s.materialize(lineitems)
        case _ =>
          s.materialize
    }

  /** Register every table at scale factor `sf` in `catalog`, generated while it's scanned */
  def register(catalog: EngineCatalog, sf: Double): Seq[TpchSource] =
    val ts = sources(sf)
    ts.foreach(catalog.register)
    ts

  private val Money: PType = PType.decimal(15, 2)

  // Dates as days since 1970-01-01
  private[catalog] val StartDate   = 8035  // 1992-01-01
  private[catalog] val CurrentDate = 9298  // 1995-06-17
  private[catalog] val EndDate     = 10591 // 1998-12-31

  // The latest order date, so that every line item ships by EndDate
  private[catalog] val LastOrderDate = EndDate - 151

  /** The columns of each table */
  private[catalog] val schemas: Map[String, Schema] = Map(
    "region" ->
      schema("r_regionkey" -> PType.Int64, "r_name" -> PType.Str, "r_comment" -> PType.Str),
    "nation" ->
      schema(
        "n_nationkey" -> PType.Int64,
        "n_name"      -> PType.Str,
        "n_regionkey" -> PType.Int64,
        "n_comment"   -> PType.Str
      ),
    "supplier" ->
      schema(
        "s_suppkey"   -> PType.Int64,
        "s_name"      -> PType.Str,
        "s_address"   -> PType.Str,
        "s_nationkey" -> PType.Int64,
        "s_phone"     -> PType.Str,
        "s_acctbal"   -> Money,
        "s_comment"   -> PType.Str
      ),
    "customer" ->
      schema(
        "c_custkey"    -> PType.Int64,
        "c_name"       -> PType.Str,
        "c_address"    -> PType.Str,
        "c_nationkey"  -> PType.Int64,
        "c_phone"      -> PType.Str,
        "c_acctbal"    -> Money,
        "c_mktsegment" -> PType.Str,
        "c_comment"    -> PType.Str
      ),
    "part" ->
      schema(
        "p_partkey"     -> PType.Int64,
        "p_name"        -> PType.Str,
        "p_mfgr"        -> PType.Str,
        "p_brand"       -> PType.Str,
        "p_type"        -> PType.Str,
        "p_size"        -> PType.Int32,
        "p_container"   -> PType.Str,
        "p_retailprice" -> Money,
        "p_comment"     -> PType.Str
      ),
    "partsupp" ->
      schema(
        "ps_partkey"    -> PType.Int64,
        "ps_suppkey"    -> PType.Int64,
        "ps_availqty"   -> PType.Int64,
        "ps_supplycost" -> Money,
        "ps_comment"    -> PType.Str
      ),
    "orders" ->
      schema(
        "o_orderkey"      -> PType.Int64,
        "o_custkey"       -> PType.Int64,
        "o_orderstatus"   -> PType.Str,
        "o_totalprice"    -> Money,
        "o_orderdate"     -> PType.Date,
        "o_orderpriority" -> PType.Str,
        "o_clerk"         -> PType.Str,
        "o_shippriority"  -> PType.Int32,
        "o_comment"       -> PType.Str
      ),
    "lineitem" ->
      schema(
        "l_orderkey"      -> PType.Int64,
        "l_partkey"       -> PType.Int64,
        "l_suppkey"       -> PType.Int64,
        "l_linenumber"    -> PType.Int64,
        "l_quantity"      -> Money,
        "l_extendedprice" -> Money,
        "l_discount"      -> Money,
        "l_tax"           -> Money,
        "l_returnflag"    -> PType.Str,
        "l_linestatus"    -> PType.Str,
        "l_shipdate"      -> PType.Date,
        "l_commitdate"    -> PType.Date,
        "l_receiptdate"   -> PType.Date,
        "l_shipinstruct"  -> PType.Str,
        "l_shipmode"      -> PType.Str,
        "l_comment"       -> PType.Str
      )
  )

  private val regions = IndexedSeq("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")

  // (name, region key) in nation-key order
  private val nations = IndexedSeq(
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1)
  )

  private val colors = IndexedSeq(
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "burnished",
    "chartreuse",
    "chiffon",
    "chocolate",
    "coral",
    "cornflower",
    "cornsilk",
    "cream",
    "cyan",
    "dark",
    "deep",
    "dim",
    "dodger",
    "drab",
    "firebrick",
    "floral",
    "forest",
    "frosted",
    "gainsboro",
    "ghost",
    "goldenrod",
    "green",
    "grey",
    "honeydew",
    "hot",
    "indian",
    "ivory",
    "khaki",
    "lace",
    "lavender",
    "lawn",
    "lemon",
    "light",
    "lime",
    "linen",
    "magenta",
    "maroon",
    "medium",
    "metallic",
    "midnight",
    "mint",
    "misty",
    "moccasin",
    "navajo",
    "navy",
    "olive",
    "orange",
    "orchid",
    "pale",
    "papaya",
    "peach",
    "peru",
    "pink",
    "plum",
    "powder",
    "puff",
    "purple",
    "red",
    "rose",
    "rosy",
    "royal",
    "saddle",
    "salmon",
    "sandy",
    "seashell",
    "sienna",
    "sky",
    "slate",
    "smoke",
    "snow",
    "spring",
    "steel",
    "tan",
    "thistle",
    "tomato",
    "turquoise",
    "violet",
    "wheat",
    "white",
    "yellow"
  )

  private val typeSyllable1 = IndexedSeq("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO")
  private val typeSyllable2 = IndexedSeq("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED")
  private val typeSyllable3 = IndexedSeq("TIN", "NICKEL", "BRASS", "STEEL", "COPPER")
  private val containers1   = IndexedSeq("SM", "LG", "MED", "JUMBO", "WRAP")
  private val containers2   = IndexedSeq("CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM")
  private val segments = IndexedSeq("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD")

  private val priorities = IndexedSeq("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
  private val instructions = IndexedSeq(
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN"
  )

  private val shipModes = IndexedSeq("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB")

  // The comment vocabulary of the specification's text grammar (4.2.2.10), abridged
  private val nouns = IndexedSeq(
    "foxes",
    "ideas",
    "theodolites",
    "pinto beans",
    "instructions",
    "dependencies",
    "excuses",
    "platelets",
    "asymptotes",
    "courts",
    "dolphins",
    "multipliers",
    "sauternes",
    "warthogs",
    "frets",
    "dinos",
    "attainments",
    "somas",
    "Tiresias",
    "patterns",
    "forges",
    "braids",
    "frays",
    "warhorses",
    "dugouts",
    "notornis",
    "epitaphs",
    "pearls",
    "tithes",
    "waters",
    "orbits",
    "gifts",
    "sheaves",
    "depths",
    "sentiments",
    "decoys",
    "realms",
    "pains",
    "grouches",
    "escapades",
    "hockey players",
    "packages",
    "requests",
    "accounts",
    "deposits"
  )

  private val verbs = IndexedSeq(
    "sleep",
    "wake",
    "are",
    "cajole",
    "haggle",
    "nag",
    "use",
    "boost",
    "affix",
    "detect",
    "integrate",
    "maintain",
    "nod",
    "was",
    "lose",
    "sublate",
    "solve",
    "thrash",
    "promise",
    "engage",
    "hinder",
    "print",
    "x-ray",
    "breach",
    "eat",
    "grow",
    "impress",
    "mold",
    "poach",
    "serve",
    "run",
    "dazzle",
    "snooze",
    "doze",
    "unwind",
    "kindle",
    "play",
    "hang",
    "believe",
    "doubt"
  )

  private val adjectives = IndexedSeq(
    "furious",
    "sly",
    "careful",
    "blithe",
    "quick",
    "fluffy",
    "slow",
    "quiet",
    "ruthless",
    "thin",
    "close",
    "dogged",
    "daring",
    "brave",
    "stealthy",
    "permanent",
    "enticing",
    "idle",
    "busy",
    "regular",
    "final",
    "ironic",
    "even",
    "bold",
    "silent",
    "special",
    "pending",
    "express",
    "unusual"
  )

  private val adverbs = IndexedSeq(
    "sometimes",
    "always",
    "never",
    "furiously",
    "slyly",
    "carefully",
    "blithely",
    "quickly",
    "fluffily",
    "slowly",
    "quietly",
    "ruthlessly",
    "thinly",
    "closely",
    "doggedly",
    "daringly",
    "bravely",
    "stealthily",
    "permanently",
    "enticingly",
    "idly",
    "busily",
    "regularly",
    "finally",
    "ironically",
    "evenly",
    "boldly",
    "silently"
  )

  private val prepositions = IndexedSeq(
    "about",
    "above",
    "according to",
    "across",
    "after",
    "against",
    "along",
    "alongside of",
    "among",
    "around",
    "at",
    "atop",
    "before",
    "behind",
    "beneath",
    "beside",
    "besides",
    "between",
    "beyond",
    "by",
    "despite",
    "during",
    "except",
    "for",
    "from",
    "inside",
    "instead of",
    "into",
    "near",
    "of",
    "on",
    "outside",
    "over",
    "past",
    "since",
    "through",
    "throughout",
    "to",
    "toward",
    "under",
    "until",
    "up",
    "upon",
    "without",
    "with",
    "within"
  )

end TpchGen

/**
  * The generator at a scale factor. A table is a sequence of blocks ([[TpchGen.blockRows]]); each
  * block draws from its own seeded random stream, so blocks generate independently, concurrently,
  * and in any order, with the same rows every time.
  */
private[catalog] class TpchGen(val sf: Double):
  import TpchGen.*

  val numSuppliers: Int = math.max(1, (10000 * sf).toInt)
  val numParts: Int     = math.max(1, (200000 * sf).toInt)
  val numCustomers: Int = math.max(1, (150000 * sf).toInt)
  val numOrders: Int    = math.max(1, (1500000 * sf).toInt)
  val numClerks: Int    = math.max(1, (1000 * sf).toInt)

  // The units a table's blocks cut: parts for partsupp, orders for lineitem
  private def units(table: String): Int =
    table match
      case "region" =>
        regions.size
      case "nation" =>
        nations.size
      case "supplier" =>
        numSuppliers
      case "customer" =>
        numCustomers
      case "part" | "partsupp" =>
        numParts
      case "orders" | "lineitem" =>
        numOrders
      case other =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Not a TPC-H table: ${other}")

  /** The number of blocks of `table` */
  def numBlocks(table: String): Int = (units(table) + blockRows - 1) / blockRows

  /** The first unit (row, part, or order index) of block `b` */
  def blockStart(b: Int): Int = b * blockRows

  /** The units in block `b` of `table` */
  def blockUnits(table: String, b: Int): Int = math.min(blockRows, units(table) - blockStart(b))

  /** The rows of block `b` of `table`, or None for lineitem, whose count only generating tells */
  def blockRowCount(table: String, b: Int): Option[Long] =
    table match
      case "partsupp" =>
        Some(blockUnits(table, b) * 4L)
      case "lineitem" =>
        None
      case _ =>
        Some(blockUnits(table, b).toLong)

  /** The rows of `table`: exact, except lineitem's (4 line items per order on average) */
  def estimatedRows(table: String): Long =
    table match
      case "partsupp" | "lineitem" =>
        units(table) * 4L
      case t =>
        units(t).toLong

  /** The order key of the i-th order: only the first 8 of every 32 keys are used (4.2.3) */
  def orderKey(i: Int): Long = (i / 8).toLong * 32 + (i % 8) + 1

  /** Generate block `b` of `table`, with every column */
  def block(table: String, b: Int): Batch =
    table match
      case "region" =>
        region
      case "nation" =>
        nation
      case "supplier" =>
        supplier(b)
      case "customer" =>
        customer(b)
      case "part" =>
        part(b)
      case "partsupp" =>
        partsupp(b)
      case "orders" =>
        ordersAndLineitems(b)._1
      case "lineitem" =>
        ordersAndLineitems(b)._2
      case other =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Not a TPC-H table: ${other}")

  private def rnd(table: Int, block: Int): Random = Random(
    19620718L + table * 1000003L + block * 2654435761L
  )

  private def batch(columns: Vec*): Batch = Batch(
    columns.toArray,
    columns.headOption.map(_.size).getOrElse(0)
  )

  private def dates(a: Array[Int]): Vec             = IntVec(PType.Date, a, null)
  private def money(a: Array[Long]): Vec            = LongVec(Money, a, null)
  private def pick[A](r: Random, xs: IndexedSeq[A]) = xs(r.nextInt(xs.size))

  // A uniform integer in [lo, hi]
  private def between(r: Random, lo: Int, hi: Int): Int = lo + r.nextInt(hi - lo + 1)

  // A uniform amount in cents in [lo, hi]
  private def cents(r: Random, lo: Long, hi: Long): Long =
    lo + (r.nextDouble() * (hi - lo + 1)).toLong.min(hi - lo)

  private def keyName(prefix: String, key: Long): String = f"${prefix}#${key}%09d"

  // A random string of 10 to 40 address characters (4.2.2.7)
  private def address(r: Random): String =
    val chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,"
    val len   = between(r, 10, 40)
    String(Array.fill(len)(chars(r.nextInt(chars.length))))

  private def phone(r: Random, nationKey: Int): String =
    f"${nationKey + 10}%02d-${between(r, 100, 999)}-${between(r, 100, 999)}-${between(
        r,
        1000,
        9999
      )}"

  // Sentences of the specification's text grammar, cut to a length in [minLen, maxLen]
  private def text(r: Random, minLen: Int, maxLen: Int): String =
    val target = between(r, minLen, maxLen)
    val sb     = StringBuilder()
    while sb.length < target do
      if sb.nonEmpty then
        sb.append(' ')
      r.nextInt(3) match
        case 0 =>
          sb.append(
            s"${pick(r, adjectives)} ${pick(r, nouns)} ${pick(r, verbs)} ${pick(r, adverbs)}"
          )
        case 1 =>
          sb.append(
            s"${pick(r, nouns)} ${pick(r, verbs)} ${pick(r, prepositions)} the ${pick(
                r,
                adjectives
              )} ${pick(r, nouns)}"
          )
        case _ =>
          sb.append(
            s"${pick(r, adverbs)} ${pick(r, adjectives)} ${pick(r, nouns)} ${pick(r, verbs)}"
          )
      sb.append(
        if r.nextInt(4) == 0 then
          "."
        else
          ""
      )
    sb.toString.take(target).trim

  private def region: Batch =
    val r = rnd(1, 0)
    batch(
      longs(regions.size)(_.toLong),
      strs(regions.size)(regions(_)),
      strs(regions.size)(_ => text(r, 31, 115))
    )

  private def nation: Batch =
    val r = rnd(2, 0)
    batch(
      longs(nations.size)(_.toLong),
      strs(nations.size)(nations(_)._1),
      longs(nations.size)(nations(_)._2.toLong),
      strs(nations.size)(_ => text(r, 31, 114))
    )

  private def supplier(b: Int): Batch =
    val r       = rnd(3, b)
    val start   = blockStart(b)
    val n       = blockUnits("supplier", b)
    val nations = Array.fill(n)(r.nextInt(25))
    // 5 per 10,000 suppliers have complaints and 5 have recommendations (4.2.3)
    val comments =
      Array.tabulate(n) { i =>
        val c = text(r, 25, 100)
        r.nextInt(2000) match
          case 0 =>
            s"${c.take(40)} Customer ${pick(r, adjectives)} Complaints"
          case 1 =>
            s"${c.take(40)} Customer ${pick(r, adjectives)} Recommends"
          case _ =>
            c
      }
    batch(
      longs(n)(start + _ + 1L),
      strs(n)(i => keyName("Supplier", start + i + 1L)),
      strs(n)(_ => address(r)),
      longs(n)(nations(_).toLong),
      strs(n)(i => phone(r, nations(i))),
      money(Array.fill(n)(cents(r, -99999, 999999))),
      strs(n)(comments(_))
    )

  end supplier

  private def customer(b: Int): Batch =
    val r       = rnd(4, b)
    val start   = blockStart(b)
    val n       = blockUnits("customer", b)
    val nations = Array.fill(n)(r.nextInt(25))
    batch(
      longs(n)(start + _ + 1L),
      strs(n)(i => keyName("Customer", start + i + 1L)),
      strs(n)(_ => address(r)),
      longs(n)(nations(_).toLong),
      strs(n)(i => phone(r, nations(i))),
      money(Array.fill(n)(cents(r, -99999, 999999))),
      strs(n)(_ => pick(r, segments)),
      strs(n)(_ => text(r, 29, 116))
    )

  // The retail price of a part in cents (4.2.3)
  private def retailPrice(partKey: Long): Long =
    90000 + ((partKey / 10) % 20001) + 100 * (partKey % 1000)

  private def part(b: Int): Batch =
    val r     = rnd(5, b)
    val start = blockStart(b)
    val n     = blockUnits("part", b)
    val names =
      Array.fill(n) {
        r.shuffle(colors.indices.toList).take(5).map(colors(_)).mkString(" ")
      }
    val mfgr = Array.fill(n)(between(r, 1, 5))
    batch(
      longs(n)(start + _ + 1L),
      strs(n)(names(_)),
      strs(n)(i => s"Manufacturer#${mfgr(i)}"),
      strs(n)(i => s"Brand#${mfgr(i)}${between(r, 1, 5)}"),
      strs(n)(_ =>
        s"${pick(r, typeSyllable1)} ${pick(r, typeSyllable2)} ${pick(r, typeSyllable3)}"
      ),
      ints(n)(_ => between(r, 1, 50)),
      strs(n)(_ => s"${pick(r, containers1)} ${pick(r, containers2)}"),
      money(Array.tabulate(n)(i => retailPrice(start + i + 1L))),
      strs(n)(_ => text(r, 5, 22))
    )

  end part

  // The i-th (0 to 3) supplier of a part (4.2.3)
  private def supplierOf(partKey: Long, i: Int): Long =
    val s = numSuppliers.toLong
    (partKey + (i * ((s / 4) + (partKey - 1) / s))) % s + 1

  private def partsupp(b: Int): Batch =
    val r     = rnd(6, b)
    val start = blockStart(b)
    val n     = blockUnits("partsupp", b) * 4
    batch(
      longs(n)(i => start + i / 4 + 1L),
      longs(n)(i => supplierOf(start + i / 4 + 1L, i % 4)),
      longs(n)(_ => between(r, 1, 9999).toLong),
      money(Array.fill(n)(cents(r, 100, 100000))),
      strs(n)(_ => text(r, 49, 198))
    )

  // A block of orders and their line items, generated together for the derived status and total
  def ordersAndLineitems(b: Int): (Batch, Batch) =
    val r     = rnd(7, b)
    val start = blockStart(b)
    val n     = blockUnits("orders", b)

    val oKey      = new Array[Long](n)
    val oCust     = new Array[Long](n)
    val oStatus   = new Array[String](n)
    val oTotal    = new Array[Long](n)
    val oDate     = new Array[Int](n)
    val oPriority = new Array[String](n)
    val oClerk    = new Array[String](n)
    val oComment  = new Array[String](n)

    val lOrder   = Array.newBuilder[Long]
    val lPart    = Array.newBuilder[Long]
    val lSupp    = Array.newBuilder[Long]
    val lLine    = Array.newBuilder[Long]
    val lQty     = Array.newBuilder[Long]
    val lPrice   = Array.newBuilder[Long]
    val lDisc    = Array.newBuilder[Long]
    val lTax     = Array.newBuilder[Long]
    val lFlag    = Array.newBuilder[String]
    val lStatus  = Array.newBuilder[String]
    val lShip    = Array.newBuilder[Int]
    val lCommit  = Array.newBuilder[Int]
    val lReceipt = Array.newBuilder[Int]
    val lInstr   = Array.newBuilder[String]
    val lMode    = Array.newBuilder[String]
    val lComment = Array.newBuilder[String]

    var i = 0
    while i < n do
      val key = orderKey(start + i)
      // Customers with a key divisible by 3 place no orders
      var cust = between(r, 1, numCustomers).toLong
      if numCustomers >= 3 then
        while cust % 3 == 0 do
          cust = between(r, 1, numCustomers).toLong
      val date  = between(r, StartDate, LastOrderDate)
      val lines = between(r, 1, 7)
      // Sum of extendedprice * (100 + tax) * (100 - discount), in 1/10000 cents
      var total = 0L
      var open  = 0
      var j     = 0
      while j < lines do
        val partKey  = between(r, 1, numParts).toLong
        val qty      = between(r, 1, 50).toLong
        val price    = qty * retailPrice(partKey)
        val discount = between(r, 0, 10).toLong
        val tax      = between(r, 0, 8).toLong
        val ship     = date + between(r, 1, 121)
        val receipt  = ship + between(r, 1, 30)
        val status   =
          if ship > CurrentDate then
            "O"
          else
            "F"
        if status == "O" then
          open += 1
        lOrder += key
        lPart += partKey
        lSupp += supplierOf(partKey, r.nextInt(4))
        lLine += (j + 1).toLong
        lQty += qty * 100
        lPrice += price
        lDisc += discount
        lTax += tax
        lFlag += (
          if receipt <= CurrentDate then
            if r.nextBoolean() then
              "R"
            else
              "A"
          else
            "N"
        )
        lStatus += status
        lShip += ship
        lCommit += date + between(r, 30, 90)
        lReceipt += receipt
        lInstr += pick(r, instructions)
        lMode += pick(r, shipModes)
        lComment += text(r, 10, 43)
        total += price * (100 + tax) * (100 - discount)
        j += 1
      end while
      oKey(i) = key
      oCust(i) = cust
      oStatus(i) =
        if open == lines then
          "O"
        else if open == 0 then
          "F"
        else
          "P"
      oTotal(i) = (total + 5000) / 10000
      oDate(i) = date
      oPriority(i) = pick(r, priorities)
      oClerk(i) = keyName("Clerk", between(r, 1, numClerks).toLong)
      oComment(i) =
        // Some comments mention special requests, which TPC-H q13 filters out
        if r.nextInt(100) == 0 then
          s"${text(r, 10, 30)} special ${pick(r, nouns)} requests ${pick(r, verbs)}"
        else
          text(r, 19, 78)
      i += 1
    end while

    val orders = batch(
      LongVec(PType.Int64, oKey, null),
      LongVec(PType.Int64, oCust, null),
      StrVec(oStatus, null),
      money(oTotal),
      dates(oDate),
      StrVec(oPriority, null),
      StrVec(oClerk, null),
      ints(n)(_ => 0),
      StrVec(oComment, null)
    )
    val lineitem = batch(
      LongVec(PType.Int64, lOrder.result(), null),
      LongVec(PType.Int64, lPart.result(), null),
      LongVec(PType.Int64, lSupp.result(), null),
      LongVec(PType.Int64, lLine.result(), null),
      money(lQty.result()),
      money(lPrice.result()),
      money(lDisc.result()),
      money(lTax.result()),
      StrVec(lFlag.result(), null),
      StrVec(lStatus.result(), null),
      dates(lShip.result()),
      dates(lCommit.result()),
      dates(lReceipt.result()),
      StrVec(lInstr.result(), null),
      StrVec(lMode.result(), null),
      StrVec(lComment.result(), null)
    )
    (orders, lineitem)

  end ordersAndLineitems

end TpchGen

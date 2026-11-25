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

  /** The largest scale factor allowed, so a browser tab can't be asked for gigabytes */
  val maxScaleFactor: Double = 1.0

  /**
    * Generate every table at scale factor `sf` (0.01 is 60K line items; 1.0 is 6M).
    *
    * @param sf
    *   the scale factor, in (0, [[maxScaleFactor]]]
    */
  def tables(sf: Double): Seq[MemTable] =
    if !(sf > 0 && sf <= maxScaleFactor) then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"TPC-H scale factor must be in (0, ${maxScaleFactor}]: ${sf}")
    TpchGen(sf).all

  /** Register every table at scale factor `sf` in `catalog` */
  def register(catalog: EngineCatalog, sf: Double): Seq[MemTable] =
    val ts = tables(sf)
    ts.foreach(catalog.register)
    ts

  private val Money: PType = PType.decimal(15, 2)

  // Dates as days since 1970-01-01
  private val StartDate   = 8035  // 1992-01-01
  private val CurrentDate = 9298  // 1995-06-17
  private val EndDate     = 10591 // 1998-12-31

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

/** One generation run at a scale factor; each table draws from its own seeded random stream */
private class TpchGen(sf: Double):
  import TpchGen.*

  private val numSuppliers = math.max(1, (10000 * sf).toInt)
  private val numParts     = math.max(1, (200000 * sf).toInt)
  private val numCustomers = math.max(1, (150000 * sf).toInt)
  private val numOrders    = math.max(1, (1500000 * sf).toInt)
  private val numClerks    = math.max(1, (1000 * sf).toInt)

  private def rnd(table: Int): Random = Random(19620718L + table * 1000003L)

  private def field(name: String, t: PType): Field = Field(name, t)

  private def table(name: String, fields: Seq[Field], columns: Seq[Vec]): MemTable =
    val n = columns.headOption.map(_.size).getOrElse(0)
    MemTable(name, Schema(fields.toIndexedSeq), Seq(Batch(columns.toArray, n)))

  private def longs(n: Int)(f: Int => Long): Vec  = LongVec(PType.Int64, Array.tabulate(n)(f), null)
  private def ints(n: Int)(f: Int => Int): Vec    = IntVec(PType.Int32, Array.tabulate(n)(f), null)
  private def dates(a: Array[Int]): Vec           = IntVec(PType.Date, a, null)
  private def money(a: Array[Long]): Vec          = LongVec(Money, a, null)
  private def strs(n: Int)(f: Int => String): Vec = StrVec(Array.tabulate(n)(f), null)
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

  private def region: MemTable =
    val r = rnd(1)
    table(
      "region",
      Seq(
        field("r_regionkey", PType.Int64),
        field("r_name", PType.Str),
        field("r_comment", PType.Str)
      ),
      Seq(
        longs(regions.size)(_.toLong),
        strs(regions.size)(regions(_)),
        strs(regions.size)(_ => text(r, 31, 115))
      )
    )

  private def nation: MemTable =
    val r = rnd(2)
    table(
      "nation",
      Seq(
        field("n_nationkey", PType.Int64),
        field("n_name", PType.Str),
        field("n_regionkey", PType.Int64),
        field("n_comment", PType.Str)
      ),
      Seq(
        longs(nations.size)(_.toLong),
        strs(nations.size)(nations(_)._1),
        longs(nations.size)(nations(_)._2.toLong),
        strs(nations.size)(_ => text(r, 31, 114))
      )
    )

  private def supplier: MemTable =
    val r       = rnd(3)
    val n       = numSuppliers
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
    table(
      "supplier",
      Seq(
        field("s_suppkey", PType.Int64),
        field("s_name", PType.Str),
        field("s_address", PType.Str),
        field("s_nationkey", PType.Int64),
        field("s_phone", PType.Str),
        field("s_acctbal", Money),
        field("s_comment", PType.Str)
      ),
      Seq(
        longs(n)(_ + 1L),
        strs(n)(i => keyName("Supplier", i + 1L)),
        strs(n)(_ => address(r)),
        longs(n)(nations(_).toLong),
        strs(n)(i => phone(r, nations(i))),
        money(Array.fill(n)(cents(r, -99999, 999999))),
        strs(n)(comments(_))
      )
    )

  end supplier

  private def customer: MemTable =
    val r       = rnd(4)
    val n       = numCustomers
    val nations = Array.fill(n)(r.nextInt(25))
    table(
      "customer",
      Seq(
        field("c_custkey", PType.Int64),
        field("c_name", PType.Str),
        field("c_address", PType.Str),
        field("c_nationkey", PType.Int64),
        field("c_phone", PType.Str),
        field("c_acctbal", Money),
        field("c_mktsegment", PType.Str),
        field("c_comment", PType.Str)
      ),
      Seq(
        longs(n)(_ + 1L),
        strs(n)(i => keyName("Customer", i + 1L)),
        strs(n)(_ => address(r)),
        longs(n)(nations(_).toLong),
        strs(n)(i => phone(r, nations(i))),
        money(Array.fill(n)(cents(r, -99999, 999999))),
        strs(n)(_ => pick(r, segments)),
        strs(n)(_ => text(r, 29, 116))
      )
    )

  // The retail price of a part in cents (4.2.3)
  private def retailPrice(partKey: Long): Long =
    90000 + ((partKey / 10) % 20001) + 100 * (partKey % 1000)

  private def part: MemTable =
    val r     = rnd(5)
    val n     = numParts
    val names =
      Array.fill(n) {
        r.shuffle(colors.indices.toList).take(5).map(colors(_)).mkString(" ")
      }
    val mfgr = Array.fill(n)(between(r, 1, 5))
    table(
      "part",
      Seq(
        field("p_partkey", PType.Int64),
        field("p_name", PType.Str),
        field("p_mfgr", PType.Str),
        field("p_brand", PType.Str),
        field("p_type", PType.Str),
        field("p_size", PType.Int32),
        field("p_container", PType.Str),
        field("p_retailprice", Money),
        field("p_comment", PType.Str)
      ),
      Seq(
        longs(n)(_ + 1L),
        strs(n)(names(_)),
        strs(n)(i => s"Manufacturer#${mfgr(i)}"),
        strs(n)(i => s"Brand#${mfgr(i)}${between(r, 1, 5)}"),
        strs(n)(_ =>
          s"${pick(r, typeSyllable1)} ${pick(r, typeSyllable2)} ${pick(r, typeSyllable3)}"
        ),
        ints(n)(_ => between(r, 1, 50)),
        strs(n)(_ => s"${pick(r, containers1)} ${pick(r, containers2)}"),
        money(Array.tabulate(n)(i => retailPrice(i + 1L))),
        strs(n)(_ => text(r, 5, 22))
      )
    )

  end part

  // The i-th (0 to 3) supplier of a part (4.2.3)
  private def supplierOf(partKey: Long, i: Int): Long =
    val s = numSuppliers.toLong
    (partKey + (i * ((s / 4) + (partKey - 1) / s))) % s + 1

  private def partsupp: MemTable =
    val r = rnd(6)
    val n = numParts * 4
    table(
      "partsupp",
      Seq(
        field("ps_partkey", PType.Int64),
        field("ps_suppkey", PType.Int64),
        field("ps_availqty", PType.Int64),
        field("ps_supplycost", Money),
        field("ps_comment", PType.Str)
      ),
      Seq(
        longs(n)(i => i / 4 + 1L),
        longs(n)(i => supplierOf(i / 4 + 1L, i % 4)),
        longs(n)(_ => between(r, 1, 9999).toLong),
        money(Array.fill(n)(cents(r, 100, 100000))),
        strs(n)(_ => text(r, 49, 198))
      )
    )

  // Orders and their line items, generated together for the derived status and total price
  private def ordersAndLineitems: (MemTable, MemTable) =
    val r = rnd(7)
    val n = numOrders

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
      // Only the first 8 of every 32 keys are used (4.2.3)
      val key = (i / 8).toLong * 32 + (i % 8) + 1
      // Customers with a key divisible by 3 place no orders
      var cust = between(r, 1, numCustomers).toLong
      if numCustomers >= 3 then
        while cust % 3 == 0 do
          cust = between(r, 1, numCustomers).toLong
      val date  = between(r, StartDate, EndDate - 151)
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

    val orders = table(
      "orders",
      Seq(
        field("o_orderkey", PType.Int64),
        field("o_custkey", PType.Int64),
        field("o_orderstatus", PType.Str),
        field("o_totalprice", Money),
        field("o_orderdate", PType.Date),
        field("o_orderpriority", PType.Str),
        field("o_clerk", PType.Str),
        field("o_shippriority", PType.Int32),
        field("o_comment", PType.Str)
      ),
      Seq(
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
    )
    val lineitem = table(
      "lineitem",
      Seq(
        field("l_orderkey", PType.Int64),
        field("l_partkey", PType.Int64),
        field("l_suppkey", PType.Int64),
        field("l_linenumber", PType.Int64),
        field("l_quantity", Money),
        field("l_extendedprice", Money),
        field("l_discount", Money),
        field("l_tax", Money),
        field("l_returnflag", PType.Str),
        field("l_linestatus", PType.Str),
        field("l_shipdate", PType.Date),
        field("l_commitdate", PType.Date),
        field("l_receiptdate", PType.Date),
        field("l_shipinstruct", PType.Str),
        field("l_shipmode", PType.Str),
        field("l_comment", PType.Str)
      ),
      Seq(
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
    )
    (orders, lineitem)

  end ordersAndLineitems

  def all: Seq[MemTable] =
    val (orders, lineitem) = ordersAndLineitems
    Seq(region, nation, supplier, customer, part, partsupp, orders, lineitem)

end TpchGen

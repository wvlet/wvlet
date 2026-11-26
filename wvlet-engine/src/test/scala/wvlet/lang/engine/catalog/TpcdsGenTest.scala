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
    val counts: Map[String, Long] = sources.view.mapValues(_.estimatedRows.get).toMap
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
    sources
      .values
      .foreach { s =>
        val all = rowsOf(s.name)
        s.schema
          .names
          .zipWithIndex
          .foreach { (c, i) =>
            val vs = all.map(_(i))
            s.columnRange(c)
              .foreach { (lo, hi) =>
                val ls = vs.map {
                  case x: Long =>
                    x
                  case x: Int =>
                    x.toLong
                  case x =>
                    fail(s"Unexpected value of ${c}: ${x}")
                }
                (c, ls.min, ls.max) shouldBe (c, lo, hi)
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

end TpcdsGenTest

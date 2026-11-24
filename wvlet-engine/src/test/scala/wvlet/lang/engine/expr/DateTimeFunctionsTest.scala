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
package wvlet.lang.engine.expr

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.vector.TimeZones
import wvlet.lang.engine.vector.ValueOps
import wvlet.uni.test.UniTest

/**
  * Date/time kernels and time zone rules on plain values, on every platform; results against DuckDB
  * are checked in EngineStdlibDateTimeTest
  */
class DateTimeFunctionsTest extends UniTest:
  private def ts(s: String): Long = ValueOps.parseTimestamp(s)

  /** Run zone-dependent checks except on Scala Native, which supports only UTC */
  private def withZones(body: => Unit): Unit =
    try
      body
    catch
      case e: WvletLangException if e.statusCode == StatusCode.NOT_IMPLEMENTED =>
        debug(s"skipped: ${e.getMessage}")

  test("should name days and months and compute the ISO year") {
    DateTimeFunctions.dayName(ts("2024-01-01")) shouldBe "Monday"
    DateTimeFunctions.monthName(ts("1969-12-31 23:00:00")) shouldBe "December"
    DateTimeFunctions.isoYear(ts("2024-12-30")) shouldBe 2025L
    DateTimeFunctions.isoYear(ts("2021-01-03")) shouldBe 2020L
  }

  test("should parse strftime patterns and reject partial matches") {
    DateTimeFunctions.strptime("2024-02-05 13:04:05.5", "%Y-%m-%d %H:%M:%S.%f") shouldBe
      Some(ts("2024-02-05 13:04:05.5"))
    DateTimeFunctions.strptime("05 feb 2024 01:00 PM", "%d %b %Y %I:%M %p") shouldBe
      Some(ts("2024-02-05 13:00:00"))
    DateTimeFunctions.strptime("2024-02-05 extra", "%Y-%m-%d") shouldBe None
    DateTimeFunctions.strptime("2024-02-30", "%Y-%m-%d") shouldBe None
    DateTimeFunctions.strptime("2024-02-05 +09:00", "%Y-%m-%d %z") shouldBe
      Some(ts("2024-02-04 15:00:00"))
  }

  test("should reject unknown strptime specifiers") {
    intercept[WvletLangException] {
      DateTimeFunctions.strptime("5", "%e")
    }.getMessage shouldContain "%e"
  }

  test("should count complete months with DuckDB's month-end rule") {
    DateTimeFunctions.dateSub("month", ts("2024-01-31"), ts("2024-02-29")) shouldBe 1L
    DateTimeFunctions.dateSub(
      "month",
      ts("2024-01-31 10:00:00"),
      ts("2024-02-29 09:00:00")
    ) shouldBe 0L
    DateTimeFunctions.dateSub("month", ts("2024-03-14"), ts("2024-01-15")) shouldBe -1L
    DateTimeFunctions.dateSub("year", ts("2020-02-29"), ts("2021-02-28")) shouldBe 1L
    DateTimeFunctions.dateSub("day", ts("2024-01-02 01:00:00"), ts("2024-01-01 02:00:00")) shouldBe
      0L
  }

  test("should parse TIMESTAMPTZ offsets and drop them for plain timestamps") {
    ValueOps.parseTimestampTz("2024-01-01 10:00:00+09") shouldBe ts("2024-01-01 01:00:00")
    ValueOps.parseTimestampTz("2024-01-01 10:00:00.5-05:30") shouldBe ts("2024-01-01 15:30:00.5")
    ValueOps.parseTimestampTz("2024-01-01T10:00:00Z") shouldBe ts("2024-01-01 10:00:00")
    ValueOps.parseTimestamp("2024-01-01 10:00:00+09") shouldBe ts("2024-01-01 10:00:00")
    ValueOps.parseTimestamp("2024-01-01 10:00:00 Asia/Tokyo") shouldBe ts("2024-01-01 10:00:00")
    ValueOps.formatTimestampTz(ts("2024-01-01 01:00:00")) shouldBe "2024-01-01 01:00:00+00"
  }

  test("should convert between wall-clock times and instants") {
    TimeZones.toInstant(ts("2024-01-01 10:00:00"), "UTC") shouldBe ts("2024-01-01 10:00:00")
    withZones {
      TimeZones.toInstant(ts("2024-01-01 10:00:00"), "Asia/Tokyo") shouldBe
        ts("2024-01-01 01:00:00")
      TimeZones.toLocal(ts("2024-01-01 01:00:00"), "Asia/Tokyo") shouldBe ts("2024-01-01 10:00:00")
      // A skipped time uses the offset before the transition, a repeated time the one after it
      TimeZones.toInstant(ts("2024-03-10 02:30:00"), "America/New_York") shouldBe
        ts("2024-03-10 07:30:00")
      TimeZones.toInstant(ts("2024-11-03 01:30:00"), "America/New_York") shouldBe
        ts("2024-11-03 06:30:00")
      ValueOps.parseTimestampTz("2024-01-01 10:00:00 Asia/Tokyo") shouldBe ts("2024-01-01 01:00:00")
    }
  }

  test("should reject unknown time zones") {
    withZones {
      intercept[WvletLangException] {
        TimeZones.toLocal(0L, "Bogus/Zone")
      }.getMessage shouldContain "Bogus/Zone"
    }
  }

end DateTimeFunctionsTest

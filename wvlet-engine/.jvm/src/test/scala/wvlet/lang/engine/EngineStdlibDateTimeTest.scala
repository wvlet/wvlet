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
package wvlet.lang.engine

import wvlet.uni.test.UniTest

/**
  * Stdlib date/time functions and the engine's time zone support match DuckDB (in UTC), NULLs and
  * edges included
  */
class EngineStdlibDateTimeTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  private val dates =
    "from [['2024-01-01'], ['2024-12-30'], ['2021-01-03'], ['2020-12-31'], ['1969-12-31'], " +
      "['2000-02-29'], [null]] as t(s)\n"

  test("day_name, month_name, and iso_year on dates match DuckDB") {
    check(
      dates +
        "select s, s.to_date.day_name as dn, s.to_date.month_name as mn, s.to_date.iso_year as y"
    )
  }

  test("day_name, month_name, and iso_year on timestamps match DuckDB") {
    check(
      "from [['2024-03-03 10:00:00'], ['2027-01-01 23:59:59.5'], ['1969-12-31 23:00:00'], " +
        "[null]] as t(s)\n" +
        "select s, s.to_timestamp.day_name as dn, s.to_timestamp.month_name as mn, " +
        "s.to_timestamp.iso_year as y"
    )
  }

  test("to_timestamp(format) parses strftime codes as DuckDB") {
    check(
      "from [['2024-01-02 03:04:05'], ['2024-1-5 7:8:9'], [null]] as t(s)\n" +
        "select s, s.to_timestamp('%Y-%m-%d %H:%M:%S') as v"
    )
    check(
      "from [['02/01/2024'], ['31/12/1999']] as t(s)\nselect s, s.to_timestamp('%d/%m/%Y') as v"
    )
    check(
      "from [['2024-01-02 03:04:05.123456'], ['2024-01-02 03:04:05.5']] as t(s)\n" +
        "select s, s.to_timestamp('%Y-%m-%d %H:%M:%S.%f') as v"
    )
    check(
      "from [['Mon, 05 Feb 2024 01:02:03 PM'], ['Fri, 09 Aug 2024 12:30:00 AM']] as t(s)\n" +
        "select s, s.to_timestamp('%a, %d %b %Y %I:%M:%S %p') as v"
    )
    check(
      "from [['Monday February 5 24'], ['Sunday December 31 69']] as t(s)\n" +
        "select s, s.to_timestamp('%A %B %d %y') as v"
    )
    check("from [['2024 036'], ['2023 365']] as t(s)\nselect s, s.to_timestamp('%Y %j') as v")
    check("from [['20240205'], ['19991231']] as t(s)\nselect s, s.to_timestamp('%Y%m%d') as v")
    check("from [['12/31/68'], ['12/31/69']] as t(s)\nselect s, s.to_timestamp('%m/%d/%y') as v")
  }

  test("try_to_timestamp returns NULL where DuckDB's strptime fails") {
    check(
      "from [['2024-02-30'], ['2024-02-05 extra'], [' 2024-02-05'], ['2024-13-01'], ['x'], " +
        "['2024-02-05'], [''], [null]] as t(s)\n" + "select s, s.try_to_timestamp('%Y-%m-%d') as v"
    )
    check(
      "from [['24:00:00'], ['23:60:00'], ['13:00:00 PM'], ['12:00:00 AM']] as t(s)\n" +
        "select s, s.try_to_timestamp('%H:%M:%S') as a, s.try_to_timestamp('%I:%M:%S %p') as b"
    )
  }

  test("from_unixtime keeps fractional seconds and to_unixtime floors") {
    check(
      "from [[1.5], [-1.5], [-0.5], [1700000000.123456], [0.0], [null]] as t(x)\n" +
        "select x, x.to_double.from_unixtime as t, x.to_double.from_unixtime.to_unixtime as u"
    )
    check(
      "from [['1969-12-31 23:59:59.5'], ['2024-01-01 00:00:00.999'], ['2024-01-01 00:00:00']] " +
        "as t(s)\nselect s, s.to_timestamp.to_unixtime as u"
    )
  }

  test("to_time_zone and from_time_zone convert as DuckDB, DST gaps and overlaps included") {
    check(
      "from [['2024-01-01 10:00:00'], ['2024-03-10 02:30:00'], ['2024-03-10 07:30:00'], " +
        "['2024-11-03 01:30:00'], ['2024-11-03 05:30:00'], ['2024-11-03 06:30:00'], " +
        "['1969-07-20 20:17:40.5'], [null]] as t(s)\n" +
        "select s, s.to_timestamp.from_time_zone('America/New_York') as ny_utc, " +
        "s.to_timestamp.to_time_zone('America/New_York') as ny, " +
        "s.to_timestamp.to_time_zone('Asia/Tokyo') as tokyo, " +
        "s.to_timestamp.from_time_zone('Asia/Kolkata') as kolkata_utc, " +
        "s.to_timestamp.to_time_zone('UTC') as utc"
    )
    check(
      "from [['2024-03-31 02:30:00'], ['2024-10-27 02:30:00'], ['2024-04-07 02:30:00'], " +
        "['2024-10-06 02:30:00']] as t(s)\n" +
        "select s, s.to_timestamp.from_time_zone('Europe/Berlin') as berlin, " +
        "s.to_timestamp.from_time_zone('Australia/Sydney') as sydney, " +
        "s.to_timestamp.to_time_zone('Australia/Sydney') as sydney_local"
    )
  }

  test("current_date and current_timestamp are UTC and work bare") {
    check(
      "from [[1]] as t(x)\n" + "select current_timestamp.to_date = current_date as same_day, " +
        "current_timestamp.diff_seconds(sql\"now() AT TIME ZONE 'UTC'\") < 5 as recent"
    )
  }

  test("AT TIME ZONE and TIMESTAMPTZ casts match DuckDB") {
    check(
      "from [['2024-01-01 10:00:00+09'], ['2024-01-01 10:00:00.123+09:30'], " +
        "['2024-01-01T10:00:00Z'], ['2024-01-01 10:00:00-0500'], " +
        "['2024-01-01 10:00:00 Asia/Tokyo'], ['2024-01-01 10:00:00'], ['2024-01-01'], " +
        "[null]] as t(s)\n" +
        "select s, sql\"cast(s as timestamptz)\" as tz, sql\"cast(s as timestamp)\" as ts, " +
        "sql\"cast(cast(s as timestamptz) as varchar)\" as str, " +
        "sql\"cast(s as timestamptz) AT TIME ZONE 'Asia/Tokyo'\" as tokyo, " +
        "sql\"cast(s as timestamp) AT TIME ZONE 'America/New_York'\" as ny"
    )
  }

  test("epoch, to_timestamp, and strptime with %z follow DuckDB's types") {
    check(
      "from [['2024-01-01 10:00:00.25'], ['1969-12-31 23:59:59.5'], [null]] as t(s)\n" +
        "select s, sql\"epoch(cast(s as timestamp))\" as e, " +
        "sql\"to_timestamp(epoch(cast(s as timestamp)))\" as back"
    )
    check(
      "from [['2024-01-02 03:04:05 +09:00'], ['2024-01-02 03:04:05 -0530']] as t(s)\n" +
        "select s, sql\"strptime(s, '%Y-%m-%d %H:%M:%S %z')\" as v"
    )
    check(
      "from [['2024-01-02 03:04:05 UTC'], ['2024-01-02 03:04:05 Asia/Tokyo']] as t(s)\n" +
        "select s, sql\"strptime(s, '%Y-%m-%d %H:%M:%S %Z')\" as v"
    )
  }

  test("TIMESTAMPTZ compares and combines with plain timestamps as DuckDB") {
    check(
      "from [['2024-01-01 10:00:00+09', '2024-01-01 01:00:00'], " +
        "['2024-01-01 10:00:00+09', '2024-01-01 02:00:00']] as t(a, b)\n" +
        "select a, b, sql\"cast(a as timestamptz) = cast(b as timestamp)\" as eq, " +
        "sql\"cast(a as timestamptz) < cast(b as timestamp)\" as lt, " +
        "sql\"cast(a as timestamptz) + INTERVAL 1 DAY\" as next_day, " +
        "sql\"date_trunc('day', cast(a as timestamptz))\" as day, " +
        "sql\"year(cast(a as timestamptz))\" as y"
    )
  }

  test("date_sub counts complete parts as DuckDB") {
    check(
      "from [['2024-01-31', '2024-02-29'], ['2024-01-15', '2024-03-14'], " +
        "['2024-03-14', '2024-01-15'], ['2024-02-29', '2024-03-31'], " +
        "['2024-03-31', '2024-02-29'], ['2024-01-30', '2024-02-29'], " +
        "['2020-02-29', '2021-02-28'], ['2020-01-01', '2020-12-31'], " +
        "['1999-12-31', '2000-01-01'], ['2024-01-01', null]] as t(a, b)\n" + "select a, b, " +
        "sql\"date_sub('month', cast(a as date), cast(b as date))\" as months, " +
        "sql\"date_sub('year', cast(a as date), cast(b as date))\" as years, " +
        "sql\"date_sub('quarter', cast(a as date), cast(b as date))\" as quarters, " +
        "sql\"date_sub('week', cast(a as date), cast(b as date))\" as weeks, " +
        "sql\"date_sub('day', cast(a as date), cast(b as date))\" as days"
    )
    check(
      "from [['2024-01-31 10:00:00', '2024-02-29 09:00:00'], " +
        "['2024-01-29 10:00:00', '2024-02-29 09:00:00'], " +
        "['2024-01-01 23:00:00', '2024-01-02 01:00:00'], " +
        "['2024-01-02 01:00:00', '2024-01-01 02:00:00'], " +
        "['2024-01-01 10:59:00', '2024-01-01 11:30:00'], " +
        "['2024-01-01 00:00:00.9', '2024-01-01 00:00:02.1'], " +
        "['1969-12-31 23:59:59.5', '1970-01-01 00:00:00.25']] as t(a, b)\n" + "select a, b, " +
        "sql\"date_sub('month', cast(a as timestamp), cast(b as timestamp))\" as months, " +
        "sql\"date_sub('day', cast(a as timestamp), cast(b as timestamp))\" as days, " +
        "sql\"date_sub('hour', cast(a as timestamp), cast(b as timestamp))\" as hours, " +
        "sql\"date_sub('minute', cast(a as timestamp), cast(b as timestamp))\" as minutes, " +
        "sql\"date_sub('second', cast(a as timestamp), cast(b as timestamp))\" as seconds, " +
        "sql\"date_sub('millisecond', cast(a as timestamp), cast(b as timestamp))\" as ms"
    )
    check(
      "from [['1900-01-01', '2100-01-01'], ['2000-01-01', '2019-12-31'], " +
        "['1000-01-01', '3000-01-01']] as t(a, b)\n" +
        "select a, b, sql\"date_sub('decade', cast(a as date), cast(b as date))\" as decades, " +
        "sql\"date_sub('century', cast(a as date), cast(b as date))\" as centuries, " +
        "sql\"date_sub('millennium', cast(a as date), cast(b as date))\" as millennia"
    )
  }

end EngineStdlibDateTimeTest

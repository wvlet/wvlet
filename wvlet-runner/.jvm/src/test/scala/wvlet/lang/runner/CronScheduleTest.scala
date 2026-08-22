package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

import java.time.ZoneId
import java.time.ZonedDateTime

class CronScheduleTest extends UniTest:

  private val utc = ZoneId.of("UTC")

  private def at(s: String): ZonedDateTime = ZonedDateTime.parse(s)

  test("fire a daily schedule at the configured time") {
    val cron = CronSchedule.parse("0 2 * * *")
    cron.nextAfter(at("2026-01-01T00:00:00Z")) shouldBe at("2026-01-01T02:00:00Z")
    cron.nextAfter(at("2026-01-01T02:00:00Z")) shouldBe at("2026-01-02T02:00:00Z")
    cron.nextAfter(at("2026-01-01T03:15:00Z")) shouldBe at("2026-01-02T02:00:00Z")
  }

  test("fire step schedules from the range start") {
    val cron = CronSchedule.parse("*/15 * * * *")
    cron.nextAfter(at("2026-01-01T00:07:00Z")) shouldBe at("2026-01-01T00:15:00Z")
    cron.nextAfter(at("2026-01-01T00:45:00Z")) shouldBe at("2026-01-01T01:00:00Z")
  }

  test("combine lists, ranges, and steps") {
    val cron = CronSchedule.parse("0 9-17/4 * * *")
    cron.nextAfter(at("2026-01-01T08:00:00Z")) shouldBe at("2026-01-01T09:00:00Z")
    cron.nextAfter(at("2026-01-01T09:00:00Z")) shouldBe at("2026-01-01T13:00:00Z")
    cron.nextAfter(at("2026-01-01T17:00:00Z")) shouldBe at("2026-01-02T09:00:00Z")

    val listed = CronSchedule.parse("5,35 0 * * *")
    listed.nextAfter(at("2026-01-01T00:05:00Z")) shouldBe at("2026-01-01T00:35:00Z")
  }

  test("match day-of-week schedules with 0 and 7 as Sunday") {
    // 2026-01-01 is a Thursday; the next Sunday is 2026-01-04
    CronSchedule.parse("0 0 * * 0").nextAfter(at("2026-01-01T00:00:00Z")) shouldBe
      at("2026-01-04T00:00:00Z")
    CronSchedule.parse("0 0 * * 7").nextAfter(at("2026-01-01T00:00:00Z")) shouldBe
      at("2026-01-04T00:00:00Z")
  }

  test("match either day field when both are restricted") {
    // Standard cron: day-of-month 1 OR Monday. From Jan 2, Monday Jan 5 comes before Feb 1
    val cron = CronSchedule.parse("0 0 1 * 1")
    cron.nextAfter(at("2026-01-02T00:00:00Z")) shouldBe at("2026-01-05T00:00:00Z")
    // From Jan 27 (after Monday Jan 26), day-of-month 1 (Feb 1, a Sunday) comes first
    cron.nextAfter(at("2026-01-27T00:00:00Z")) shouldBe at("2026-02-01T00:00:00Z")
  }

  test("evaluate schedules in the given time zone") {
    val cron  = CronSchedule.parse("0 2 * * *")
    val tokyo = ZoneId.of("Asia/Tokyo")
    val next  = cron.nextAfter(ZonedDateTime.of(2026, 1, 1, 0, 0, 0, 0, tokyo))
    next shouldBe ZonedDateTime.of(2026, 1, 1, 2, 0, 0, 0, tokyo)
  }

  test("reject malformed cron expressions") {
    List("0 2 * *", "x * * * *", "99 * * * *", "* 30-2 * * *", "*/0 * * * *").foreach { expr =>
      val e = intercept[WvletLangException] {
        CronSchedule.parse(expr)
      }
      e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
  }

  test("report schedules that can never fire") {
    // February 30 does not exist
    val e = intercept[WvletLangException] {
      CronSchedule.parse("0 0 30 2 *").nextAfter(at("2026-01-01T00:00:00Z"))
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("find the most recent fire at or before a time") {
    val cron = CronSchedule.parse("0 2 * * *")
    cron.prevAtOrBefore(at("2026-01-01T10:00:00Z")) shouldBe at("2026-01-01T02:00:00Z")
    // A time before today's fire returns yesterday's fire
    cron.prevAtOrBefore(at("2026-01-01T01:59:00Z")) shouldBe at("2025-12-31T02:00:00Z")
    // An exact fire time returns itself (seconds are truncated to the minute)
    cron.prevAtOrBefore(at("2026-01-01T02:00:45Z")) shouldBe at("2026-01-01T02:00:00Z")
    // Month-restricted schedules skip back across months
    CronSchedule.parse("30 6 15 3 *").prevAtOrBefore(at("2026-01-10T00:00:00Z")) shouldBe
      at("2025-03-15T06:30:00Z")
  }

  test("report schedules that never fired in the past") {
    val e = intercept[WvletLangException] {
      CronSchedule.parse("0 0 30 2 *").prevAtOrBefore(at("2026-01-01T00:00:00Z"))
    }
    e.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

end CronScheduleTest

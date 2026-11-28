package wvlet.lang.server

import wvlet.uni.test.UniTest

import java.time.Instant

class QueryIdGeneratorTest extends UniTest:

  private val fixedTime = Instant.parse("2026-11-27T15:54:37.123Z")

  test("format ids as UTC time, sequence, and server id") {
    val gen = QueryIdGenerator(serverId = "x7k2p", clock = () => fixedTime)
    gen.next() shouldBe "20261127_155437_00000_x7k2p"
    gen.next() shouldBe "20261127_155437_00001_x7k2p"
  }

  test("make neighboring ids differ in the sequence") {
    val gen = QueryIdGenerator(clock = () => fixedTime)
    val ids = (0 until 3).map(_ => gen.next())
    ids.distinct.size shouldBe 3
    ids.map(_.split("_")(2)) shouldBe Seq("00000", "00001", "00002")
    ids.forall(QueryIdGenerator.isQueryId) shouldBe true
  }

  test("issue different ids from two server instances at the same time") {
    val a = QueryIdGenerator(serverId = "aaaaa", clock = () => fixedTime)
    val b = QueryIdGenerator(serverId = "bbbbb", clock = () => fixedTime)
    (a.next() == b.next()) shouldBe false
  }

  test("pick valid random server ids") {
    val ids = (0 until 20).map(_ => QueryIdGenerator.randomServerId())
    ids.forall(QueryIdGenerator.isServerId) shouldBe true
    (ids.distinct.size > 1) shouldBe true
  }

  test("reject malformed ids") {
    QueryIdGenerator.isQueryId("01JDQ3W5S7X9Y2Z4A6B8C0D1E2") shouldBe false
    QueryIdGenerator.isQueryId("20261127_155437_42_x7k2p") shouldBe false
    QueryIdGenerator.isServerId("X7K2P") shouldBe false
    intercept[IllegalArgumentException] {
      QueryIdGenerator(serverId = "bad")
    }
  }

end QueryIdGeneratorTest

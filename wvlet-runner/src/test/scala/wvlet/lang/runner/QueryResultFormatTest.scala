package wvlet.lang.runner

import wvlet.airspec.AirSpec

class QueryResultFormatTest extends AirSpec:
  test("unicode width") {
    for ch <- 'a' to 'Z' do
      QueryResultFormat.wcWidth(ch) shouldBe 1
    QueryResultFormat.wcWidth('東') shouldBe 2
  }

  test("truncate long strings") {
    QueryResultFormat.trimToWidth("hello", 3) shouldBe "he…"
    QueryResultFormat.trimToWidth("hello", 10) shouldBe "hello"
  }

  test("fit to width unicode strings") {
    QueryResultFormat.wcWidth("東京都") shouldBe 6
    QueryResultFormat.trimToWidth("東京都", 5) shouldBe "東京…"
    QueryResultFormat.trimToWidth("東京都", 10) shouldBe "東京都"
    QueryResultFormat.trimToWidth("東京都", 6) shouldBe "東京都"
  }

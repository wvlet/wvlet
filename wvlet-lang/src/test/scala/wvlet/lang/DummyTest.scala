package wvlet.lang

import wvlet.airspec.AirSpec

/**
  * Dummy test for PR verification
  */
class DummyTest extends AirSpec:

  test("dummy test should pass") {
    1 + 1 shouldBe 2
  }

  test("string concatenation") {
    val result = "Hello" + " " + "World"
    result shouldBe "Hello World"
  }

  test("list operations") {
    val numbers = List(1, 2, 3, 4, 5)
    numbers.size shouldBe 5
    numbers.sum shouldBe 15
  }
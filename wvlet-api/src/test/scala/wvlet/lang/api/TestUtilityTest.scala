package wvlet.lang.api

import wvlet.airspec.AirSpec

class TestUtilityTest extends AirSpec:

  test("formatMessage should combine prefix and message") {
    TestUtility.formatMessage("INFO", "Hello World") shouldBe "INFO: Hello World"
    TestUtility.formatMessage("ERROR", "Something went wrong") shouldBe "ERROR: Something went wrong"
  }

  test("isPalindrome should detect palindromes") {
    TestUtility.isPalindrome("racecar") shouldBe true
    TestUtility.isPalindrome("A man a plan a canal Panama") shouldBe true
    TestUtility.isPalindrome("hello") shouldBe false
    TestUtility.isPalindrome("12321") shouldBe true
  }

  test("factorial should calculate correctly") {
    TestUtility.factorial(0) shouldBe 1
    TestUtility.factorial(1) shouldBe 1
    TestUtility.factorial(5) shouldBe 120
    TestUtility.factorial(10) shouldBe 3628800
  }
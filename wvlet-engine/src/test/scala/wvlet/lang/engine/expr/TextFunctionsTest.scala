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

import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

/**
  * String functions on plain values; results against DuckDB are checked in EngineStdlibStringTest
  */
class TextFunctionsTest extends UniTest:

  test("should translate supplementary characters as single characters") {
    TextFunctions.translate("a😀b", "😀b", "x") shouldBe "ax"
    TextFunctions.translate("abc", "cba", "xyz") shouldBe "zyx"
  }

  test("should round-trip URL encoding of non-ASCII text") {
    TextFunctions.urlEncode("a b/日") shouldBe "a%20b%2F%E6%97%A5"
    TextFunctions.urlDecode("a%20b%2F%E6%97%A5") shouldBe "a b/日"
    TextFunctions.urlDecode("%") shouldBe "%"
  }

  test("should fail when URL-decoded bytes are not UTF-8") {
    intercept[WvletLangException] {
      TextFunctions.urlDecode("%e6")
    }.getMessage shouldContain "not valid UTF-8"
  }

  test("should require non-empty strings of the same length for hamming") {
    TextFunctions.hamming("日本", "日a") shouldBe 1L
    intercept[WvletLangException] {
      TextFunctions.hamming("ab", "abc")
    }.getMessage shouldContain "same length"
    intercept[WvletLangException] {
      TextFunctions.hamming("", "")
    }.getMessage shouldContain "must not be empty"
  }

  test("should compute Jaro-Winkler similarity") {
    TextFunctions.jaroWinkler("abc", "abc") shouldBe 1.0
    TextFunctions.jaroWinkler("", "") shouldBe 0.0
    TextFunctions.jaroWinkler("ab", "ba") shouldBe 0.0
  }

  test("should reject a missing group in regexp_extract_all") {
    intercept[WvletLangException] {
      TextFunctions.regexpExtractAll("abc", java.util.regex.Pattern.compile("b"), 3)
    }.getMessage shouldContain "no group 3"
  }

  test("should split around regex matches, keeping empty strings") {
    val comma = java.util.regex.Pattern.compile(",")
    TextFunctions.regexpSplit("a,,b,", comma) shouldBe Seq("a", "", "b", "")
    TextFunctions.regexpSplit("abc", java.util.regex.Pattern.compile("")) shouldBe
      Seq("a", "b", "c")
  }

  test("should normalize to NFC") {
    TextFunctions.nfcNormalize("abc") shouldBe "abc"
  }

end TextFunctionsTest

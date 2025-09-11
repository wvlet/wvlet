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
package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec

class KeywordClassificationTest extends AirSpec:

  test("SqlToken keyword classification should be mutually exclusive") {
    // Test reserved keywords
    val reservedKeyword = SqlToken.SELECT
    reservedKeyword.isKeyword shouldBe true
    reservedKeyword.isReservedKeyword shouldBe true
    reservedKeyword.isNonReservedKeyword shouldBe false

    // Test non-reserved keywords
    val nonReservedKeyword = SqlToken.IF // IF is in nonReservedKeywords set
    nonReservedKeyword.isKeyword shouldBe true
    nonReservedKeyword.isReservedKeyword shouldBe false
    nonReservedKeyword.isNonReservedKeyword shouldBe true

    // Test non-keywords
    val identifier = SqlToken.IDENTIFIER
    identifier.isKeyword shouldBe false
    identifier.isReservedKeyword shouldBe false
    identifier.isNonReservedKeyword shouldBe false
  }

  test("WvletToken keyword classification should be mutually exclusive") {
    // Test reserved keywords
    val reservedKeyword = WvletToken.SELECT
    reservedKeyword.isKeyword shouldBe true
    reservedKeyword.isReservedKeyword shouldBe true
    reservedKeyword.isNonReservedKeyword shouldBe false

    // Test non-reserved keywords
    val nonReservedKeyword = WvletToken.COUNT // COUNT is in nonReservedKeywords set
    nonReservedKeyword.isKeyword shouldBe true
    nonReservedKeyword.isReservedKeyword shouldBe false
    nonReservedKeyword.isNonReservedKeyword shouldBe true

    // Test non-keywords
    val identifier = WvletToken.IDENTIFIER
    identifier.isKeyword shouldBe false
    identifier.isReservedKeyword shouldBe false
    identifier.isNonReservedKeyword shouldBe false
  }

  test("should verify all keywords are either reserved or non-reserved") {
    val allSqlKeywords = SqlToken.values.filter(_.isKeyword)

    allSqlKeywords.foreach { token =>
      // Each keyword should be either reserved XOR non-reserved, but not both
      val isReserved    = token.isReservedKeyword
      val isNonReserved = token.isNonReservedKeyword

      if isReserved && isNonReserved then
        fail(s"SqlToken ${token} is both reserved and non-reserved")
      else if !isReserved && !isNonReserved then
        fail(s"SqlToken ${token} is neither reserved nor non-reserved")
    }
  }

  test("should verify SqlToken nonReservedKeywords set matches isNonReservedKeyword") {
    SqlToken
      .nonReservedKeywords
      .foreach { token =>
        token.isNonReservedKeyword shouldBe true
        token.isReservedKeyword shouldBe false
        token.isKeyword shouldBe true
      }
  }

  test("should verify WvletToken nonReservedKeywords set matches isNonReservedKeyword") {
    WvletToken
      .nonReservedKeywords
      .foreach { token =>
        token.isNonReservedKeyword shouldBe true
        token.isReservedKeyword shouldBe false
        token.isKeyword shouldBe true
      }
  }

end KeywordClassificationTest

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

import wvlet.lang.compiler.SourceFile
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class WvletScannerTest extends AirSpec:
  test("scan text") {
    val src     = SourceFile.fromWvletString("from A")
    val scanner = WvletScanner(src)
    val token   = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.FROM
    token.offset shouldBe 0
    token.str shouldBe "from"
    token.length shouldBe 4

    val token2 = scanner.nextToken()
    debug(token2)
    token2.token shouldBe WvletToken.IDENTIFIER
    token2.offset shouldBe 5
    token2.str shouldBe "A"
    token2.length shouldBe 1

    val token3 = scanner.nextToken()
    debug(token3)
    token3.token shouldBe WvletToken.EOF
    token3.offset shouldBe 6
    token3.str shouldBe ""
    token3.length shouldBe 0
  }

  inline def testScanToken(txt: String, expectedToken: WvletToken): Unit =
    test(s"scan ${txt}") {
      val src     = SourceFile.fromWvletString(txt)
      val scanner = WvletScanner(src)
      val token   = scanner.nextToken()
      debug(token)
      token.token shouldBe expectedToken
      token.offset shouldBe 0
      token.str shouldBe expectedToken.str
      token.length shouldBe expectedToken.str.length

      val token2 = scanner.nextToken()
      debug(token2)
      token2.token shouldBe WvletToken.EOF
      token2.offset shouldBe token.length
      token2.str shouldBe ""
      token2.length shouldBe 0
    }

  WvletToken
    .allKeywordAndSymbol
    .foreach: t =>
      testScanToken(t.str, t)

  test("read comments") {
    val src =
      """-- line comment
        |from A""".stripMargin
    val scanner = WvletScanner(SourceFile.fromWvletString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.COMMENT
    token.str shouldBe "-- line comment"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.EOF
  }

  test("read block comment") {
    val src =
      """/* block comment */
        |from A""".stripMargin
    val scanner = WvletScanner(SourceFile.fromWvletString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.COMMENT
    token.str shouldBe "/* block comment */"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.EOF
    token.offset shouldBe src.length
    token.length shouldBe 0
  }

  test("expr in string interpolation") {
    val src = "from sql\"select ${x}\""
    //             0    5  8 9      16
    val scanner = WvletScanner(SourceFile.fromWvletString(src))

    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.FROM

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.STRING_INTERPOLATION_PREFIX
    token.str shouldBe "sql"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.STRING_PART

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.DOLLAR

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.L_BRACE

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.IDENTIFIER

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.R_BRACE

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.STRING_PART
    token.length shouldBe 1
    token.offset shouldBe 20

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.EOF
    token.offset shouldBe src.length
    token.length shouldBe 0
  }

  test("open expr in string interpolation") {
    val src     = "from sql\"select ${x}"
    val scanner = WvletScanner(SourceFile.fromWvletString(src))

    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.FROM

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.STRING_INTERPOLATION_PREFIX

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.STRING_PART

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.DOLLAR

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.L_BRACE

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.IDENTIFIER

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe WvletToken.R_BRACE

  }

end WvletScannerTest

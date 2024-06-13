package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.SourceFile
import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

class ScannerTest extends AirSpec:
  test("scan text") {
    val src     = SourceFile.fromString("from A")
    val scanner = FlowScanner(src)
    val token   = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.FROM
    token.offset shouldBe 0
    token.str shouldBe "from"
    token.length shouldBe 4

    val token2 = scanner.nextToken()
    debug(token2)
    token2.token shouldBe FlowToken.IDENTIFIER
    token2.offset shouldBe 5
    token2.str shouldBe "A"
    token2.length shouldBe 1

    val token3 = scanner.nextToken()
    debug(token3)
    token3.token shouldBe FlowToken.EOF
    token3.offset shouldBe 6
    token3.str shouldBe ""
    token3.length shouldBe 0
  }

  inline def testScanToken(txt: String, expectedToken: FlowToken): Unit =
    test(s"scan ${txt}") {
      val src     = SourceFile.fromString(txt)
      val scanner = FlowScanner(src)
      val token   = scanner.nextToken()
      debug(token)
      token.token shouldBe expectedToken
      token.offset shouldBe 0
      token.str shouldBe expectedToken.str
      token.length shouldBe expectedToken.str.length

      val token2 = scanner.nextToken()
      debug(token2)
      token2.token shouldBe FlowToken.EOF
      token2.offset shouldBe token.length
      token2.str shouldBe ""
      token2.length shouldBe 0
    }

  FlowToken.allKeywordAndSymbol.foreach: t =>
    testScanToken(t.str, t)

  test("read comments") {
    val src =
      """-- line comment
        |from A""".stripMargin
    val scanner = FlowScanner(SourceFile.fromString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.COMMENT
    token.str shouldBe "-- line comment"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.EOF
  }

  test("read block comment") {
    val src =
      """/* block comment */
        |from A""".stripMargin
    val scanner = FlowScanner(SourceFile.fromString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.COMMENT
    token.str shouldBe "/* block comment */"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe FlowToken.EOF
  }

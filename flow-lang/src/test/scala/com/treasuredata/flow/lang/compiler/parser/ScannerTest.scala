package com.treasuredata.flow.lang.compiler.parser

import wvlet.airspec.AirSpec

class ScannerTest extends AirSpec:
  test("scan text") {
    val src     = ScannerSource.fromText("from A")
    val scanner = Scanner(src)
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

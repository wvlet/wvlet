package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.SourceFile

class SqlScannerTest extends AirSpec:
  inline def testScanToken(txt: String, expectedToken: SqlToken): Unit =
    test(s"scan ${txt}") {
      val src     = SourceFile.fromWvletString(txt)
      val scanner = SqlScanner(src)
      val token   = scanner.nextToken()
      debug(token)
      token.token shouldBe expectedToken
      token.offset shouldBe 0
      token.str shouldBe expectedToken.str
      token.length shouldBe expectedToken.str.length

      val token2 = scanner.nextToken()
      debug(token2)
      token2.token shouldBe SqlToken.EOF
      token2.offset shouldBe token.length
      token2.str shouldBe ""
      token2.length shouldBe 0
    }

  SqlToken
    .allKeywordsAndSymbols
    .foreach: t =>
      testScanToken(t.str, t)

  test("read comment") {
    val src =
      """-- line comment
        |from A""".stripMargin
    val scanner = SqlScanner(SourceFile.fromWvletString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.COMMENT
    token.str shouldBe "-- line comment"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
  }

  test("read block comment") {
    val src =
      """/* block comment */
        |from A""".stripMargin
    val scanner = SqlScanner(SourceFile.fromWvletString(src))
    var token   = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.COMMENT
    token.str shouldBe "/* block comment */"

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.FROM
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.IDENTIFIER
    token.str shouldBe "A"
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
    token.offset shouldBe src.length
    token.length shouldBe 0
  }

  test("scan >> as two GT tokens") {
    val src     = ">>"
    val scanner = SqlScanner(SourceFile.fromWvletString(src))

    // First token should be GT
    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"
    token.offset shouldBe 0
    token.length shouldBe 1

    // Second token should also be GT
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"
    token.offset shouldBe 1
    token.length shouldBe 1

    // Third token should be EOF
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
  }

  test("scan >>> as three GT tokens") {
    val src     = ">>>"
    val scanner = SqlScanner(SourceFile.fromWvletString(src))

    // First GT
    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"

    // Second GT
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"

    // Third GT
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"

    // EOF
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
  }

  test("scan nested array type array<array<int>>") {
    val src     = "array<array<int>>"
    val scanner = SqlScanner(SourceFile.fromWvletString(src))

    // array
    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.ARRAY
    token.str shouldBe "array"

    // <
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.LT
    token.str shouldBe "<"

    // array
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.ARRAY
    token.str shouldBe "array"

    // <
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.LT
    token.str shouldBe "<"

    // int
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.IDENTIFIER
    token.str shouldBe "int"

    // First >
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"

    // Second >
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GT
    token.str shouldBe ">"

    // EOF
    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
  }

  test("ensure >= still works as single token") {
    val src     = ">="
    val scanner = SqlScanner(SourceFile.fromWvletString(src))

    var token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.GTEQ
    token.str shouldBe ">="

    token = scanner.nextToken()
    debug(token)
    token.token shouldBe SqlToken.EOF
  }

end SqlScannerTest

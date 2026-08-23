package wvlet.lang.compiler.analyzer.trino

import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

/**
  * Cross-platform tests for the Trino `Authorization` header: bearer tokens, HTTPS-gated basic
  * auth, and the token/password exclusivity rule. Pure header construction — no server needed, so
  * the same behavior is verified on JVM, JS, and Native.
  */
class TrinoAuthTest extends UniTest:

  private val base = TrinoConfig(host = "localhost", user = "alice")

  test("send no Authorization header without credentials") {
    Trino.authorizationHeader(base) shouldBe None
  }

  test("send a bearer token as Authorization: Bearer") {
    Trino.authorizationHeader(base.withToken("jwt-abc")) shouldBe Some("Bearer jwt-abc")
  }

  test("encode password auth as Authorization: Basic over HTTPS") {
    val header = Trino.authorizationHeader(base.withHttps().withPassword("secret"))
    // base64("alice:secret")
    header shouldBe Some("Basic YWxpY2U6c2VjcmV0")
  }

  test("reject password auth over insecure connections") {
    val err = intercept[WvletLangException] {
      Trino.authorizationHeader(base.withPassword("secret"))
    }
    err.getMessage shouldContain "HTTPS"
  }

  test("reject setting both token and password") {
    val err = intercept[WvletLangException] {
      Trino.authorizationHeader(base.withHttps().withPassword("secret").withToken("jwt"))
    }
    err.getMessage shouldContain "not both"
  }

end TrinoAuthTest

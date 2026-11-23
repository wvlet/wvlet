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
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * Encoding functions on constants; results against DuckDB are checked in EngineStdlibEncodingTest
  */
class EncodingFunctionsTest extends UniTest:

  private def call(name: String, args: Any*): Any =
    val bound = args.map {
      case s: String =>
        Const(s, PType.Str)
      case l: Long =>
        Const(l, PType.Int64)
      case null =>
        Const(null, PType.Str)
      case other =>
        fail(s"Unexpected argument: ${other}")
    }
    ScalarFunctions.lookup(name, bound).get.eval(Batch.ofRows(1)).get(0)

  test("should compute SHA-1 and SHA-256 test vectors") {
    call("sha1", "abc") shouldBe "a9993e364706816aba3e25717850c26c9cd0d89d"
    call("sha1", "") shouldBe "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    call("sha256", "abc") shouldBe
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    call("sha256", "") shouldBe "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    // Two 64-byte blocks after padding
    call("sha256", "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq") shouldBe
      "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
  }

  test("should round-trip base64 and hex over UTF-8 bytes") {
    call("to_base64", "日本") shouldBe "5pel5pys"
    call("from_base64", "5pel5pys") shouldBe "日本"
    call("hex", "wv") shouldBe "7776"
    call("unhex", "7776") shouldBe "wv"
    call("unhex", "777") shouldBe "\u0007w"
  }

  test("should fail on invalid base64 or hex input") {
    intercept[WvletLangException] {
      call("from_base64", "not base64!")
    }.getMessage shouldContain "Invalid base64"
    intercept[WvletLangException] {
      call("unhex", "zz")
    }.getMessage shouldContain "Invalid hex"
  }

  test("should fail when the decoded bytes are not UTF-8") {
    intercept[WvletLangException] {
      call("unhex", "ff")
    }.getMessage shouldContain "not valid UTF-8"
  }

  test("should reject non-string arguments of encoding functions") {
    intercept[WvletLangException] {
      call("hex", 255L)
    }.getMessage shouldContain "pass a string"
  }

  test("should generate version-4 UUID strings") {
    val u = call("uuid").asInstanceOf[String]
    u shouldMatch {
      case s: String
          if s.matches("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}") =>
    }
    call("uuid") shouldNotBe u
  }

end EncodingFunctionsTest

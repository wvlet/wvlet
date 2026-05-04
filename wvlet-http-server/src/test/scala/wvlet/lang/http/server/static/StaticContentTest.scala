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
package wvlet.lang.http.server.static

import wvlet.uni.http.HttpStatus
import wvlet.uni.test.UniTest

/**
  * Smoke test for the static-content helper. Exercises the StaticContent helper directly via
  * classpath lookup; Netty-backed integration is covered by uni-netty.
  */
class StaticContentTest extends UniTest:

  // Resolve via classpath so the test runs from any working directory and
  // doesn't fail on Windows checkouts where text fixtures may be CRLF-converted.
  private val content = StaticContent.fromResource("static")

  test("serve a known file") {
    val resp = content("hello.txt")
    resp.status shouldBe HttpStatus.Ok_200
    // Normalize line endings so a Windows checkout with autocrlf doesn't break the assertion.
    val body = resp.contentAsString.getOrElse("").replace("\r\n", "\n")
    body shouldBe "hello\n"
    resp.header("Content-Type") shouldBe Some("text/plain")
  }

  test("404 on missing file") {
    val resp = content("does-not-exist")
    resp.status shouldBe HttpStatus.NotFound_404
  }

  test("403 on path traversal") {
    content("../../etc/passwd").status shouldBe HttpStatus.Forbidden_403
    content("/etc/passwd").status shouldBe HttpStatus.Forbidden_403
    content("foo//bar").status shouldBe HttpStatus.Forbidden_403
  }

  // Pin the NUL-byte rejection. The implementation uses indexOf(0) so the
  // source itself stays ASCII; constructing the NUL char at runtime here
  // keeps the test file ASCII too.
  test("403 on path with embedded NUL byte") {
    val nul = 0.toChar.toString
    content(s"hello${nul}.txt").status shouldBe HttpStatus.Forbidden_403
  }

  // Match airframe Resource.find semantics: leading slashes on the basePath
  // and empty basePath should still resolve files relative to the classpath
  // root. ClassLoader.getResource rejects leading slashes, so the helper has
  // to normalize.
  test("normalize basePath with leading slash") {
    StaticContent.fromResource("/static")("hello.txt").status shouldBe HttpStatus.Ok_200
  }

  test("empty basePath resolves classpath-rooted relative path") {
    StaticContent.fromResource("")("static/hello.txt").status shouldBe HttpStatus.Ok_200
  }

end StaticContentTest

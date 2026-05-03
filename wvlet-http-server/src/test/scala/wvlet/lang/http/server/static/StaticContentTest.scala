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

import wvlet.airframe.http.HttpStatus
import wvlet.airspec.AirSpec

/**
  * Unit-level smoke test for the verbatim port from airframe (#1662 phase 1a). Exercises the helper
  * directly — Netty-backed integration tests live in the airframe source and are deferred until
  * later phases of #1662.
  */
class StaticContentTest extends AirSpec:

  private val content = StaticContent.fromDirectory("./wvlet-http-server/src/test/resources/static")

  test("serve a known file") {
    val resp = content("hello.txt")
    resp.status shouldBe HttpStatus.Ok_200
    resp.contentString shouldBe "hello\n"
    resp.contentType shouldBe Some("text/plain")
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

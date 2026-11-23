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
package wvlet.lang.engine

import wvlet.uni.test.UniTest

/** Stdlib encoding and hashing functions on the engine match DuckDB, NULLs and edges included */
class EngineStdlibEncodingTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  private val strings = "from [['wvlet'], [''], ['日本語 ✓'], ['a,b;c'], [null]] as t(s)"

  test("digests match DuckDB") {
    check(s"${strings}\nselect s, s.md5 as m, s.sha1 as s1, s.sha256 as s2")
  }

  test("digests of multi-block inputs match DuckDB") {
    check(
      "from [[55], [56], [64], [119], [1000]] as t(n)\n" +
        "select 'x'.lpad(n, 'ab') as s\nselect s.length as n, s.sha1 as s1, s.sha256 as s2"
    )
  }

  test("base64 and hex round-trip match DuckDB") {
    check(
      s"${strings}\nselect s, s.to_base64 as b, s.to_base64.from_base64 as rb, " +
        "s.to_hex as h, s.to_hex.from_hex as rh"
    )
  }

  test("from_hex accepts either case and odd lengths as DuckDB") {
    check(
      "from [['776C'], ['776c'], ['F'], ['141'], [''], [null]] as t(s)\nselect s, s.from_hex as v"
    )
  }

  test("from_base64 decodes padded input as DuckDB") {
    check(
      "from [['d3ZsZXQ='], ['YQ=='], ['YWI='], [''], [null]] as t(s)\nselect s, s.from_base64 as v"
    )
  }

  test("uuid() returns version-4 UUID strings") {
    check(
      "from [[1], [2], [3]] as t(x)\nselect uuid() as u\n" +
        "select u.length as n, u.regexp_extract('^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') = u as v\n" +
        "group by n, v\nagg _.count as c"
    )
  }

end EngineStdlibEncodingTest

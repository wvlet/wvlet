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

/** Stdlib numeric casts on the engine match DuckDB, and the engine's own casts follow DuckDB's */
class EngineStdlibCastTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  private val values =
    "from [[2.5], [-2.5], [2.7], [-2.7], [3.5], [0.5], [0.0], [123456.789], [null]] as casts(x)\n"

  test("to_int and to_long truncate decimals and doubles as DuckDB") {
    check(values + "select x, x.to_int as i, x.to_long as l")
    check(values + "select x, x.to_double.to_int as i, x.to_double.to_long as l")
  }

  test("numeric to_boolean is true for non-zero values as DuckDB") {
    check(values + "select x, x.to_boolean as b, x.to_double.to_boolean as d")
    check(
      "from [[0], [1], [-3], [null]] as casts(x)\n" +
        "select x, x.to_boolean as b, x.to_long.to_boolean as l"
    )
  }

  test("plain casts round doubles half to even and decimals half away from zero as DuckDB") {
    check(values + "select x, x::bigint as l, x.to_double::bigint as d, x.to_double::int as i")
  }

end EngineStdlibCastTest

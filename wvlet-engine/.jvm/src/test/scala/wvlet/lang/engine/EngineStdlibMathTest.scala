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

/** Stdlib math functions on the engine match DuckDB, NULLs and domain edges included */
class EngineStdlibMathTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  private val doubles =
    "from [[0.0], [0.5], [1.0], [1.5], [-1.5], [2.0], [null]] as t(d)\nselect d.to_double as x"

  test("hyperbolic functions match DuckDB") {
    check(s"${doubles}\nselect x, x.sinh as s, x.cosh as c, x.tanh as t")
  }

  test("inverse hyperbolic functions and cot match DuckDB, NaN and infinity included") {
    check(s"${doubles}\nselect x, x.asinh as a, x.acosh as b, x.atanh as c, x.cot as d")
  }

  test("inverse hyperbolic functions stay precise for tiny and huge inputs") {
    check(
      "from [['1e-20'], ['-1e-20'], ['1e200'], ['-1e8'], ['0.999999']] as t(s)\n" +
        "select s.to_double as x\nselect x, x.asinh as a, x.acosh as b, x.atanh as c"
    )
  }

  test("hyperbolic functions apply to integers") {
    check("from [[0], [3], [-2], [null]] as t(i)\nselect i, i.sinh as s, i.asinh as a, i.cot as c")
  }

  test("round_half_even matches DuckDB on doubles, ties included") {
    check(
      // Exponent literals (2.5e0) are decimals in Wvlet but doubles in DuckDB SQL: cast explicitly
      "from [[2.5], [3.5], [-2.5], [2.675], [1.015], [0.125], [null]] as t(d)\n" +
        "select d.to_double as x\n" +
        "select x, x.round_half_even as r0, x.round_half_even(2) as r2, x.round(2) as h2"
    )
  }

  test("round_half_even matches DuckDB on decimals") {
    check(
      "from [[1.25], [1.35], [-1.25], [26.75]] as t(d)\n" +
        "select d, d.round_half_even(1) as r, d.round_half_even(-1) as n, d.round(-1) as h"
    )
  }

  test("rounding integers with negative digits matches DuckDB") {
    check(
      "from [[25], [35], [-25], [1234]] as t(i)\n" +
        "select i, i.round(-1) as r, i.round_half_even(-1) as e, i.round_half_even(-2) as h"
    )
  }

  test("gcd and lcm match DuckDB on ints and longs") {
    check(
      "from [[12, 18], [-12, 18], [0, 0], [4, 6], [0, 5], [7, null]] as t(a, b)\n" +
        "select a, b, a.gcd(b) as g, a.lcm(b) as l, a.to_long.gcd(b) as gl, a.to_long.lcm(b) as ll"
    )
  }

  test("factorial matches DuckDB up to 20") {
    check(
      "from [[0], [1], [5], [20], [null]] as t(n)\nselect n, n.factorial as f, n.to_long.factorial as fl"
    )
  }

end EngineStdlibMathTest

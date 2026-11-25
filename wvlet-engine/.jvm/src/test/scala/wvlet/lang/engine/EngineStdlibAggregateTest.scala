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

/** Stdlib aggregate functions on the engine match DuckDB, NULLs and degenerate groups included */
class EngineStdlibAggregateTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  // Groups: a regular sample with NULLs, a single pair, constant x, constant y, all NULL
  private val pairs =
    "from [" +
      "['a', 1, 2], ['a', 2, 9], ['a', 3, 5], ['a', 4, 11], ['a', null, 1], ['a', 5, null], " +
      "['b', 1, 2], " + "['c', 1, 2], ['c', 1, 3], ['c', 1, 7], " +
      "['d', 1, 2], ['d', 2, 2], ['d', 3, 2], " + "['e', null, null]] as t(g, xi, yi)\n" +
      "select g, xi.to_double as x, yi.to_double as y"

  test("corr and covariances match DuckDB") {
    check(s"${pairs}\ngroup by g\nagg y.corr(x) as r, y.covar_pop(x) as cp, y.covar_samp(x) as cs")
  }

  test("linear regression aggregates match DuckDB") {
    check(
      s"${pairs}\ngroup by g\n" +
        "agg y.regr_slope(x) as s, y.regr_intercept(x) as i, y.regr_r2(x) as r2, y.regr_count(x) as n"
    )
  }

  test("map_agg skips NULL keys and keeps NULL values as DuckDB") {
    check(
      "from [['a', 'k1', 1], ['a', 'k2', null], ['a', null, 3], ['b', null, 4], ['c', 'k1', 5]] " +
        "as kv(g, k, v)\ngroup by g\nagg k.map_agg(v) as m"
    )
    check(
      "from [[1, 'x'], [2, 'y'], [null, 'z']] as kv(k, v)\ngroup by 'all'\n" +
        "agg k.map_agg(v) as m, k.map_agg(v).size as n"
    )
  }

  test("to_array(order_by) collects values in key order as DuckDB") {
    val rows =
      "from [['a', 'x', 3], ['a', 'y', 1], ['a', null, 2], ['a', 'z', null], ['b', 'w', 5], " +
        "['c', null, null]] as ev(g, item, ts)\n"
    check(rows + "group by g\nagg item.to_array(ts) as items, item.to_array(ts).size as n")
    // string_agg is order-sensitive too: DuckDB SQL with ORDER BY inside the call
    check(rows + "group by g\nagg sql\"string_agg(item, ',' order by ts desc)\" as s")
    // Aggregates differing only in their ORDER BY must not share one subplan
    check(
      "val ev(g, item, ts) = [['a', 'x', 3], ['a', 'y', 1], ['a', 'z', 2], ['b', 'w', 5]]\n" +
        "with l as { from ev group by g agg item.to_array(ts) as up }\n" +
        "with r as { from ev group by g agg sql\"array_agg(item order by ts desc)\" as down }\n" +
        "from l join r on l.g = r.g\nselect l.g, up, down"
    )
    check(rows + "group by g\nagg item.to_array(ts) as items, ts.to_array(item) as by_item")
  }

  test("aggregates over integer columns match DuckDB") {
    check(
      "from [[1, 2], [2, 5], [3, 4], [7, 1]] as t(x, y)\n" +
        "group by 'all'\nagg y.corr(x) as r, y.regr_slope(x) as s, x.kurtosis as k, x.skewness as sk"
    )
  }

  test("kurtosis and skewness match DuckDB, small and constant groups included") {
    check(
      "from [" + "['a', 1.0], ['a', 2.0], ['a', 3.0], ['a', 10.0], ['a', -4.0], ['a', 2.5], " +
        "['b', 1.0], ['b', 2.0], ['b', 3.0], " +
        "['c', 2.0], ['c', 2.0], ['c', 2.0], ['c', 2.0], " + "['d', null]] as t(g, v)\n" +
        "select g, v.to_double as x\ngroup by g\nagg x.kurtosis as k, x.skewness as s"
    )
  }

  test("bit_and, bit_or, and bit_xor match DuckDB on ints and longs") {
    check(
      "from [['a', 12], ['a', 10], ['a', null], ['a', 6], ['b', -3], ['b', 12], ['c', null]] as t(g, i)\n" +
        "group by g\nagg i.bit_and as a, i.bit_or as o, i.bit_xor as x, " +
        "i.to_long.bit_and as al, i.to_long.bit_xor as xl"
    )
  }

end EngineStdlibAggregateTest

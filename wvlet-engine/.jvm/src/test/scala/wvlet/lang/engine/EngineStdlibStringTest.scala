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

/** Stdlib string functions on the engine match DuckDB, NULLs and edges included */
class EngineStdlibStringTest extends UniTest:
  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private def check(query: String): Unit =
    val r = conformance.compareQuery(query)
    if r.message.startsWith("skipped") || !r.matched then
      fail(s"${r.message}\n${query}")

  test("translate maps, removes, and keeps characters as DuckDB") {
    check(
      "from [['abcabc', 'abc', 'x'], ['abc', 'aa', 'xy'], ['日本', '日', '月'], ['abc', '', 'x'], " +
        "[null, 'a', 'b'], ['abc', null, 'b']] as t(s, f, r)\nselect s, s.translate(f, r) as v"
    )
  }

  test("octet_length counts UTF-8 bytes as DuckDB") {
    check(
      "from [['abc'], [''], ['日本'], ['é'], [null]] as t(s)\nselect s, s.octet_length as n, s.length as c"
    )
  }

  test("normalize returns NFC as DuckDB") {
    check("from [['e' + 'x'], ['abc'], [''], [null]] as t(s)\n" + "select s, s.normalize as n")
  }

  test("regexp_extract_all returns every match as DuckDB") {
    check(
      "from [['a1b22c'], [''], ['abc'], [null]] as t(s)\n" +
        "select s, s.regexp_extract_all('\\d+') as d, s.regexp_extract_all('([a-z])(\\d+)', 2) as g, " +
        "s.regexp_extract_all('x*') as e, s.regexp_extract_all('(x)?b', 1) as m"
    )
  }

  test("regexp_extract returns NULL on no match as DuckDB") {
    val q =
      "from [['a1b22'], ['abc'], [''], [null]] as t(s)\n" +
        "select s, s.regexp_extract('\\d+') as d, s.regexp_extract('([a-z])(\\d+)', 2) as g, " +
        "s.regexp_extract('x*') as e, s.regexp_extract('(a)|b', 1) as p, " +
        "s.regexp_extract('\\d+') is null as none"
    check(q)
  }

  test("regexp_split keeps empty strings as DuckDB") {
    check(
      "from [['a,b,,c,'], [''], ['abc'], [',a'], [null]] as t(s)\n" +
        "select s, s.regexp_split(',') as c, s.regexp_split('') as e, s.regexp_split(',?b') as b"
    )
  }

  test("url_encode and url_decode match DuckDB") {
    check(
      "from [['a b+c~*-_.!/?é%'], [''], ['日本'], [null]] as t(s)\n" +
        "select s, s.url_encode as e, s.url_encode.url_decode as r"
    )
    check(
      "from [['a+b%20c%2b%E6%97%A5'], ['%zz'], ['%4'], ['100%'], [null]] as t(s)\nselect s, s.url_decode as d"
    )
  }

  test("hamming_distance and jaro_winkler_similarity match DuckDB") {
    check(
      "from [['abc', 'abd'], ['karolin', 'kathrin'], ['日本', '月本'], ['日本', '日a'], ['é', 'e'], ['a', 'a'], [null, 'a']] as t(a, b)\n" +
        "select a, b, a.hamming_distance(b) as h"
    )
    check(
      "from [['martha', 'marhta'], ['dixon', 'dicksonx'], ['abc', 'abc'], ['ab', 'ba'], ['', ''], " +
        "['a', ''], ['a', 'a'], ['a', 'b'], ['日本語', '日本人'], ['é', 'e'], ['Hello', 'hello'], ['crate', 'trace'], " +
        "['dwayne', 'duane'], [null, 'a']] as t(a, b)\n" +
        "select a, b, a.jaro_winkler_similarity(b) as j"
    )
  }

end EngineStdlibStringTest

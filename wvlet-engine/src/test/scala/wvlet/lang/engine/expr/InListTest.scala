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

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

class InListTest extends UniTest:
  private val a = ColRef(0, PType.Int64, "a")
  private val s = ColRef(1, PType.Str, "s")

  test("tell IN lists with and without a NULL value apart by signature") {
    InList(a, Set(1L), false, true).sig shouldNotBe InList(a, Set(1L), true, true).sig
    InList(s, Set.empty, true, false).sig shouldNotBe InList(s, Set("null"), false, false).sig
  }

  test("tell IN lists whose values join into the same text apart by signature") {
    InList(s, Set("x,y"), false, false).sig shouldNotBe InList(s, Set("x", "y"), false, false).sig
    InList(s, Set(""), false, false).sig shouldNotBe InList(s, Set.empty, false, false).sig
  }

  test("keep the signature text of plain values") {
    InList(s, Set("IN", "OH"), false, false).sig shouldBe "in(#1,IN,OH,false)"
    InList(a, Set(1L), true, true).sig shouldBe "in(#0,1,null,true)"
  }

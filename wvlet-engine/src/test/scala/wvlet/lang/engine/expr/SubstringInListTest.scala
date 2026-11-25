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

class SubstringInListTest extends UniTest:
  private val s = ColRef(0, PType.Str, "s")

  private val strings: Seq[String] = Seq(
    "13-555-0101",
    "31-555",
    "1",
    "",
    null,
    "29",
    "x13",
    // Surrogate pairs before and inside the region: code points, not chars, count
    "😀13",
    "1😀",
    "13😀"
  )

  private def substring(start: Long, len: Long): VExpr =
    ScalarFunctions
      .lookup("substring", Seq(s, Const(start, PType.Int64), Const(len, PType.Int64)))
      .get

  private def results(e: VExpr): Seq[Any] =
    val v = e.eval(
      Batch(Array[Vec](StrVec(strings.toArray, strings.map(_ == null).toArray)), strings.size)
    )
    (0 until v.size).map(v.get)

  // The region comparison agrees with building the substrings, for each NULL and NOT IN variant
  private def check(start: Long, len: Long, values: Set[Any]): Unit =
    for
      hasNull <- Seq(false, true)
      negate  <- Seq(false, true)
    do
      val plain  = InList(substring(start, len), values, hasNull, negate)
      val region = plain.copy(substringRegions = true)
      results(region) shouldBe results(plain)

  test("apply to substrings with constant start and length over string values only") {
    (SubstringInList.of(substring(1, 2), Set("13")) != null) shouldBe true
    (SubstringInList.of(substring(0, 2), Set("13")) == null) shouldBe true
    (SubstringInList.of(substring(1, 2), Set(13L)) == null) shouldBe true
    (SubstringInList.of(s, Set("13")) == null) shouldBe true
  }

  test("match substring regions as the substring function does") {
    check(1, 2, Set("13", "31", "29", "1"))
    check(2, 2, Set("13", "3-", "1", ""))
    check(1, 0, Set(""))
    check(5, 3, Set("555", "5"))
    check(20, 2, Set("", "13"))
  }

  test("look up regions among many values") {
    check(1, 2, (10 to 99).map(_.toString).toSet - "13")
    check(1, 2, (10 to 99).map(_.toString).toSet ++ Set("", "1"))
  }

  test("count code points for strings with surrogate pairs") {
    check(2, 2, Set("13"))
    check(1, 2, Set("1😀", "13"))
    check(3, 1, Set("😀"))
  }

  test("keep the signature of the plain IN list") {
    val plain = InList(substring(1, 2), Set("13"), false, false)
    plain.copy(substringRegions = true).sig shouldBe plain.sig
  }

end SubstringInListTest

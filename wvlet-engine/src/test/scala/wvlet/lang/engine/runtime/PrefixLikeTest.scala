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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.LikeExpr
import wvlet.lang.engine.vector.PType
import wvlet.uni.test.UniTest

/** Which LIKE patterns are prefix ranges */
class PrefixLikeTest extends UniTest:
  private val x = ColRef(0, PType.Str, "x")

  private def prefix(
      pattern: String,
      negate: Boolean = false,
      caseInsensitive: Boolean = false,
      escape: Option[Char] = None
  ): Option[String] = PrefixLike.prefix(LikeExpr(x, pattern, negate, caseInsensitive, escape))

  test("should read the prefix of a pattern with one trailing %") {
    prefix("forest%") shouldBe Some("forest")
    prefix("a%") shouldBe Some("a")
  }

  test("should not read a prefix from other patterns") {
    prefix("%") shouldBe None
    prefix("forest") shouldBe None
    prefix("%forest") shouldBe None
    prefix("%forest%") shouldBe None
    prefix("for%est%") shouldBe None
    prefix("for_st%") shouldBe None
    prefix("forest%%") shouldBe None
  }

  test("should not read a prefix from NOT LIKE, ILIKE, or a pattern with an escape character") {
    prefix("forest%", negate = true) shouldBe None
    prefix("forest%", caseInsensitive = true) shouldBe None
    prefix("forest!%%", escape = Some('!')) shouldBe None
  }

end PrefixLikeTest

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

  private def isPrefix(
      pattern: String,
      negate: Boolean = false,
      caseInsensitive: Boolean = false,
      escape: Option[Char] = None
  ): Boolean = PrefixLike.isPrefix(LikeExpr(x, pattern, negate, caseInsensitive, escape))

  test("should take a pattern with one trailing % as a prefix") {
    isPrefix("forest%") shouldBe true
    isPrefix("a%") shouldBe true
  }

  test("should not take other patterns as a prefix") {
    isPrefix("%") shouldBe false
    isPrefix("forest") shouldBe false
    isPrefix("%forest") shouldBe false
    isPrefix("%forest%") shouldBe false
    isPrefix("for%est%") shouldBe false
    isPrefix("for_st%") shouldBe false
    isPrefix("forest%%") shouldBe false
  }

  test("should not take NOT LIKE, ILIKE, or a pattern with an escape character as a prefix") {
    isPrefix("forest%", negate = true) shouldBe false
    isPrefix("forest%", caseInsensitive = true) shouldBe false
    isPrefix("forest!%%", escape = Some('!')) shouldBe false
  }

end PrefixLikeTest

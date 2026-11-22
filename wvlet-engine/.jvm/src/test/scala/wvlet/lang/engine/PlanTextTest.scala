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

class PlanTextTest extends UniTest:
  private val plan =
    """Scan[a] -> [x:long]
                       |↓ Filter(#0) -> [x:long]
                       |  Scan[b] -> [x:long]
                       |  ↓ Project(#0) -> [x:long]
                       |  Scan[c] -> [x:long]
                       |  ↓ Filter(#0) -> [x:long]
                       |↙ UnionAll -> [x:long]
                       |↓ Sort(#0) -> [x:long]""".stripMargin

  test("should follow the main flow, skipping nested inputs") {
    PlanText.downstream(plan, "Scan[a]") shouldBe Seq("Filter(#0)", "UnionAll", "Sort(#0)")
  }

  test("should follow a nested input to its reader, skipping the other inputs") {
    PlanText.downstream(plan, "Scan[b]") shouldBe Seq("Project(#0)", "UnionAll", "Sort(#0)")
    PlanText.downstream(plan, "Scan[c]") shouldBe Seq("Filter(#0)", "UnionAll", "Sort(#0)")
  }

  test("should return nothing for an unknown operator") {
    PlanText.downstream(plan, "Scan[d]") shouldBe Nil
  }

end PlanTextTest

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
package wvlet.lang.engine.vector

import wvlet.uni.test.UniTest

class ValueOpsTest extends UniTest:

  test("should cast integers to strings without a fraction") {
    // On Scala.js an Int is a JS number, which also matches Double
    ValueOps.cast(1, PType.Int32, PType.Str) shouldBe "1"
    ValueOps.cast(-42, PType.Int32, PType.Str) shouldBe "-42"
    ValueOps.cast(7L, PType.Int64, PType.Str) shouldBe "7"
  }

  test("should cast doubles to strings with a fraction") {
    ValueOps.cast(1.0, PType.Float64, PType.Str) shouldBe "1.0"
    ValueOps.cast(2.5, PType.Float64, PType.Str) shouldBe "2.5"
  }

  test("should format dates and timestamps by their type") {
    ValueOps.toStr(0, PType.Date) shouldBe "1970-01-01"
    ValueOps.toStr(1, PType.Int32) shouldBe "1"
  }

end ValueOpsTest

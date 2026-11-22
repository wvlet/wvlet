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

class BatchTest extends UniTest:
  private def batchOf(n: Int): Batch = Batch(
    Array(
      IntVec(PType.Int32, Array.tabulate(n)(identity), null),
      StrVec(Array.tabulate(n)(i => s"s${i}"), null)
    ),
    n
  )

  test("slice past the end should stop at the last row") {
    val b = batchOf(5000)
    val s = b.slice(4096, 8192)
    s.size shouldBe 904
    s.column(0).get(0) shouldBe 4096
    s.column(1).get(903) shouldBe "s4999"
  }

  test("slice covering the whole batch should return the batch itself") {
    val b = batchOf(10)
    b.slice(0, 4096) shouldBeTheSameInstanceAs b
  }

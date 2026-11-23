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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

class PartialGroupSizerTest extends UniTest:
  private val schema                     = Schema(IndexedSeq(Field("x", PType.Int64)))
  private def part(rows: Long): Operator = IteratorOp(schema, () => Iterator.empty, "part", rows)

  test("should cap the query estimate by the part's row count before any part finishes") {
    val s = PartialGroupSizer(50000)
    s.hint(part(1000)) shouldBe 1000
    s.hint(part(80000)) shouldBe 50000
    // A part of unknown size, e.g. the whole scan when the source has a single part
    s.hint(IteratorOp(schema, () => Iterator.empty, "scan")) shouldBe 50000
  }

  test("should size later parts by the largest group count of finished parts") {
    val s = PartialGroupSizer(50000)
    s.report(300)
    s.report(700)
    s.report(200)
    s.hint(part(80000)) shouldBe 700
    s.hint(part(500)) shouldBe 500
  }

  test("should give no hint without a query estimate") {
    val s = PartialGroupSizer(0)
    s.report(700)
    s.hint(part(80000)) shouldBe 0
  }

  test("should never exceed the pre-size cap") {
    val s = PartialGroupSizer(Int.MaxValue)
    s.hint(part(Long.MaxValue)) shouldBe GroupTable.MaxPresizeGroups
  }

  test("should report the group count of a hash aggregation") {
    val b      = Batch(Array(LongVec(PType.Int64, Array(1L, 2L, 2L, 3L, 1L), null)), 5)
    var groups = -1
    val op     = HashAggOp(
      IteratorOp(schema, () => Iterator.single(b), "in"),
      IndexedSeq(ColRef(0, PType.Int64, "x")),
      IndexedSeq.empty,
      schema,
      expectedGroups = 4,
      onGroups = n => groups = n
    )
    op.drain().map(_.size).sum shouldBe 3
    groups shouldBe 3
  }

end PartialGroupSizerTest

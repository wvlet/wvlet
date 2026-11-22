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
package wvlet.lang.engine.plan

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.parquet.ParquetFilter
import wvlet.uni.parquet.ParquetFilter.col
import wvlet.uni.test.UniTest

import java.time.LocalDate

class ScanFiltersTest extends UniTest:
  private val id   = ColRef(0, PType.Int64, "id")
  private val name = ColRef(1, PType.Str, "name")
  private val day  = ColRef(2, PType.Date, "day")
  private val cols = IndexedSeq("id", "name", "day")

  private def translate(e: VExpr): Option[ParquetFilter] = ScanFilters.toParquet(
    e,
    i => Some(cols(i))
  )

  test("should translate integer and date comparisons") {
    translate(Compare(CmpOp.Ge, id, Const(10L, PType.Int64))) shouldBe Some(col("id") >= 10L)
    // constant on the left
    translate(Compare(CmpOp.Lt, Const(10L, PType.Int64), id)) shouldBe Some(col("id") > 10L)
    translate(Compare(CmpOp.Le, day, Const(3, PType.Date))) shouldBe
      Some(col("day") <= LocalDate.ofEpochDay(3))
  }

  test("should keep the translatable side of a conjunction") {
    val e = AndExpr(
      Compare(CmpOp.Eq, id, Const(1L, PType.Int64)),
      Compare(CmpOp.Ne, id, Const(2L, PType.Int64))
    )
    translate(e) shouldBe Some(col("id") === 1L)
  }

  test("should drop a disjunction with an untranslatable side") {
    val e = OrExpr(
      Compare(CmpOp.Eq, id, Const(1L, PType.Int64)),
      Compare(CmpOp.Gt, name, Const("x", PType.Str))
    )
    translate(e) shouldBe None
  }

  test("should translate only equality for strings") {
    translate(Compare(CmpOp.Eq, name, Const("x", PType.Str))) shouldBe Some(col("name") === "x")
    translate(Compare(CmpOp.Lt, name, Const("x", PType.Str))) shouldBe None
  }

  test("should skip comparisons with NULL or a different type") {
    translate(Compare(CmpOp.Eq, id, Const(null, PType.Int64))) shouldBe None
    translate(Compare(CmpOp.Eq, id, Const(1.5, PType.Float64))) shouldBe None
  }

  test("should skip columns the source can't filter on") {
    val e = AndExpr(
      Compare(CmpOp.Eq, id, Const(1L, PType.Int64)),
      Compare(CmpOp.Eq, name, Const("x", PType.Str))
    )
    ScanFilters.toParquet(e, i => Option.when(i != 0)(cols(i))) shouldBe Some(col("name") === "x")
  }

end ScanFiltersTest

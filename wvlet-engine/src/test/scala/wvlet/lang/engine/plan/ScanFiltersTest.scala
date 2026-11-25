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

import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.cloud.parquet.ParquetFilter.col
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal
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

  test("should build a key filter from the incoming key values") {
    ScanFilters.valuesFilter("id", PType.Int64, Seq(5L, 2L, 9L)) shouldBe
      Some(col("id") >= 2L && col("id") <= 9L)
    ScanFilters.valuesFilter("n", PType.Int32, Seq(3, -1)) shouldBe
      Some(col("n") >= -1L && col("n") <= 3L)
    ScanFilters.valuesFilter("day", PType.Date, Seq(3, 1)) shouldBe
      Some(col("day") >= LocalDate.ofEpochDay(1) && col("day") <= LocalDate.ofEpochDay(3))
    // Strings only as equalities, since Parquet orders them by UTF-8 bytes
    ScanFilters.valuesFilter("name", PType.Str, Seq("a", "b", "a")) shouldBe
      Some(col("name") === "a" || col("name") === "b")
    ScanFilters.valuesFilter("name", PType.Str, (0 until 65).map(_.toString)) shouldBe None
    ScanFilters.valuesFilter("price", PType.Float64, Seq(1.0)) shouldBe None
    ScanFilters.valuesFilter("id", PType.Int64, Nil) shouldBe None
  }

  private val price = ColRef(0, PType.decimal(12, 2), "price")
  private val x     = ColRef(1, PType.Float64, "x")

  private def numeric(e: VExpr): Option[ParquetFilter] = ScanFilters.toParquet(
    e,
    i => Some(IndexedSeq("price", "x")(i)),
    numeric = true
  )

  test("should translate decimal comparisons only with numeric filters") {
    val k = Const(JBigDecimal("50.00"), price.ptype)
    numeric(Compare(CmpOp.Gt, price, k)) shouldBe Some(col("price") > JBigDecimal("50.00"))
    numeric(Compare(CmpOp.Le, k, price)) shouldBe Some(col("price") >= JBigDecimal("50.00"))
    numeric(Compare(CmpOp.Ne, price, k)) shouldBe None
    // A constant of another decimal type is not compared
    numeric(Compare(CmpOp.Gt, price, Const(JBigDecimal("50.0"), PType.decimal(3, 1)))) shouldBe None
    ScanFilters.toParquet(Compare(CmpOp.Gt, price, k), _ => Some("price")) shouldBe None
  }

  test("should translate only the floating-point comparisons NaN and -0.0 can't break") {
    def f(op: CmpOp, v: Double) = numeric(Compare(op, x, Const(v, PType.Float64)))
    f(CmpOp.Lt, 1.5) shouldBe Some(col("x") < 1.5)
    f(CmpOp.Le, 1.5) shouldBe Some(col("x") <= 1.5)
    f(CmpOp.Eq, 1.5) shouldBe Some(col("x") === 1.5)
    // NaN rows satisfy > and >= in the engine, but writers leave NaN out of the statistics
    f(CmpOp.Gt, 1.5) shouldBe None
    f(CmpOp.Ge, 1.5) shouldBe None
    f(CmpOp.Lt, Double.NaN) shouldBe None
    // -0.0 < 0.0 in the engine, and uni reads a -0.0 minimum as 0.0
    f(CmpOp.Lt, 0.0) shouldBe Some(col("x") <= 0.0)
    ScanFilters.toParquet(Compare(CmpOp.Lt, x, Const(1.5, PType.Float64)), _ => Some("x")) shouldBe
      None
  }

  test("should translate a row filter only when every part translates") {
    val eq = Compare(CmpOp.Eq, id, Const(1L, PType.Int64))
    val ne = Compare(CmpOp.Ne, id, Const(2L, PType.Int64))
    ScanFilters.exact(eq, i => Some(cols(i))) shouldBe Some(col("id") === 1L)
    // A row filter can't drop the part it doesn't express, as a row-group filter does
    ScanFilters.exact(AndExpr(eq, ne), i => Some(cols(i))) shouldBe None
    ScanFilters.exact(OrExpr(eq, IsNullExpr(name, false)), i => Some(cols(i))) shouldBe
      Some(col("id") === 1L || ParquetFilter.IsNull("name"))
    // A column the source can't filter rows by exactly
    ScanFilters.exact(eq, _ => None) shouldBe None
    // Floating-point and decimal translations are only implied (x < 0.0 becomes x <= 0.0)
    ScanFilters.exact(Compare(CmpOp.Lt, x, Const(0.0, PType.Float64)), _ => Some("x")) shouldBe None
  }

  test("should order row filters most selective first") {
    // id in [0, 999] and day in [0, 99] by their statistics
    val source =
      new MemTable("t", Schema(IndexedSeq.empty), Nil):
        override def columnRange(column: String): Option[(Long, Long)] =
          column match
            case "id" =>
              Some((0L, 999L))
            case "day" =>
              Some((0L, 99L))
            case _ =>
              None
    def conj(e: VExpr) = e -> ScanFilters.exact(e, i => Some(cols(i))).get
    val idRange        = Seq(
      conj(Compare(CmpOp.Ge, id, Const(100L, PType.Int64))),
      conj(Compare(CmpOp.Lt, id, Const(600L, PType.Int64)))
    )
    val dayEq   = conj(Compare(CmpOp.Eq, day, Const(5, PType.Date)))
    val nameEq  = conj(Compare(CmpOp.Eq, name, Const("x", PType.Str)))
    val ordered = ScanFilters.ordered(nameEq +: idRange :+ dayEq, source, cols)
    // day = 5 (0.01), then name = 'x' (0.3 by default), then the id range (0.5), kept adjacent
    ordered shouldBe (
      col("day") === LocalDate.ofEpochDay(5) && col("name") === "x" && col("id") >= 100L &&
        col("id") < 600L
    )
  }

end ScanFiltersTest

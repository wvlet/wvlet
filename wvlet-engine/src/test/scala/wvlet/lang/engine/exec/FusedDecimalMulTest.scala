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
import wvlet.lang.engine.runtime.FuseDecimalArith
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal

class FusedDecimalMulTest extends UniTest:
  private val dec   = PType.Decimal(15, 2)
  private val inner = PType.Decimal(16, 2)
  private val prod  = PType.Decimal(31, 4)
  private val prod2 = PType.Decimal(38, 6)

  private val x = ColRef(0, dec, "x")
  private val y = ColRef(1, dec, "y")
  private val z = ColRef(2, dec, "z")
  private val k = ColRef(3, PType.Int64, "k")

  private val one = Const(JBigDecimal("1.00"), inner)

  private def batch(n: Int, xs: Int => Long, nulls: Boolean = false): Batch =
    val rnd = scala.util.Random(3)
    Batch(
      Array(
        LongVec(dec, Array.tabulate(n)(xs), null),
        LongVec(
          dec,
          Array.fill(n)(rnd.nextLong(11L)),
          if nulls then
            Array.fill(n)(rnd.nextInt(5) == 0)
          else
            null
        ),
        LongVec(dec, Array.fill(n)(rnd.nextLong(9L)), null),
        LongVec(PType.Int64, Array.fill(n)(rnd.nextLong(100L)), null)
      ),
      n
    )

  private def values(v: Vec): Seq[Any] = (0 until v.size).map(v.get)

  /** e fused, after checking that it fuses */
  private def fused(e: VExpr): VExpr =
    val f = FuseDecimalArith(e)
    f.toString shouldBe e.toString
    f shouldNotBe e
    f

  private val forms = Seq(
    Arith(ArithOp.Mul, x, Arith(ArithOp.Sub, one, y, inner), prod),
    Arith(ArithOp.Mul, x, Arith(ArithOp.Add, one, y, inner), prod),
    Arith(ArithOp.Mul, x, Arith(ArithOp.Add, y, one, inner), prod),
    Arith(ArithOp.Mul, x, Arith(ArithOp.Sub, y, one, inner), prod),
    Arith(ArithOp.Mul, Arith(ArithOp.Sub, one, y, inner), x, prod),
    Arith(
      ArithOp.Mul,
      Arith(ArithOp.Mul, x, Arith(ArithOp.Sub, one, y, inner), prod),
      Arith(ArithOp.Add, one, z, inner),
      prod2
    )
  )

  test("should compute decimal products of constant sums as Arith does") {
    val b = batch(1000, i => (i * 7919L) % 10000000L - 5000000L)
    forms.foreach { e =>
      values(fused(e).eval(b)) shouldBe values(e.eval(b))
    }
  }

  test("should fuse both levels of a product of constant sums") {
    fused(forms.last) match
      case f: FusedDecimalMul =>
        f.x shouldMatch { case _: FusedDecimalMul =>
        }
      case other =>
        fail(s"Not fused: ${other}")
  }

  test("should keep NULL rows as Arith does") {
    val b = batch(500, i => i.toLong, nulls = true)
    forms.foreach { e =>
      values(fused(e).eval(b)) shouldBe values(e.eval(b))
    }
  }

  test("should compute products of operands outside the Int range exactly") {
    // x beyond the Int range: the product still fits, which the division checks
    val b = batch(300, i => (1L << 40) + i)
    forms
      .take(5)
      .foreach { e =>
        values(fused(e).eval(b)) shouldBe values(e.eval(b))
      }
  }

  test("should fall back to BigDecimal arithmetic when a product overflows a Long") {
    val b = batch(300, i => Long.MaxValue / 3 - i)
    forms.foreach { e =>
      val v = fused(e).eval(b)
      values(v) shouldBe values(e.eval(b))
    }
    val big = fused(forms(1)).eval(b).get(0).asInstanceOf[JBigDecimal]
    big.compareTo(JBigDecimal.valueOf(Long.MaxValue / 3, 2).multiply(JBigDecimal("1.00"))) >
      -1 shouldBe true
  }

  test("should read deferred operands in place as over gathered columns") {
    val src = batch(800, i => (i * 31L) % 100000L, nulls = true)
    // Every other row, and rows of NULLs (-1) as an outer join's
    val idx =
      Array.tabulate(400)(i =>
        if i % 37 == 0 then
          -1
        else
          2 * i
      )
    val taken    = src.take(idx, idx.length)
    val gathered = src.gather(idx, idx.length)
    forms.foreach { e =>
      val f = fused(e)
      values(SelectedInputs.eval(f, taken)) shouldBe values(e.eval(gathered))
    }
  }

  test("should not fuse products whose scales don't add up or without a constant") {
    val integral = Arith(ArithOp.Mul, k, Arith(ArithOp.Sub, one, y, inner), PType.Decimal(31, 2))
    FuseDecimalArith(integral) shouldBe integral
    val noConst = Arith(ArithOp.Mul, x, Arith(ArithOp.Sub, z, y, inner), prod)
    FuseDecimalArith(noConst) shouldBe noConst
  }

  test("should turn back into Arith when the inner sum is replaced") {
    val f = fused(forms.head).asInstanceOf[FusedDecimalMul]
    f.mapChildren {
      case a: Arith =>
        z
      case other =>
        other
    } shouldBe Arith(ArithOp.Mul, x, z, prod)
  }

end FusedDecimalMulTest

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
package wvlet.lang.engine.gpu

import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.DType
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.PType
import wvlet.uni.test.UniTest

import java.math.BigDecimal as JBigDecimal

/**
  * Checks that arithmetic fused into one cudf AST kernel ([[GpuAstArith]]) gives the same values
  * and types as the per-operator path, over decimals of mixed scales with negatives and NULLs.
  * Skipped without a CUDA device.
  */
class GpuAstArithTest extends UniTest:
  private def dec(p: Int, s: Int)                              = PType.decimal(p, s)
  private def arith(op: ArithOp, l: VExpr, r: VExpr, p: PType) = Arith(op, l, r, p)

  private val price = ColRef(0, dec(15, 2), "price")
  private val disc  = ColRef(1, dec(15, 2), "disc")
  private val tax   = ColRef(2, dec(15, 2), "tax")
  private val c4    = ColRef(3, dec(15, 4), "c4")
  private val f     = ColRef(4, PType.Float64, "f")
  private val one   = Const(1, PType.Int32)

  // TPC-H q1's charge: price * (1 - disc) * (1 + tax), declared in 128-bit precisions
  private val charge = arith(
    ArithOp.Mul,
    arith(ArithOp.Mul, price, arith(ArithOp.Sub, one, disc, dec(38, 2)), dec(38, 4)),
    arith(ArithOp.Add, one, tax, dec(38, 2)),
    dec(38, 6)
  )

  private val cases: Seq[(String, VExpr, Boolean)] = Seq(
    ("q1 charge", charge, true),
    (
      "mixed scales with negation",
      arith(ArithOp.Add, arith(ArithOp.Sub, price, c4, dec(38, 4)), Negate(disc), dec(38, 4)),
      true
    ),
    (
      "a scale-changing cast as a leaf",
      arith(
        ArithOp.Mul,
        arith(ArithOp.Add, CastExpr(price, dec(18, 4)), c4, dec(38, 4)),
        disc,
        dec(38, 6)
      ),
      true
    ),
    (
      "FLOAT64",
      arith(
        ArithOp.Mul,
        f,
        arith(ArithOp.Sub, f, Const(1.5, PType.Float64), PType.Float64),
        PType.Float64
      ),
      true
    ),
    ("a single operator stays per operator", arith(ArithOp.Mul, price, disc, dec(38, 4)), false)
  )

  private def show(cv: ColumnVector): Seq[String] =
    val h = cv.copyToHost()
    try (0 until cv.getRowCount.toInt).map { i =>
        if h.isNull(i) then
          "null"
        else if cv.getType == DType.FLOAT64 then
          h.getDouble(i).toString
        else
          h.getBigDecimal(i).toString
      }
    finally h.close()

  test("should match the per-operator path") {
    if !GpuRuntime.isAvailable then
      skip("No CUDA device")
    val cols = IndexedSeq(
      ColumnVector.decimalFromBoxedLongs(-2, 1234L, -500L, null, 99999L),
      ColumnVector.decimalFromBoxedLongs(-2, 6L, 10L, 5L, null),
      ColumnVector.decimalFromBoxedLongs(-2, 8L, 0L, null, 2L),
      ColumnVector.decimalFromBoxedLongs(-4, 12345L, 1L, -10000L, 7L),
      ColumnVector.fromBoxedDoubles(1.5, -2.25, null, 0.1)
    )
    try
      val bounds = IndexedSeq("999.99", "0.10", "0.08", "1.2345", "2.25").map(b =>
        Some(JBigDecimal(b))
      )
      val perOp = Bounds(bounds, narrow = true)
      val fused = Bounds(bounds, narrow = true, ast = true)
      cases.foreach { (name, e, fuses) =>
        val a = e.asInstanceOf[Arith]
        GpuAstArith.eval(a, cols, 4)(using fused).map(_.close()).isDefined shouldBe fuses
        val expected = GpuExprs.eval(e, cols, 4)(using perOp)
        try
          val actual = GpuExprs.eval(e, cols, 4)(using fused)
          try
            val (x, y) = (expected.toColumn(4), actual.toColumn(4))
            try
              (name, y.getType) shouldBe (name, x.getType)
              (name, show(y)) shouldBe (name, show(x))
            finally
              x.close()
              y.close()
          finally
            actual.close()
        finally
          expected.close()
      }
    finally
      cols.foreach(_.close())
  }

end GpuAstArithTest

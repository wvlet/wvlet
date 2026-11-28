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
import ai.rapids.cudf.Table
import ai.rapids.cudf.ast.AstExpression
import ai.rapids.cudf.ast.BinaryOperation
import ai.rapids.cudf.ast.BinaryOperator
import ai.rapids.cudf.ast.ColumnReference
import ai.rapids.cudf.ast.Literal
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.PType

import scala.collection.mutable.ArrayBuffer

/**
  * Evaluates a tree of `+`, `-`, `*`, and negation over DECIMAL64 or FLOAT64 as one cudf AST
  * kernel, instead of one cudf kernel (and one intermediate column) per operator.
  *
  * Each node computes in the type [[GpuExprs]] would use: a decimal node fuses only when the value
  * bounds narrow it to DECIMAL64, and each side keeps its own scale, as in the per-operator path.
  * cudf's AST applies the same scale rules as its binary operators (a product's scale is the sum of
  * the sides' scales; a sum aligns to the larger scale), so results are identical. Division stays
  * per operator: the CPU returns NULL on division by zero. A child that doesn't fuse (a CASE, a
  * cast that changes the scale, a DECIMAL128 node) is evaluated per operator first and read by the
  * kernel as one more input column.
  */
private[gpu] object GpuAstArith:

  /** A planned AST node; [[Leaf]]s are evaluated per operator before the kernel runs */
  private sealed trait Node
  private case class Col(index: Int)                                 extends Node
  private case class Lit(literal: Literal)                           extends Node
  private case class Leaf(e: VExpr, p: PType)                        extends Node
  private case class Op(op: BinaryOperator, left: Node, right: Node) extends Node

  /** Operators fused in the tree under `n` */
  private def ops(n: Node): Int =
    n match
      case Op(_, l, r) =>
        1 + ops(l) + ops(r)
      case _ =>
        0

  /** Whether the tree under `n` reads a column, which sets the kernel's output rows */
  private def readsColumn(n: Node): Boolean =
    n match
      case Op(_, l, r) =>
        readsColumn(l) || readsColumn(r)
      case Lit(_) =>
        false
      case _ =>
        true

  /**
    * `a` computed as one AST kernel, or None when `a` itself doesn't fuse, fewer than two operators
    * would (a single operator gains nothing over cudf's binary operator), or it reads only
    * constants
    */
  def eval(a: Arith, input: IndexedSeq[ColumnVector], rows: Int)(using Bounds): Option[Operand] =
    val out = GpuExprs.arithOut(a)
    arith(a, out, input) match
      case Some(tree) if ops(tree) >= 2 && readsColumn(tree) =>
        Some(run(tree, input, rows))
      case _ =>
        None

  private def run(tree: Node, input: IndexedSeq[ColumnVector], rows: Int)(using Bounds): Operand =
    val leaves = ArrayBuffer.empty[ColumnVector]
    try
      def build(n: Node): AstExpression =
        n match
          case Col(i) =>
            ColumnReference(i)
          case Lit(l) =>
            l
          case Leaf(e, p) =>
            val o = GpuExprs.evalAs(e, p, input, rows)
            try leaves += o.toColumn(rows)
            finally o.close()
            ColumnReference(input.size + leaves.size - 1)
          case Op(op, l, r) =>
            BinaryOperation(op, build(l), build(r))
      val expr = build(tree)
      val ce   = expr.compile()
      try
        val t = Table((input ++ leaves)*)
        try ColOperand(ce.computeColumn(t), owned = true)
        finally t.close()
      finally
        ce.close()
    finally
      leaves.foreach(_.close())

  /** The engine type of a value held as `d` (DECIMAL64 at its scale, or FLOAT64) */
  private def ptypeOf(d: DType): PType =
    if d == DType.FLOAT64 then
      PType.Float64
    else
      PType.decimal(GpuTypes.MaxDecimal64Precision, -d.getScale)

  private def fusable(d: DType): Boolean =
    d == DType.FLOAT64 || d.getTypeId == DType.DTypeEnum.DECIMAL64

  /** `a` as an AST node computing in `out`, when `a` is `+ - *` and `out` fuses */
  private def arith(a: Arith, out: DType, input: IndexedSeq[ColumnVector])(using
      Bounds
  ): Option[Node] =
    val op =
      a.op match
        case ArithOp.Add =>
          Some(BinaryOperator.ADD)
        case ArithOp.Sub =>
          Some(BinaryOperator.SUB)
        case ArithOp.Mul =>
          Some(BinaryOperator.MUL)
        case _ =>
          None
    op.filter(_ => fusable(out) && (a.ptype == PType.Float64 || a.ptype.isDecimal))
      .map { o =>
        // As GpuExprs.arith: decimal sides at their own scales, FLOAT64 sides as FLOAT64
        def side(e: VExpr): DType =
          if out == DType.FLOAT64 then
            DType.FLOAT64
          else
            DType.create(DType.DTypeEnum.DECIMAL64, -GpuExprs.scaleOf(e.ptype).get)
        Op(o, node(a.left, side(a.left), input), node(a.right, side(a.right), input))
      }

  /** `e` as an AST node of type `want` (DECIMAL64 at a scale, or FLOAT64) */
  private def node(e: VExpr, want: DType, input: IndexedSeq[ColumnVector])(using Bounds): Node =
    def leaf = Leaf(e, ptypeOf(want))
    e match
      case ColRef(i, _, _) if input(i).getType == want =>
        Col(i)
      case Const(v, p) if v != null =>
        literal(v, p, want).map(Lit(_)).getOrElse(leaf)
      case a: Arith if GpuExprs.arithOut(a) == want =>
        arith(a, want, input).getOrElse(leaf)
      case Negate(c) =>
        // 0 - c, in the type of c (negation keeps the scale)
        literal(0, PType.Int32, want) match
          case Some(zero) =>
            Op(BinaryOperator.SUB, Lit(zero), node(c, want, input))
          case None =>
            leaf
      case c: CastExpr if !c.tryCast && sameValues(c.child.ptype, c.ptype) =>
        // A cast that changes only the declared precision leaves the values unchanged
        node(c.child, want, input)
      case _ =>
        leaf

  /** Whether a value of `from` cast to `to` keeps its unscaled value */
  private def sameValues(from: PType, to: PType): Boolean =
    (from, to) match
      case (PType.Decimal(_, s1), PType.Decimal(_, s2)) =>
        s1 == s2
      case (PType.Float64, PType.Float64) =>
        true
      case _ =>
        false

  /** Constant `v` (engine type `p`) as a literal of `want`, when it converts exactly */
  private def literal(v: Any, p: PType, want: DType): Option[Literal] =
    if want == DType.FLOAT64 then
      v match
        case n: Number =>
          // As GpuExprs.evalAs converts constants to FLOAT64
          Some(Literal.ofDouble(n.doubleValue()))
        case _ =>
          None
    else if p.isDecimal || p.isIntegral then
      GpuTypes
        .unscaled(v, -want.getScale)
        .filter(_.bitLength() < 64)
        .map(u => Literal.ofDecimal(want, u))
    else
      None

end GpuAstArith

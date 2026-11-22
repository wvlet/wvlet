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

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.uni.test.UniTest

class JoinOrderOptimizerTest extends UniTest:
  private def input(cols: String*): Operator =
    val schema = Schema(cols.map(Field(_, PType.Int64)).toIndexedSeq)
    BatchesOp(schema, Nil, cols.head)

  private def col(name: String): Expression = UnquotedIdentifier(name, NoSpan)

  /** Binds identifiers by column name; other conditions bind to `true` */
  private object NameBinding extends JoinBinding:
    private def bindCol(schema: Schema, e: Expression): VExpr =
      val name = e.asInstanceOf[Identifier].unquotedValue
      val i    = schema.fields.indexWhere(_.name == name)
      ColRef(i, schema(i).ptype, name)

    override def joinKeys(
        left: Schema,
        right: Schema,
        pairs: Seq[(Expression, Expression)]
    ): (IndexedSeq[VExpr], IndexedSeq[VExpr]) = (
      pairs.map((l, _) => bindCol(left, l)).toIndexedSeq,
      pairs.map((_, r) => bindCol(right, r)).toIndexedSeq
    )

    override def bind(schema: Schema, e: Expression): VExpr = Const(true, PType.Bool)

  private def operators(op: Operator): List[Operator] = op :: op.children.toList.flatMap(operators)

  // A fact table with two dimensions, and a condition over both dimensions
  private val graph = JoinGraph(
    IndexedSeq(input("a_k"), input("f_a", "f_b"), input("b_k")),
    IndexedSeq(10.0, 1e6, 100.0),
    List(
      JoinEdge(
        Eq(col("f_a"), col("a_k"), NoSpan),
        Set(0, 1),
        Some(JoinEdge.Sides(col("f_a"), Set(1), col("a_k"), Set(0)))
      ),
      JoinEdge(
        Eq(col("f_b"), col("b_k"), NoSpan),
        Set(1, 2),
        Some(JoinEdge.Sides(col("f_b"), Set(1), col("b_k"), Set(2)))
      ),
      JoinEdge(GreaterThan(col("a_k"), col("b_k"), NoSpan), Set(0, 2), None)
    )
  )

  private def plan(options: OptimizerOptions): (Operator, List[Expression]) = JoinOrderOptimizer(
    options
  ).plan(graph, JoinOrderOptimizer.expressionSearch(OperatorTreeBuilder, NameBinding))

  test("should join every input once and place every condition once") {
    val (tree, remaining) = plan(OptimizerOptions())
    remaining shouldBe Nil
    val ops = operators(tree)
    graph.inputs.foreach(in => ops.count(_ eq in) shouldBe 1)
    ops.count(_.isInstanceOf[HashJoinOp]) shouldBe 2
    ops.count(_.isInstanceOf[FilterOp]) shouldBe 1
    ops.count(_.isInstanceOf[NestedLoopJoinOp]) shouldBe 0
  }

  test("should start from the largest input and probe with it") {
    val (tree, _) = plan(OptimizerOptions())
    val joins     = operators(tree).collect { case j: HashJoinOp =>
      j
    }
    // The bottom join probes with the fact table
    joins.last.left shouldBeTheSameInstanceAs graph.inputs(1)
  }

  test("should join in written order without reorder_joins") {
    val (tree, remaining) = plan(OptimizerOptions().withDisabled(Optimization.ReorderJoins))
    remaining shouldBe Nil
    val joins = operators(tree).collect { case j: HashJoinOp =>
      j
    }
    joins.size shouldBe 2
    // Starts from the first input; the fact table is connected to it by an equality
    operators(joins.last).exists(_ eq graph.inputs(0)) shouldBe true
    operators(joins.last).exists(_ eq graph.inputs(1)) shouldBe true
  }

end JoinOrderOptimizerTest

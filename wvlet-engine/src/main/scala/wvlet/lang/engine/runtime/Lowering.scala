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
package wvlet.lang.engine.runtime

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.plan.JoinOrderOptimizer

/**
  * Turns a [[RuntimePlan]] into physical operators: column ids become input positions. The physical
  * passes over operators (column pruning, parallelism) run afterwards, as for the recursive
  * planner.
  */
object Lowering:
  import RuntimePlan.*

  def apply(p: RuntimePlan): Operator =
    p match
      case Scan(source, _) =>
        ScanOp(source, p.schema, None, None)
      case Values(_, rows) =>
        BatchesOp(p.schema, Seq(rows), "Values")
      case Filter(child, cond) =>
        FilterOp(apply(child), cond.lower(positions(child)))
      case Project(child, exprs, _) =>
        val pos = positions(child)
        ProjectOp(apply(child), exprs.map(_.lower(pos)), p.schema)
      case Sort(child, keys, limit) =>
        val pos = positions(child)
        SortOp(
          apply(child),
          keys.map(k => SortKey(k.expr.lower(pos), k.ascending, k.nullsFirst)),
          limit
        )
      case Limit(child, limit, offset) =>
        LimitOp(apply(child), limit, offset)
      case Join(left, right, kind, lk, rk, residual, buildLeft) =>
        val lpos = positions(left)
        val rpos = positions(right)
        JoinOrderOptimizer.joinOp(
          apply(left),
          apply(right),
          kind,
          lk.map(_.lower(lpos)),
          rk.map(_.lower(rpos)),
          residual.map(_.lower(positions(p))),
          buildLeft,
          p.schema
        )
      case Aggregate(child, keys, calls, _) =>
        val pos = positions(child)
        HashAggOp(apply(child), keys.map(_.lower(pos)), calls.map(_.lower(pos)), p.schema)
      case j: InnerJoins =>
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(
            s"InnerJoins must be ordered by ReorderJoins before lowering:\n${p.explain}"
          )
      case Legacy(op, _) =>
        op

  /** Column id -> position in the output of `p` (-1 for ids it doesn't output) */
  private def positions(p: RuntimePlan): Array[Int] =
    val out = p.output
    val pos = Array.fill(out.map(_.id.value).maxOption.getOrElse(-1) + 1)(-1)
    out.indices.foreach(i => pos(out(i).id.value) = i)
    pos

end Lowering

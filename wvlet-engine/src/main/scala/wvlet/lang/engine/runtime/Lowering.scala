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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.VExprs

/**
  * Turns a [[RuntimePlan]] into physical operators: column ids become input positions. The physical
  * passes over operators (column pruning, parallelism) run afterwards, as for the recursive
  * planner.
  */
object Lowering:
  import RuntimePlan.*

  def apply(p: RuntimePlan): Operator =
    p match
      case Scan(source, _, read, filter) =>
        ScanOp(source, p.schema, read, filter)
      case Values(columns, rows) if columns.isEmpty && rows.size == 1 =>
        // One row without columns (a query without FROM), as the planner plans it
        SingleRowOp()
      case Values(_, rows) =>
        BatchesOp(p.schema, Seq(rows), "Values")
      case Filter(ref @ CTERef(cte, columns), cond, counts) =>
        // The reader reports its condition over the body's columns, which share positions
        val body   = cte.body.output
        val toBody = columns.indices.map(i => columns(i).id.value -> body(i)).toMap
        val e      = VExprs.mapRefs(
          cond.expr,
          r => ColRef(toBody(r.index).id.value, r.ptype, toBody(r.index).name)
        )
        val filter = JoinCondition(BExpr(e), PushDownFilters.remapCounts(cond.expr, e, counts))
        FilterOp(cte.reader(ref.schema, Some(filter)), cond.lower(positions(ref)))
      case Filter(child, cond, _) =>
        FilterOp(apply(child), cond.lower(positions(child)))
      case Project(child, exprs, _) =>
        val pos = positions(child)
        ProjectOp(apply(child), exprs.map(_.lower(pos)), p.schema)
      case Sort(child, keys, limit) =>
        val pos = positions(child)
        SortOp(apply(child), keys.map(_.lower(pos)), limit)
      case Limit(child, limit, offset) =>
        LimitOp(apply(child), limit, offset)
      case j @ Join(left, right, kind, lk, rk, residual, buildLeft) =>
        val lpos = positions(left)
        val rpos = positions(right)
        JoinOrderOptimizer.joinOp(
          apply(left),
          apply(right),
          kind,
          lk.map(_.lower(lpos)),
          rk.map(_.lower(rpos)),
          residual.map(_.lower(positions(j.inputs))),
          buildLeft,
          p.schema
        )
      case Aggregate(child, keys, calls, _) =>
        val pos = positions(child)
        HashAggOp(apply(child), keys.map(_.lower(pos)), calls.map(_.lower(pos)), p.schema)
      case _: InnerJoins =>
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(
            s"InnerJoins must be ordered by ReorderJoins before lowering:\n${p.explain}"
          )
      case Union(inputs, _) =>
        UnionAllOp(inputs.map(apply), p.schema)
      case SetOp(left, right, intersect, all, _) =>
        SetOpOp(apply(left), apply(right), intersect, all, p.schema)
      case Distinct(child) =>
        DistinctOp(apply(child))
      case Sample(child, percentage) =>
        Planner.sampleFilter(apply(child), percentage)
      case Window(child, calls, _) =>
        val pos = positions(child)
        WindowOp(apply(child), calls.map(_.lower(pos)), p.schema)
      case Unnest(child, lists, keepInput, outer, _) =>
        val pos = positions(child)
        UnnestOp(apply(child), lists.map(_.lower(pos)), keepInput, p.schema, outer)
      case Legacy(op, _) =>
        op
      case CTERef(cte, _) =>
        cte.reader(p.schema)

  /** Column id -> position in the output of `p` (-1 for ids it doesn't output) */
  private def positions(p: RuntimePlan): Array[Int] = positions(p.output)

  /** Column id -> position in `out` (-1 for ids it doesn't have) */
  private def positions(out: IndexedSeq[Column]): Array[Int] =
    val pos = Array.fill(out.map(_.id.value).maxOption.getOrElse(-1) + 1)(-1)
    out.indices.foreach(i => pos(out(i).id.value) = i)
    pos

end Lowering

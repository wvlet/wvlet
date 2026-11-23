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

import wvlet.lang.engine.plan.Optimization

/**
  * Drops the columns no node above needs, by column id, in one top-down walk after
  * [[ReorderJoins]]. A node's needs are the columns its parent needs plus those its own expressions
  * read; projections drop the expressions nobody needs and scans read only the needed columns.
  * Aggregations and windows keep their calls, and the other leaves (VALUES, CTE reads, `Legacy`)
  * keep their columns: the physical pruning after lowering narrows `Legacy` subtrees. The root
  * keeps its output.
  *
  * The planner also runs it over a shared CTE body with the columns its readers read, so that the
  * body computes only those ([[prune]]).
  */
object PruneColumns extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.PruneColumns.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if ctx.options.isEnabled(Optimization.PruneColumns) then
      prune(p, p.output.map(_.id).toSet, ctx)
    else
      p

  /**
    * The plan with only the columns needed to output `needed`: its output is the output of `p`
    * without some columns not in `needed`, in the same order
    */
  def prune(p: RuntimePlan, needed: Set[ColumnId], ctx: OptimizerContext): RuntimePlan =
    p match
      case s @ Scan(_, columns, _, _) =>
        val keep = columns.indices.filter(i => needed.contains(columns(i).id))
        if keep.size == columns.size then
          p
        else
          ctx.fired(name)
          // Keep at least one column, so that the scan still carries the row count
          s.project(
            if keep.isEmpty then
              IndexedSeq(0)
            else
              keep
          )
      case Values(_, _) | Legacy(_, _) | CTERef(_, _) =>
        p
      case Filter(child, cond, _) =>
        p.withChildren(Seq(prune(child, needed ++ cond.refs, ctx)))
      case Project(child, exprs, columns) =>
        val keep = columns.indices.filter(i => needed.contains(columns(i).id))
        val c    = prune(child, keep.flatMap(i => exprs(i).refs).toSet, ctx)
        if keep.size == columns.size then
          p.withChildren(Seq(c))
        else
          ctx.fired(name)
          Project(c, keep.map(exprs), keep.map(columns))
      case Sort(child, keys, _) =>
        p.withChildren(Seq(prune(child, needed ++ keys.flatMap(_.expr.refs), ctx)))
      case Limit(child, _, _) =>
        p.withChildren(Seq(prune(child, needed, ctx)))
      case Sample(child, _) =>
        p.withChildren(Seq(prune(child, needed, ctx)))
      case Aggregate(child, keys, calls, _) =>
        val reads = (keys ++ calls.flatMap(_.exprs)).flatMap(_.refs).toSet
        p.withChildren(Seq(prune(child, reads, ctx)))
      case Join(left, right, _, lk, rk, residual, _) =>
        val reads = needed ++ (lk ++ rk ++ residual).flatMap(_.refs)
        p.withChildren(Seq(prune(left, reads, ctx), prune(right, reads, ctx)))
      case InnerJoins(inputs, conds) =>
        val reads = needed ++ conds.flatMap(_.cond.refs)
        p.withChildren(inputs.map(prune(_, reads, ctx)))
      case Window(child, calls, _) =>
        // Every call is kept, as ColumnPruner does
        p.withChildren(Seq(prune(child, needed ++ calls.flatMap(_.exprs).flatMap(_.refs), ctx)))
      case Unnest(child, lists, _, _, _) =>
        // Every list is kept: dropping one changes the rows
        p.withChildren(Seq(prune(child, needed ++ lists.flatMap(_.refs), ctx)))
      case Union(_, _) | SetOp(_, _, _, _, _) | Distinct(_) =>
        // Set operations read whole rows of their inputs: the physical pruning narrows union
        // inputs by position after lowering
        p.withChildren(p.children.map(c => prune(c, c.output.map(_.id).toSet, ctx)))

end PruneColumns

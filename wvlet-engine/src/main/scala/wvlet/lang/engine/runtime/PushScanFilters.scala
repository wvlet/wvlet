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
import wvlet.lang.engine.plan.ScanFilters

/**
  * Gives each scan directly under a filter the row-group filter its source can apply
  * ([[ScanFilters.toParquet]]), so that a Parquet source skips row groups that can't match. The
  * filter stays in place: the scan's filter only skips data. It runs after [[PushDownFilters]] has
  * moved the conjuncts over the scans, and after [[PruneColumns]]; the scan's filter is recomputed
  * from the condition over it, so running the pass again (after pushing more conjuncts into a
  * shared CTE body) replaces it.
  *
  * Scans inside `Legacy` leaves get theirs from the physical pruning after lowering.
  */
object PushScanFilters extends Pass:
  import RuntimePlan.*

  override def name: String = Optimization.PushScanFilters.name

  override def run(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    if ctx.options.isEnabled(Optimization.PushScanFilters) then
      attach(p, ctx)
    else
      p

  private def attach(p: RuntimePlan, ctx: OptimizerContext): RuntimePlan =
    p match
      case f @ Filter(s @ Scan(source, columns, _, filter), cond, _) if source.supportsFilter =>
        val pos = columns.map(_.id.value).zipWithIndex.toMap
        val f2  = ScanFilters.toParquet(
          cond.expr,
          id => pos.get(id).map(s.sourceColumnName).filter(source.canFilterOn)
        )
        if f2 == filter then
          p
        else
          ctx.fired(name)
          f.copy(child = s.copy(filter = f2))
      case _ =>
        p.withChildren(p.children.map(attach(_, ctx)))

end PushScanFilters

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

import wvlet.lang.engine.exec.AggCall
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.runtime.PushDownFilters
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Schema

/**
  * The groups of an aggregation, computed at plan time, that aggregations of the same input merge
  * instead of reading the input again (`share_in_subquery_groups`)
  *
  * @param key
  *   the loose key of the aggregation's input
  *   ([[wvlet.lang.engine.runtime.ShareSubplans.looseKey]])
  * @param keys
  *   the grouping keys over the input's canonical column ids ([[SharedGroups.canon]])
  * @param calls
  *   the aggregate calls over the input's canonical column ids
  * @param schema
  *   the columns of `rows`: the keys, then the calls
  */
private[engine] case class SharedGroups(
    key: Any,
    keys: IndexedSeq[VExpr],
    calls: IndexedSeq[AggCall],
    schema: Schema,
    rows: Batch
)

private[engine] object SharedGroups:
  /**
    * Whether aggregating the values of `c` over groups gives its value over the groups' rows: a sum
    * of sums (integers or decimals, whose sums are exact), the min of mins, and the max of maxes.
    * Counts are left out, since a count over no groups is NULL rather than 0. A call over a
    * non-deterministic expression (`random()`) is left out too: another call of the same text draws
    * its own values
    */
  def mergeable(c: AggCall): Boolean =
    !c.distinct && c.order.isEmpty && c.args.forall(PushDownFilters.isDeterministic) &&
      c.filter.forall(PushDownFilters.isDeterministic) && (
        c.name.toLowerCase match
          case "min" | "max" =>
            true
          case "sum" =>
            c.ptype == PType.Int64 || c.ptype.isInstanceOf[PType.Decimal]
          case _ =>
            false
      )

  /** `e` over canonical column ids: column `i` reads `ids(i)`, and names are dropped */
  def canon(e: VExpr, ids: IndexedSeq[Int]): VExpr = VExprs.mapRefs(
    e,
    c => c.copy(index = ids(c.index), name = "")
  )

  /** `c` over canonical column ids */
  def canon(c: AggCall, ids: IndexedSeq[Int]): AggCall = c.copy(
    args = c.args.map(canon(_, ids)),
    filter = c.filter.map(canon(_, ids))
  )

  /**
    * An aggregation by `keys` of `calls`, over an input whose canonical column ids are `ids`, as an
    * aggregation of the groups `g` of the same input: keys and calls over the columns of `g.rows`.
    * None unless every key is a key of `g` and every call a mergeable call of `g`
    */
  def merge(
      g: SharedGroups,
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      ids: IndexedSeq[Int]
  ): Option[(IndexedSeq[VExpr], IndexedSeq[AggCall])] =
    try
      val n  = g.keys.size
      val ki = keys.map(k => g.keys.indexOf(canon(k, ids)))
      val ci = calls.map(c => g.calls.indexOf(canon(c, ids)))
      Option.when(
        keys.nonEmpty && ki.forall(_ >= 0) && ci.forall(_ >= 0) && calls.forall(mergeable) &&
          keys.forall(PushDownFilters.isDeterministic)
      ) {
        val s           = g.schema
        val mergedKeys  = ki.map(i => ColRef(i, s(i).ptype, s(i).name))
        val mergedCalls = ci.map { j =>
          val f = s(n + j)
          AggCall(
            g.calls(j).name.toLowerCase,
            Seq(ColRef(n + j, f.ptype, f.name)),
            false,
            None,
            f.ptype
          )
        }
        (mergedKeys, mergedCalls)
      }
    catch
      case _: IndexOutOfBoundsException =>
        None

end SharedGroups

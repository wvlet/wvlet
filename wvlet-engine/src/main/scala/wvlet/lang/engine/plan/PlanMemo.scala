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

import wvlet.lang.api.Span
import wvlet.lang.model.plan.LogicalPlan

import scala.collection.mutable

/**
  * Values memoized by logical plan node. A node is found by identity first, then among the copies
  * recorded with [[putCopy]] by value: rewrites such as `transform` and `Binder.expandTemplatesIn`
  * copy the subtrees they pass through, so a plan-time subquery can be bound as a copy that an
  * identity map misses (#2464, #2526). Copies are kept by span, so that a lookup compares whole
  * trees only with the nodes at the same place in the query
  */
final class PlanMemo[K <: LogicalPlan, V]:
  private val byIdentity = java.util.IdentityHashMap[K, V]()
  private val copies     = mutable.HashMap.empty[Span, List[(LogicalPlan, V)]]

  /** Memoize `v` for the node `k` */
  def put(k: K, v: V): Unit = byIdentity.put(k, v)

  /** Memoize `v` also for the nodes equal to `copy`, a copy of a node memoized with [[put]] */
  def putCopy(copy: LogicalPlan, v: V): Unit =
    copies(copy.span) = (copy, v) :: copies.getOrElse(copy.span, Nil)

  /** The value of the node `k`, or of a recorded copy equal to it */
  def get(k: K): Option[V] = Option(byIdentity.get(k)).orElse(
    if copies.isEmpty then
      None
    else
      copies
        .get(k.span)
        .flatMap(
          _.collectFirst {
            case (p, v) if p == k =>
              v
          }
        )
  )

end PlanMemo

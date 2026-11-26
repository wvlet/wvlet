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
package wvlet.lang.engine.expr

import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.*

/**
  * `deferred_null_checks`: IS NULL of a column that a join or filter output has not gathered yet
  * ([[Batch.take]]) reads the column's row positions, where -1 is a NULL row (the padding of an
  * outer join), and its source vector's nulls, instead of gathering the column first. TPC-DS q97
  * checks both sides of a FULL join for NULL and reads no other column of them.
  */
object DeferredNullChecks:

  /** Mark every IS NULL of a column reference in `e` to read deferred columns in place */
  def apply(e: VExpr): VExpr =
    if !contains(e) then
      e
    else
      try
        VExprs.mapChildren(e, apply) match
          case x @ IsNullExpr(_: ColRef, _, false) =>
            x.copy(deferred = true)
          case other =>
            other
      catch
        case _: IllegalStateException =>
          e

  private def contains(e: VExpr): Boolean =
    e match
      case IsNullExpr(_: ColRef, _, false) =>
        true
      case _ =>
        e.children.exists(contains)

  /**
    * `child IS NULL` (`!=` negate) over batch b, read from the positions and source of the deferred
    * column `child` refers to, or null when `child` is no column reference or its column is
    * gathered already
    */
  def eval(child: VExpr, negate: Boolean, b: Batch): Vec =
    child match
      case c: ColRef =>
        val src = b.deferredSource(c.index)
        if src == null then
          null
        else
          val sel   = b.deferredPositions(c.index)
          val nulls = src.nulls
          val n     = b.size
          val o     = new Array[Boolean](n)
          var i     = 0
          if nulls == null then
            while i < n do
              o(i) = (sel(i) < 0) != negate
              i += 1
          else
            while i < n do
              val p = sel(i)
              o(i) = (p < 0 || nulls(p)) != negate
              i += 1
          BoolVec(o, null)
      case _ =>
        null

end DeferredNullChecks

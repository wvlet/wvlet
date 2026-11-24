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

import wvlet.lang.engine.expr.LikeExpr
import wvlet.lang.engine.plan.CardinalityEstimator

/**
  * Prefix patterns of LIKE. `x like 'p%'` holds for the strings from `p` up to, not including, the
  * next string after every string starting with `p`: a range with both bounds, as `x between a and
  * b`, which the estimator gives the default selectivity once per bound.
  */
object PrefixLike:

  /** The estimated fraction of rows matching a prefix pattern: one default per range bound */
  val Selectivity: Double =
    CardinalityEstimator.DefaultSelectivity * CardinalityEstimator.DefaultSelectivity

  /**
    * The prefix of a non-negated, case-sensitive `x like 'p%'` with a non-empty p and no other
    * wildcard or escape character
    */
  def prefix(l: LikeExpr): Option[String] =
    val p = l.pattern
    Option.when(
      !l.negate && !l.caseInsensitive && l.escape.isEmpty && p.length > 1 && p.last == '%' &&
        p.indexOf('%') == p.length - 1 && !p.contains('_')
    )(p.dropRight(1))

end PrefixLike

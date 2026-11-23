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

import wvlet.lang.engine.exec.Aggregates
import wvlet.lang.engine.exec.WindowCall
import wvlet.lang.engine.expr.ScalarFunctions

/**
  * The function names the engine binds, across every place that resolves a call by name. A name
  * counts when the engine binds it for some arity or argument shape (e.g., lambda list functions
  * need a lambda argument); calls of other names fail with "Unsupported function in the engine"
  */
object EngineFunctions:

  /** Calls the Binder or Planner match by name before the scalar registry */
  val specialForms: Set[String] =
    Binder.LambdaFunctions ++
      Set(
        // Trino-style date_add(unit, n, date) and date_add(date, interval)
        "date_add",
        "grouping",
        "count_star",
        "unnest",
        "rollup",
        "cube"
      )

  /** Function names the parser turns into dedicated expression nodes (written without parens) */
  val keywordForms: Set[String] = Set("current_date", "current_timestamp", "current_localtimestamp")

  /** True when the engine binds calls of the given function name */
  def isSupported(name: String): Boolean =
    val n = name.toLowerCase
    ScalarFunctions.isDefined(n) || Aggregates.isAggregate(n) ||
    WindowCall.rankingFunctions.contains(n) || specialForms.contains(n) || keywordForms.contains(n)

end EngineFunctions

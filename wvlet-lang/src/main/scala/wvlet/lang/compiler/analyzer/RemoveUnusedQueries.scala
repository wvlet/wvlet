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
package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Phase
import wvlet.lang.model.plan.ModelDef
import wvlet.lang.model.plan.TypeDef

/**
  * Check unused compilation units and exclude them
  */
class RemoveUnusedQueries extends Phase("check-unused"):

  private var contextUnit: Option[CompilationUnit] = None
  private var usedUnits                            = List.empty[CompilationUnit]

  override protected def init(units: List[CompilationUnit], context: Context): Unit =
    contextUnit = context
      .global
      .getContextUnit
      .flatMap { unit =>
        units.find(_ eq unit)
      }
    usedUnits = List.empty

  override def runAlways: Boolean =
    // Need to run this phase always to check unused queries depending on the context unit
    true

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    if contextUnit.exists(_ eq unit) then
      usedUnits = unit :: usedUnits
    else
      var hasDef = false
      unit
        .unresolvedPlan
        .traverse {
          case p: TypeDef =>
            hasDef = true
          case m: ModelDef =>
            hasDef = true
        }
      if hasDef then
        usedUnits = unit :: usedUnits
    unit

  override protected def refineUnits(units: List[CompilationUnit]): List[CompilationUnit] =
    debug(s"Compiling ${usedUnits.size} files out of ${units.size} files")
    usedUnits.reverse

end RemoveUnusedQueries

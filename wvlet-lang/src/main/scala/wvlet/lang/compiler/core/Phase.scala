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
package wvlet.lang.compiler.core

import wvlet.log.LogSupport

/**
  * Compilation phase
  */
trait Phase(
    // The name of the phase
    val name: String
) extends LogSupport:
  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    debug(s"Running phase ${name}")
    val buf = List.newBuilder[CompilationUnit]
    val phaseCtx = context.withPhase(this)
    for unit <- units do
      val unitContext = context.withCompilationUnit(unit)
      buf += run(unit, unitContext)
    buf.result()

  def run(unit: CompilationUnit, context: Context): CompilationUnit

object Phase:
  object NoPhase extends Phase("NoPhase"):
    def run(unit: CompilationUnit, context: Context): CompilationUnit = unit

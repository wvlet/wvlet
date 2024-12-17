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
package wvlet.lang.compiler

import wvlet.log.{LogSource, LogSupport, Logger}

import scala.util.control.NonFatal

/**
  * Compilation phase that processes a list of compilation units
  * @param name
  */
abstract class Phase(
    // The name of the phase
    val name: String
) extends LogSupport:
  private val logger = Logger.of[Phase]

  // Initialize the phase once before processing the units
  protected def init(units: List[CompilationUnit], context: Context): Unit = {}

  protected def refineUnits(units: List[CompilationUnit]): List[CompilationUnit] = units

  protected def runAlways: Boolean = false

  def runOn(units: List[CompilationUnit], context: Context): List[CompilationUnit] =
    logger.debug(s"Running phase ${name}")
    init(units, context)
    val completedUnits = List.newBuilder[CompilationUnit]
    for
      unit <- units
      if !unit.isFailed
    do
      if !runAlways && unit.isFinished(this) then
        completedUnits += unit
      else
        try
          logger.trace(s"Running phase ${name} on ${unit.sourceFile.relativeFilePath}")
          val sourceContext = context.withCompilationUnit(unit)
          completedUnits += run(unit, sourceContext)
          unit.setFinished(this)
        catch
          case NonFatal(e) =>
            context.workEnv.logError(e)
            unit.setFailed(e)

    refineUnits(completedUnits.result())

  def run(unit: CompilationUnit, context: Context): CompilationUnit

end Phase

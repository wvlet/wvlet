package com.treasuredata.flow.lang.compiler

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
      logger.trace(s"Running phase ${name} on ${unit.sourceFile.relativeFilePath}")
      val sourceContext = context.withCompilationUnit(unit)
      if !runAlways && unit.isFinished(this) then
        completedUnits += unit
      else
        try
          completedUnits += run(unit, sourceContext)
          unit.setFinished(this)
        catch
          case NonFatal(e) =>
            unit.lastError = Some(e)

    refineUnits(completedUnits.result())

  def run(unit: CompilationUnit, context: Context): CompilationUnit

end Phase

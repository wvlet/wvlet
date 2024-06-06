package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.plan.LogicalPlan

class Parsers(unit: CompilationUnit):

  private val scanner = Scanner(unit.sourceFile)

  def parse: LogicalPlan =
    ???

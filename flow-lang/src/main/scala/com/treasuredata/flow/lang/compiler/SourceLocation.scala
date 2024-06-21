package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.NodeLocation

case class SourceLocation(compileUnit: CompilationUnit, nodeLocation: Option[NodeLocation]):
  def locationString: String =
    nodeLocation match
      case Some(loc) =>
        s"${compileUnit.sourceFile.file}:${loc.line}:${loc.column}"
      case None =>
        compileUnit.sourceFile.file

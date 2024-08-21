package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.NodeLocation

case class SourceLocation(compileUnit: CompilationUnit, nodeLocation: Option[NodeLocation]):
  def codeLineAt: String = nodeLocation
    .map { loc =>
      val line = compileUnit.sourceFile.sourceLine(loc.line)
      line
    }
    .getOrElse("")

  def locationString: String =
    nodeLocation match
      case Some(loc) =>
        s"${compileUnit.sourceFile.fileName}:${loc.line}:${loc.column}"
      case None =>
        compileUnit.sourceFile.relativeFilePath

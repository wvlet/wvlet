package wvlet.lang.compiler

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.NodeLocation

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

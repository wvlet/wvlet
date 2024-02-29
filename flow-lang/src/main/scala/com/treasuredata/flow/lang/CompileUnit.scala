package com.treasuredata.flow.lang

import com.treasuredata.flow.lang.model.NodeLocation
import wvlet.log.io.IOUtil

case class CompileUnit(sourceFile: String):
  private[lang] def readAsString: String =
    IOUtil.readAsString(sourceFile)

  def toSourceLocation(nodeLocation: Option[NodeLocation]) =
    SourceLocation(this, nodeLocation)

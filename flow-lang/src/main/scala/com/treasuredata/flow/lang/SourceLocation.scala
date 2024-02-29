package com.treasuredata.flow.lang

import com.treasuredata.flow.lang.model.NodeLocation

case class SourceLocation(compileUnit: CompileUnit, nodeLocation: Option[NodeLocation])

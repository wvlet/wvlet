package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.model.NodeLocation

case class SourceLocation(compileUnit: CompilationUnit, nodeLocation: Option[NodeLocation])

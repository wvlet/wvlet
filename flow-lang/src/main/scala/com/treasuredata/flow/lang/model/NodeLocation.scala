package com.treasuredata.flow.lang.model

case class NodeLocation(
    line: Int,
    // column position in the line (1-origin)
    column: Int
)

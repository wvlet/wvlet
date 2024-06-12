package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.expr.{Attribute, Name}
import wvlet.airframe.ulid.ULID

/**
  * Additional nodes that for organizing tasks for executing LogicalPlan nodes
  */
sealed trait ExecutionPlan
//def sessionID: ULID
end ExecutionPlan

object ExecutionPlan:
  case class Tasks(
      name: String,
      tasks: List[ExecutionPlan]
  )

  /**
    * Materialize a model using a logical plan
    * @param name
    */
  case class MaterializeModel(
      modelName: Name,
      plan: LogicalPlan
  )

package wvlet.lang.model.plan

import wvlet.lang.model.NodeLocation
import wvlet.lang.model.expr.{Attribute, NameExpr}
import wvlet.airframe.ulid.ULID

/**
  * Additional nodes that for organizing tasks for executing LogicalPlan nodes
  */
sealed trait ExecutionPlan
//def sessionID: ULID
end ExecutionPlan

object ExecutionPlan:
  case class Tasks(name: String, tasks: List[ExecutionPlan]) extends ExecutionPlan

  /**
    * Materialize a model using a logical plan
    * @param name
    */
  case class MaterializeModel(modelName: NameExpr, plan: LogicalPlan) extends ExecutionPlan

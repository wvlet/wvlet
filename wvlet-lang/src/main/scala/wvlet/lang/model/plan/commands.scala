package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.{Expression, NameExpr}

sealed trait Command extends TopLevelStatement with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class ShowQuery(name: NameExpr, span: Span)       extends Command
case class ExecuteExpr(expr: Expression, span: Span)   extends Command
case class ExplainPlan(child: LogicalPlan, span: Span) extends Command

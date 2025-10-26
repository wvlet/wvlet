package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.expr.QualifiedName

sealed trait Command extends TopLevelStatement with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

// EXPLAIN options for Trino syntax support
sealed trait ExplainOption

case class ExplainFormat(format: String)    extends ExplainOption
case class ExplainType(explainType: String) extends ExplainOption

case class ShowQuery(name: NameExpr, span: Span)     extends Command
case class ExecuteExpr(expr: Expression, span: Span) extends Command
case class ExplainPlan(
    child: LogicalPlan,
    options: List[ExplainOption] = Nil,
    analyze: Boolean = false,
    verbose: Boolean = false,
    span: Span
) extends Command

case class UseSchema(schema: QualifiedName, span: Span) extends Command
case class DescribeInput(name: NameExpr, span: Span)    extends Command
case class DescribeOutput(name: NameExpr, span: Span)   extends Command

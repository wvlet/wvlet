/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.model.plan

import wvlet.lang.api.Span
import wvlet.lang.compiler.{Name, TermName}
import wvlet.lang.model.DataType.{EmptyRelationType, NamedType}
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.{Expression, FunctionArg, Identifier, NameExpr, QualifiedName}

/**
  * Top-level flow definition.
  *
  * Example:
  * {{{
  * flow CustomerJourney(entry_segment: string) = {
  *   stage entry = from users | where segment_id = entry_segment
  *   stage process = from entry | switch { case cond -> target }
  * }
  * }}}
  *
  * @param name
  *   Flow name
  * @param params
  *   Flow parameters
  * @param stages
  *   List of stage definitions in this flow
  * @param span
  *   Source location
  */
case class FlowDef(name: TermName, params: List[DefArg], stages: List[StageDef], span: Span)
    extends TopLevelStatement
    with LeafPlan:
  override def children: List[LogicalPlan] = stages

  override def isEmpty: Boolean                = stages.isEmpty
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

  /**
    * Find a stage by name within this flow
    */
  def findStage(stageName: TermName): Option[StageDef] = stages.find(_.name == stageName)

/**
  * A stage within a flow. Each stage has a name and a body that describes its data source and
  * operations.
  *
  * Example:
  * {{{
  * stage entry = from users | where active = true
  * stage merged = merge stage_a, stage_b on _.user_id = _.user_id
  * stage control = depends on etl_complete | run_validation()
  * }}}
  *
  * @param name
  *   Stage name
  * @param input
  *   Input specification (StageInput)
  * @param body
  *   The relation/query body of this stage, or None for pure control stages
  * @param ops
  *   Stage operations (switch, fork, wait, activate, etc.)
  * @param span
  *   Source location
  */
case class StageDef(
    name: TermName,
    input: StageInput,
    body: Option[Relation],
    ops: List[StageOp],
    span: Span
) extends LogicalPlan
    with LeafPlan:
  override def children: List[LogicalPlan] =
    val b = List.newBuilder[LogicalPlan]
    body.foreach(b += _)
    ops.foreach(b += _)
    b.result()

  override def relationType: RelationType = body.map(_.relationType).getOrElse(EmptyRelationType)

  override def inputRelationType: RelationType =
    input match
      case FromStage(ref, _) =>
        EmptyRelationType // Resolved during type checking
      case MergeInput(_, _, _) =>
        EmptyRelationType
      case DependsOn(_, _) =>
        EmptyRelationType
      case NoStageInput(_) =>
        EmptyRelationType

/**
  * Represents the input source for a stage
  */
sealed trait StageInput extends LogicalPlan with LeafPlan:
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

/**
  * Input from a single stage or relation.
  *
  * Example: `from entry` or `from users`
  */
case class FromStage(ref: QualifiedName, span: Span) extends StageInput:
  override def children: List[LogicalPlan] = Nil

/**
  * Fan-in from multiple stages.
  *
  * Example:
  * {{{
  * merge stage_a, stage_b on _.user_id = _.user_id
  * merge stage_a, stage_b  -- union semantics
  * }}}
  */
case class MergeInput(refs: List[NameExpr], cond: Option[Expression], span: Span)
    extends StageInput:
  override def children: List[LogicalPlan] = Nil

/**
  * Control-only dependency (no data flow).
  *
  * Example: `depends on etl_complete, validation`
  */
case class DependsOn(refs: List[NameExpr], span: Span) extends StageInput:
  override def children: List[LogicalPlan] = Nil

/**
  * No explicit input (e.g., for entry stages that receive input from flow caller).
  *
  * Example: `stage entry = from _`
  */
case class NoStageInput(span: Span) extends StageInput:
  override def children: List[LogicalPlan] = Nil

/**
  * Operations that can be applied to a stage
  */
sealed trait StageOp extends LogicalPlan with LeafPlan:
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

/**
  * Conditional routing to different stages.
  *
  * Example:
  * {{{
  * switch {
  *   case _.email_opens > 5  -> high_engagement
  *   case _.email_opens <= 2 -> low_engagement
  *   else                    -> moderate_engagement
  * }
  * }}}
  */
case class SwitchOp(cases: List[SwitchCase], span: Span) extends StageOp:
  override def children: List[LogicalPlan] = cases

/**
  * A single case in a switch operation.
  *
  * @param condition
  *   The condition expression, or None for the else case
  * @param target
  *   Target stage name
  */
case class SwitchCase(condition: Option[Expression], target: NameExpr, span: Span)
    extends LogicalPlan
    with LeafPlan:
  override def children: List[LogicalPlan]     = Nil
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

/**
  * Percentage-based routing (A/B testing).
  *
  * Example:
  * {{{
  * split {
  *   case 50% -> variant_a
  *   case 50% -> variant_b
  * }
  * }}}
  */
case class SplitOp(cases: List[SplitCase], span: Span) extends StageOp:
  override def children: List[LogicalPlan] = cases

/**
  * A single case in a split operation.
  *
  * @param percentage
  *   Percentage for this case (0-100)
  * @param target
  *   Target stage name
  */
case class SplitCase(percentage: Int, target: NameExpr, span: Span)
    extends LogicalPlan
    with LeafPlan:
  override def children: List[LogicalPlan]     = Nil
  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

/**
  * Parallel execution of multiple stages.
  *
  * Example:
  * {{{
  * fork {
  *   stage email = activate('email')
  *   stage sms = activate('sms')
  *   stage push = activate('push')
  * }
  * }}}
  */
case class ForkOp(stages: List[StageDef], span: Span) extends StageOp:
  override def children: List[LogicalPlan] = stages

/**
  * Pause execution for a duration.
  *
  * Example: `wait('7 days')`
  */
case class WaitOp(duration: Expression, span: Span) extends StageOp:
  override def children: List[LogicalPlan] = Nil

/**
  * Activate/send to an external system.
  *
  * Example: `activate('email', template: 'promo_v1')`
  */
case class ActivateOp(target: Expression, args: List[FunctionArg], span: Span) extends StageOp:
  override def children: List[LogicalPlan] = Nil

/**
  * Jump to another flow.
  *
  * Example: `-> RetentionFlow`
  */
case class JumpTo(flowRef: NameExpr, span: Span) extends StageOp:
  override def children: List[LogicalPlan] = Nil

/**
  * Terminate the flow.
  *
  * Example: `end()`
  */
case class EndFlow(span: Span) extends StageOp:
  override def children: List[LogicalPlan] = Nil

/**
  * Control dependency in pipe position (for stages that have both data input and control
  * dependencies).
  *
  * Example: `from data_stage | depends on validation_stage`
  */
case class ControlDependencyOp(refs: List[NameExpr], span: Span) extends StageOp:
  override def children: List[LogicalPlan] = Nil

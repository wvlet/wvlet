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
import wvlet.lang.compiler.TermName
import wvlet.lang.model.DataType.*
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * FlowOp is a marker trait for flow/workflow operators.
  *
  * Flow operators are control flow constructs that orchestrate when and how stages are executed.
  * They extend Relation for compatibility with the existing parser infrastructure, but they are
  * semantically different from pure data transformations.
  *
  * Design rationale:
  *   - Semantic clarity: Flow operators can be distinguished via pattern matching on FlowOp
  *   - Type safety: Can use `case f: FlowOp` to match flow operators specifically
  *   - Compatibility: Extends Relation to work with existing pipe chain parsing
  *   - Cleaner compiler passes: Type checking can treat FlowOp differently from regular Relations
  *
  * Note: This is a transitional design. A future refactoring may fully separate FlowOp from
  * Relation if the execution layer requires different semantics.
  */
trait FlowOp extends Relation

/**
  * UnaryFlowOp is a flow operator with a single child relation.
  *
  * Most flow operators transform or wrap a single input relation. The relation type is propagated
  * from the child.
  */
trait UnaryFlowOp extends FlowOp with UnaryRelation:
  override def relationType: RelationType = child.relationType

/**
  * StageDef represents a named stage within a flow definition.
  *
  * A stage is a named step in a data pipeline that can:
  *   - Read from other stages (via inputRefs)
  *   - Have control dependencies (via dependsOn)
  *   - Transform data (via body)
  *
  * @param name
  *   The stage name
  * @param inputRefs
  *   References to source stages (from clause)
  * @param dependsOn
  *   References to stages for control-only dependencies
  * @param body
  *   The relation/query body of the stage (optional for control-only stages)
  * @param span
  *   Source location
  */
case class StageDef(
    name: TermName,
    inputRefs: List[NameExpr],
    dependsOn: List[NameExpr],
    body: Option[Relation],
    span: Span
) extends FlowOp:
  override def children: List[LogicalPlan] = body.toList
  override def relationType: RelationType  = body.map(_.relationType).getOrElse(EmptyRelationType)

  override def toString: String =
    val inputs =
      if inputRefs.isEmpty then
        ""
      else
        s" from ${inputRefs.map(_.fullName).mkString(", ")}"
    val deps =
      if dependsOn.isEmpty then
        ""
      else
        s" depends on ${dependsOn.map(_.fullName).mkString(", ")}"
    val bodyStr = body.map(b => s" | ${b}").getOrElse("")
    s"StageDef[${name.name}]${inputs}${deps}${bodyStr}"

/**
  * FlowRoute represents unified routing - both conditional and percentage-based.
  *
  * Routes data to different stages based on conditions or percentages.
  *
  * Example (conditional):
  * {{{
  * route {
  *   case _.age > 18 -> adult
  *   else -> minor
  * }
  * }}}
  *
  * Example (percentage with deterministic partitioning):
  * {{{
  * route by hash(user_id) {
  *   case 50 -> variant_a
  *   case 50 -> variant_b
  * }
  * }}}
  */
case class FlowRoute(
    child: Relation,
    byExpr: Option[Expression],
    cases: List[FlowRouteCase],
    elseTarget: Option[NameExpr],
    span: Span
) extends UnaryFlowOp

/**
  * FlowRouteCase represents a single case in a FlowRoute.
  *
  * Either condition or percentage should be set, not both.
  *
  * @param condition
  *   The condition expression for conditional routing (e.g., _.age > 18)
  * @param percentage
  *   The percentage for percentage-based routing (0-100)
  * @param target
  *   The target stage name to route to
  */
case class FlowRouteCase(
    condition: Option[Expression],
    percentage: Option[Int],
    target: NameExpr,
    span: Span
)

/**
  * FlowFork represents parallel execution of multiple stages.
  *
  * Executes multiple stages in parallel with the same input data.
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
case class FlowFork(child: Relation, stages: List[StageDef], span: Span) extends UnaryFlowOp

/**
  * FlowMerge represents fan-in from multiple stages.
  *
  * Merges data from multiple upstream stages, optionally with a join condition.
  *
  * Example:
  * {{{
  * -- Join multiple stages
  * merge stage_a, stage_b on _.user_id = _.user_id
  *
  * -- Union multiple stages
  * merge stage_a, stage_b | union
  * }}}
  */
case class FlowMerge(sources: List[NameExpr], joinCondition: Option[Expression], span: Span)
    extends FlowOp:
  override def children: List[LogicalPlan] = Nil // Sources are name references, not plans
  override def relationType: RelationType = EmptyRelationType // Resolved later

/**
  * FlowWait represents a time-based delay in the flow.
  *
  * Pauses execution for a specified duration.
  *
  * Example: `wait('7 days')`
  */
case class FlowWait(child: Relation, duration: Expression, span: Span) extends UnaryFlowOp

/**
  * FlowActivate represents sending data to an external system.
  *
  * Example: `activate('email', template: 'promo_v1')`
  */
case class FlowActivate(child: Relation, target: Expression, params: List[Expression], span: Span)
    extends UnaryFlowOp

/**
  * FlowJump represents a jump to another flow.
  *
  * Transfers control to another flow definition.
  *
  * Example: `-> RetentionFlow`
  */
case class FlowJump(child: Relation, targetFlow: NameExpr, span: Span) extends UnaryFlowOp

/**
  * FlowEnd represents the termination of a flow path.
  *
  * Example: `end()`
  */
case class FlowEnd(child: Relation, span: Span) extends UnaryFlowOp

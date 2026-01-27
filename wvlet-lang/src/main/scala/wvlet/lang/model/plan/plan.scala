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

import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.api.LinePosition
import wvlet.lang.api.Span
import wvlet.lang.compiler.TermName
import wvlet.lang.compiler.TypeName
import wvlet.lang.compiler.SourceFile
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.TypeParameter
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.Attribute
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.expr.QualifiedName
import wvlet.lang.model.expr.StringLiteral
import wvlet.lang.model.plan.LogicalPlan

sealed trait LanguageStatement extends TopLevelStatement with LeafPlan:
  override def isEmpty: Boolean            = true
  override def children: List[LogicalPlan] = Nil

  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

trait HasSourceFile:
  def sourceFile: SourceFile

// Top-level definition for each source file
case class PackageDef(
    name: QualifiedName,
    statements: List[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    span: Span
) extends LanguageStatement
    with HasSourceFile:
  override def isEmpty: Boolean            = statements.isEmpty
  override def children: List[LogicalPlan] = statements

case class Import(
    importRef: NameExpr,
    alias: Option[NameExpr],
    fromSource: Option[StringLiteral],
    span: Span
) extends LanguageStatement

case class TypeAlias(alias: NameExpr, sourceTypeName: NameExpr, span: Span)
    extends LanguageStatement

case class TypeDef(
    name: TypeName,
    params: List[TypeParameter],
    defContexts: List[DefContext],
    parent: Option[NameExpr],
    elems: List[TypeElem],
    span: Span
) extends LanguageStatement

// type elements (def or column (field) definition)
sealed trait TypeElem extends Expression

case class TopLevelFunctionDef(functionDef: FunctionDef, span: Span) extends LanguageStatement

// def ... { ... } in the type definition
case class FunctionDef(
    name: TermName,
    args: List[DefArg],
    defContexts: List[DefContext],
    retType: Option[DataType],
    expr: Option[Expression],
    span: Span
) extends TypeElem:
  override def children: List[Expression] = Nil

case class DefArg(
    name: TermName,
    override val dataType: DataType,
    defaultValue: Option[Expression],
    span: Span
) extends Expression:
  override def children: List[Expression] = Nil

case class FieldDef(
    name: TermName,
    fieldType: NameExpr,
    params: List[TypeParameter],
    body: Option[Expression],
    span: Span
) extends TypeElem:
  override def children: List[Expression] = Nil

/**
  * Definition scope (e.g., in xxx)
  * @param name
  * @param tpe
  * @param nodeLocation
  */
case class DefContext(name: Option[NameExpr], contextType: NameExpr, span: Span) extends Expression:
  override def children: List[Expression] = Nil

case class ModelDef(
    name: TableName,
    params: List[DefArg],
    givenRelationType: Option[RelationType],
    child: Query,
    span: Span
) extends LogicalPlan
    with HasTableName
    with LanguageStatement:
  override def children: List[LogicalPlan] = Nil

  override def relationType: RelationType = givenRelationType.getOrElse(child.relationType)

case class ValDef(name: TermName, dataType: DataType, expr: Expression, span: Span)
    extends LanguageStatement

/**
  * A partial query definition that can be applied to relations via pipe. Unlike ModelDef which
  * defines a complete query starting with 'from', PartialQueryDef defines a query fragment starting
  * with an operator (where, select, etc.) that can be composed with any relation.
  *
  * Example: def is_active = where age > 18 def core_fields = select id, name def older_than(min:
  * int) = where age > min
  *
  * Usage: from users | is_active | core_fields
  */
case class PartialQueryDef(name: TermName, params: List[DefArg], body: Relation, span: Span)
    extends LanguageStatement

/**
  * FlowDef represents a data flow/workflow definition.
  *
  * A flow is a collection of named stages that define a data pipeline with branching, merging, and
  * control flow capabilities. Unlike PartialQueryDef which is a reusable query fragment, FlowDef
  * represents a complete workflow with multiple interconnected stages.
  *
  * Example:
  * {{{
  * flow CustomerJourney(entry_segment: string) = {
  *   stage entry = from users | where segment_id = entry_segment
  *   stage check = from entry | switch { case _.active -> engaged; else -> dormant }
  *   stage engaged = from check | activate('email')
  *   stage dormant = from check | end()
  * }
  * }}}
  *
  * @param name
  *   The name of the flow
  * @param dependency
  *   Optional dependency on another flow
  * @param params
  *   Parameters for the flow
  * @param config
  *   Configuration items for the flow (e.g., schedule, timezone)
  * @param stages
  *   List of stage definitions within the flow
  * @param span
  *   Source location
  */
case class FlowDef(
    name: TermName,
    dependency: Option[FlowDependency],
    params: List[DefArg],
    config: List[ConfigItem],
    stages: List[StageDef],
    span: Span
) extends LanguageStatement

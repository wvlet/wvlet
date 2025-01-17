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
import wvlet.lang.api.{LinePosition, Span}
import wvlet.lang.compiler.{TermName, TypeName, SourceFile}
import wvlet.lang.model.DataType.{EmptyRelationType, TypeParameter}
import wvlet.lang.model.{DataType, RelationType}
import wvlet.lang.model.expr.{Attribute, Expression, NameExpr, QualifiedName, StringLiteral}
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
    tpe: NameExpr,
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
case class DefContext(name: Option[NameExpr], tpe: NameExpr, span: Span) extends Expression:
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

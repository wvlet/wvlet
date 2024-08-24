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

import wvlet.lang.compiler.{SourceFile, TermName, TypeName}
import wvlet.lang.model.DataType.{EmptyRelationType, TypeParameter}
import wvlet.lang.model.{DataType, NodeLocation, RelationType}
import wvlet.lang.model.expr.{Attribute, Expression, NameExpr, QualifiedName, StringLiteral}
import wvlet.lang.model.plan.LogicalPlan

sealed trait LanguageStatement extends LogicalPlan with LeafPlan:
  override def isEmpty: Boolean           = true
  override def children: Seq[LogicalPlan] = Nil

  override def relationType: RelationType      = EmptyRelationType
  override def inputRelationType: RelationType = EmptyRelationType

trait HasSourceFile:
  def sourceFile: SourceFile

// Top-level definition for each source file
case class PackageDef(
    name: QualifiedName,
    statements: List[LogicalPlan],
    sourceFile: SourceFile = SourceFile.NoSourceFile,
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement
    with HasSourceFile:
  override def isEmpty: Boolean           = statements.isEmpty
  override def children: Seq[LogicalPlan] = statements

case class Import(
    importRef: NameExpr,
    alias: Option[NameExpr],
    fromSource: Option[StringLiteral],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

case class TypeAlias(alias: NameExpr, sourceTypeName: NameExpr, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

case class TypeDef(
    name: TypeName,
    params: List[TypeParameter],
    defContexts: List[DefContext],
    parent: Option[NameExpr],
    elems: List[TypeElem],
    nodeLocation: Option[NodeLocation]
) extends LanguageStatement

// type elements (def or column (field) definition)
sealed trait TypeElem extends Expression

case class TopLevelFunctionDef(functionDef: FunctionDef, nodeLocation: Option[NodeLocation])
    extends LanguageStatement

// def ... { ... } in the type definition
case class FunctionDef(
    name: TermName,
    args: List[DefArg],
    defContexts: List[DefContext],
    retType: Option[DataType],
    expr: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

case class DefArg(
    name: TermName,
    override val dataType: DataType,
    defaultValue: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class FieldDef(
    name: TermName,
    tpe: NameExpr,
    params: List[TypeParameter],
    body: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends TypeElem:
  override def children: Seq[Expression] = Seq.empty

/**
  * Definition scope (e.g., in xxx)
  * @param name
  * @param tpe
  * @param nodeLocation
  */
case class DefContext(name: Option[NameExpr], tpe: NameExpr, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq.empty

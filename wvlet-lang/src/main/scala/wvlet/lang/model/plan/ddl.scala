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

import wvlet.lang.compiler.TermName
import wvlet.lang.api.LinePosition
import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*

/*
 * SQL statements for changing the table schema or catalog
 */
trait DDL extends TopLevelStatement with LeafPlan:
  override def relationType: RelationType = EmptyRelationType

case class TableDef(name: NameExpr, params: Seq[TableDefParam], span: Span) extends DDL:

  def getParam(paramName: NameExpr): Option[NameExpr] = params
    .find(_.name == paramName)
    .map(_.paramValue)

  def getType: Option[NameExpr] = params.find(_.name.fullName == "type").map(_.paramValue)

case class TableDefParam(name: NameExpr, paramValue: NameExpr, span: Span) extends Expression:
  override def children: Seq[Expression] = Nil

case class CreateSchema(
    schema: NameExpr,
    ifNotExists: Boolean,
    properties: Option[Seq[SchemaProperty]],
    span: Span
) extends DDL

case class DropSchema(schema: NameExpr, ifExists: Boolean, span: Span) extends DDL

case class DropDatabase(database: NameExpr, ifExists: Boolean, cascade: Boolean, span: Span)
    extends DDL

case class RenameDatabase(database: NameExpr, renameTo: NameExpr, span: Span) extends DDL

case class CreateTable(
    table: NameExpr,
    ifNotExists: Boolean,
    tableElems: List[TableElement],
    properties: List[(NameExpr, Expression)] = Nil,
    span: Span
) extends DDL

case class DropTable(table: NameExpr, ifExists: Boolean, span: Span) extends DDL

// Unified ALTER TABLE structure
case class AlterTable(table: NameExpr, ifExists: Boolean, operation: AlterTableOps, span: Span)
    extends DDL

// ALTER TABLE operations
sealed trait AlterTableOps:
  def nodeName: String = this.getClass.getSimpleName
  def span: Span

case class RenameTableOp(newName: NameExpr, span: Span) extends AlterTableOps

case class AddColumnOp(column: ColumnDef, ifNotExists: Boolean = false, span: Span)
    extends AlterTableOps

case class DropColumnOp(column: NameExpr, ifExists: Boolean = false, span: Span)
    extends AlterTableOps

case class RenameColumnOp(
    oldName: NameExpr,
    newName: NameExpr,
    ifExists: Boolean = false,
    span: Span
) extends AlterTableOps

case class AlterColumnSetDataTypeOp(
    column: NameExpr,
    dataType: DataType,
    using: Option[Expression] = None,
    span: Span
) extends AlterTableOps

case class AlterColumnDropNotNullOp(column: NameExpr, span: Span) extends AlterTableOps

case class AlterColumnSetDefaultOp(column: NameExpr, defaultValue: Expression, span: Span)
    extends AlterTableOps

case class AlterColumnDropDefaultOp(column: NameExpr, span: Span) extends AlterTableOps

case class AlterColumnSetNotNullOp(column: NameExpr, span: Span) extends AlterTableOps

case class SetAuthorizationOp(
    principal: NameExpr,
    principalType: Option[String] = None, // "USER" or "ROLE"
    span: Span
) extends AlterTableOps

case class SetPropertiesOp(properties: List[(NameExpr, Expression)], span: Span)
    extends AlterTableOps

case class ExecuteOp(
    command: NameExpr,
    parameters: List[(NameExpr, Expression)] = Nil,
    where: Option[Expression] = None,
    span: Span
) extends AlterTableOps

case class CreateView(viewName: NameExpr, replace: Boolean, query: Relation, span: Span) extends DDL

case class DropView(viewName: NameExpr, ifExists: Boolean, span: Span) extends DDL

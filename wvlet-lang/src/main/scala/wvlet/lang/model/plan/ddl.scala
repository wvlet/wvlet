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
import wvlet.lang.api.{LinePosition, Span, StatusCode}
import wvlet.lang.model.DataType.EmptyRelationType
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

case class DropDatabase(database: NameExpr, ifExists: Boolean, cascade: Boolean, span: Span)
    extends DDL

case class RenameDatabase(database: NameExpr, renameTo: NameExpr, span: Span) extends DDL

case class CreateTable(
    table: NameExpr,
    ifNotExists: Boolean,
    tableElems: List[ColumnDef],
    span: Span
) extends DDL


case class DropTable(table: NameExpr, ifExists: Boolean, span: Span) extends DDL

case class RenameTable(table: NameExpr, renameTo: NameExpr, span: Span) extends DDL

case class RenameColumn(table: NameExpr, column: NameExpr, renameTo: NameExpr, span: Span)
    extends DDL

case class DropColumn(table: NameExpr, column: NameExpr, span: Span) extends DDL

case class AddColumn(table: NameExpr, column: ColumnDef, span: Span) extends DDL

case class CreateView(viewName: NameExpr, replace: Boolean, query: Relation, span: Span) extends DDL

case class DropView(viewName: NameExpr, ifExists: Boolean, span: Span) extends DDL

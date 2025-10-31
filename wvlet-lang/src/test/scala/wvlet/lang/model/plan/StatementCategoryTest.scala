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

import wvlet.airspec.AirSpec
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.model.expr.Identifier
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.expr.QualifiedName
import wvlet.lang.model.plan.*

/**
  * Test for statement category classification. This verifies that statements are properly
  * classified as DDL, DML, DQL, DCL, Utility, etc., while preserving the Update trait for JDBC
  * routing.
  */
class StatementCategoryTest extends AirSpec:

  test("DDL statements should be classified as DDL") {
    // DDL trait default
    val createSchema = CreateSchema(NameExpr("schema1"), ifNotExists = false, None, NoSpan)
    createSchema.category shouldBe StatementCategory.DDL
    createSchema.isDDL shouldBe true

    val dropSchema = DropSchema(NameExpr("schema1"), ifExists = false, NoSpan)
    dropSchema.category shouldBe StatementCategory.DDL

    val createTable = CreateTable(NameExpr("table1"), ifNotExists = false, Nil, Nil, NoSpan)
    createTable.category shouldBe StatementCategory.DDL

    val dropTable = DropTable(NameExpr("table1"), ifExists = false, NoSpan)
    dropTable.category shouldBe StatementCategory.DDL

    val createView = CreateView(NameExpr("view1"), replace = false, EmptyRelation(NoSpan), NoSpan)
    createView.category shouldBe StatementCategory.DDL

    val dropView = DropView(NameExpr("view1"), ifExists = false, NoSpan)
    dropView.category shouldBe StatementCategory.DDL
  }

  test("Truncate should be DDL but kept under Update for JDBC routing") {
    val truncate = Truncate(TableOrFileName.TableName(NameExpr("table1")), NoSpan)

    // Category is DDL per SQL standards
    truncate.category shouldBe StatementCategory.DDL
    truncate.isDDL shouldBe true

    // But it's still an Update for JDBC routing
    truncate.requiresExecuteUpdate shouldBe true
    truncate.isInstanceOf[Update] shouldBe true
  }

  test("CreateTableAs should be DDL but kept under Update for JDBC routing") {
    val ctas = CreateTableAs(
      TableOrFileName.TableName(NameExpr("table1")),
      CreateMode.NoOverwrite,
      EmptyRelation(NoSpan),
      Nil,
      Nil,
      NoSpan
    )

    // Category is DDL (creates schema)
    ctas.category shouldBe StatementCategory.DDL
    ctas.isDDL shouldBe true

    // But it's still an Update for JDBC routing
    ctas.requiresExecuteUpdate shouldBe true
    ctas.isInstanceOf[Update] shouldBe true
    ctas.isInstanceOf[Save] shouldBe true
  }

  test("DML statements should be classified as DML") {
    val saveTo = SaveTo(
      EmptyRelation(NoSpan),
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      NoSpan
    )
    saveTo.category shouldBe StatementCategory.DML
    saveTo.isDML shouldBe true
    saveTo.requiresExecuteUpdate shouldBe true

    val appendTo = AppendTo(
      EmptyRelation(NoSpan),
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      NoSpan
    )
    appendTo.category shouldBe StatementCategory.DML
    appendTo.isDML shouldBe true

    val delete = Delete(
      EmptyRelation(NoSpan),
      TableOrFileName.TableName(NameExpr("table1")),
      NoSpan
    )
    delete.category shouldBe StatementCategory.DML
    delete.isDML shouldBe true

    val insertInto = InsertInto(
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      EmptyRelation(NoSpan),
      Nil,
      NoSpan
    )
    insertInto.category shouldBe StatementCategory.DML

    val insertOverwrite = InsertOverwrite(
      TableOrFileName.TableName(NameExpr("table1")),
      EmptyRelation(NoSpan),
      Nil,
      NoSpan
    )
    insertOverwrite.category shouldBe StatementCategory.DML

    val updateRows = UpdateRows(TableOrFileName.TableName(NameExpr("table1")), Nil, None, NoSpan)
    updateRows.category shouldBe StatementCategory.DML

    val insert = Insert(
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      EmptyRelation(NoSpan),
      NoSpan
    )
    insert.category shouldBe StatementCategory.DML

    val upsert = Upsert(
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      EmptyRelation(NoSpan),
      NoSpan
    )
    upsert.category shouldBe StatementCategory.DML

    val merge = Merge(
      TableOrFileName.TableName(NameExpr("table1")),
      None,
      EmptyRelation(NoSpan),
      Identifier("true"),
      None,
      None,
      NoSpan
    )
    merge.category shouldBe StatementCategory.DML
  }

  test("Query should be classified as DQL") {
    val query = Query(EmptyRelation(NoSpan), NoSpan)

    query.category shouldBe StatementCategory.DQL
    query.isDQL shouldBe true

    // Query should NOT require executeUpdate
    query.requiresExecuteUpdate shouldBe false
    query.isInstanceOf[Update] shouldBe false
  }

  test("Prepared statement operations should be classified as DCL") {
    val prepare = PrepareStatement(NameExpr("stmt1"), Query(EmptyRelation(NoSpan), NoSpan), NoSpan)
    prepare.category shouldBe StatementCategory.DCL

    val execute = ExecuteStatement(NameExpr("stmt1"), Nil, NoSpan)
    execute.category shouldBe StatementCategory.DCL

    val deallocate = DeallocateStatement(NameExpr("stmt1"), NoSpan)
    deallocate.category shouldBe StatementCategory.DCL
  }

  test("Utility commands should be classified as Utility") {
    val showQuery = ShowQuery(NameExpr("table1"), NoSpan)
    showQuery.category shouldBe StatementCategory.Utility

    val explainPlan = ExplainPlan(Query(EmptyRelation(NoSpan), NoSpan), Nil, false, false, NoSpan)
    explainPlan.category shouldBe StatementCategory.Utility

    val useSchema = UseSchema(QualifiedName(List("schema1")), NoSpan)
    useSchema.category shouldBe StatementCategory.Utility

    val describeInput = DescribeInput(NameExpr("table1"), NoSpan)
    describeInput.category shouldBe StatementCategory.Utility

    val describeOutput = DescribeOutput(NameExpr("table1"), NoSpan)
    describeOutput.category shouldBe StatementCategory.Utility
  }

  test("Update trait should imply requiresExecuteUpdate") {
    val saveTo = SaveTo(
      EmptyRelation(NoSpan),
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      NoSpan
    )
    saveTo.requiresExecuteUpdate shouldBe true

    val truncate = Truncate(TableOrFileName.TableName(NameExpr("table1")), NoSpan)
    truncate.requiresExecuteUpdate shouldBe true

    val query = Query(EmptyRelation(NoSpan), NoSpan)
    query.requiresExecuteUpdate shouldBe false
  }

  test("Helper methods should work correctly") {
    val ddl = CreateTable(NameExpr("table1"), ifNotExists = false, Nil, Nil, NoSpan)
    ddl.isDDL shouldBe true
    ddl.isDML shouldBe false
    ddl.isDQL shouldBe false

    val dml = SaveTo(
      EmptyRelation(NoSpan),
      TableOrFileName.TableName(NameExpr("table1")),
      Nil,
      NoSpan
    )
    dml.isDDL shouldBe false
    dml.isDML shouldBe true
    dml.isDQL shouldBe false

    val dql = Query(EmptyRelation(NoSpan), NoSpan)
    dql.isDDL shouldBe false
    dql.isDML shouldBe false
    dql.isDQL shouldBe true
  }

end StatementCategoryTest

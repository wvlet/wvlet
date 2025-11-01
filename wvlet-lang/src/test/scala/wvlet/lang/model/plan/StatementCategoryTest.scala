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
import wvlet.lang.model.expr.UnquotedIdentifier
import wvlet.lang.model.plan.*

/**
  * Test for statement category classification. This verifies that statements are properly
  * classified as DDL, DML, DQL, DCL, Utility, etc., while preserving the Update trait for JDBC
  * routing.
  */
class StatementCategoryTest extends AirSpec:

  test("DDL statements should be classified as DDL") {
    // DDL trait default
    val createSchema = CreateSchema(
      UnquotedIdentifier("schema1", NoSpan),
      ifNotExists = false,
      None,
      NoSpan
    )
    createSchema.category shouldBe StatementCategory.DDL
    createSchema.isDDL shouldBe true

    val dropSchema = DropSchema(UnquotedIdentifier("schema1", NoSpan), ifExists = false, NoSpan)
    dropSchema.category shouldBe StatementCategory.DDL

    val createTable = CreateTable(
      UnquotedIdentifier("table1", NoSpan),
      ifNotExists = false,
      Nil,
      Nil,
      NoSpan
    )
    createTable.category shouldBe StatementCategory.DDL

    val dropTable = DropTable(UnquotedIdentifier("table1", NoSpan), ifExists = false, NoSpan)
    dropTable.category shouldBe StatementCategory.DDL

    val createView = CreateView(
      UnquotedIdentifier("view1", NoSpan),
      replace = false,
      EmptyRelation(NoSpan),
      NoSpan
    )
    createView.category shouldBe StatementCategory.DDL

    val dropView = DropView(UnquotedIdentifier("view1", NoSpan), ifExists = false, NoSpan)
    dropView.category shouldBe StatementCategory.DDL
  }

  test("Truncate should be DDL but kept under Update for JDBC routing") {
    val truncate = Truncate(UnquotedIdentifier("table1", NoSpan), NoSpan)

    // Category is DDL per SQL standards
    truncate.category shouldBe StatementCategory.DDL
    truncate.isDDL shouldBe true

    // But it's still an Update for JDBC routing
    truncate.requiresExecuteUpdate shouldBe true
    truncate.isInstanceOf[Update] shouldBe true
  }

  test("CreateTableAs should be DDL but kept under Update for JDBC routing") {
    val ctas = CreateTableAs(
      UnquotedIdentifier("table1", NoSpan),
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
    val saveTo = SaveTo(EmptyRelation(NoSpan), UnquotedIdentifier("table1", NoSpan), Nil, NoSpan)
    saveTo.category shouldBe StatementCategory.DML
    saveTo.isDML shouldBe true
    saveTo.requiresExecuteUpdate shouldBe true

    val appendTo = AppendTo(
      EmptyRelation(NoSpan),
      UnquotedIdentifier("table1", NoSpan),
      Nil,
      NoSpan
    )
    appendTo.category shouldBe StatementCategory.DML
    appendTo.isDML shouldBe true

    val delete = Delete(EmptyRelation(NoSpan), UnquotedIdentifier("table1", NoSpan), NoSpan)
    delete.category shouldBe StatementCategory.DML
    delete.isDML shouldBe true

    val insertInto = InsertInto(
      UnquotedIdentifier("table1", NoSpan),
      Nil,
      EmptyRelation(NoSpan),
      Nil,
      NoSpan
    )
    insertInto.category shouldBe StatementCategory.DML

    val insertOverwrite = InsertOverwrite(
      UnquotedIdentifier("table1", NoSpan),
      EmptyRelation(NoSpan),
      Nil,
      NoSpan
    )
    insertOverwrite.category shouldBe StatementCategory.DML

    val updateRows = UpdateRows(UnquotedIdentifier("table1", NoSpan), Nil, None, NoSpan)
    updateRows.category shouldBe StatementCategory.DML

    val insert = Insert(UnquotedIdentifier("table1", NoSpan), Nil, EmptyRelation(NoSpan), NoSpan)
    insert.category shouldBe StatementCategory.DML

    val upsert = Upsert(UnquotedIdentifier("table1", NoSpan), Nil, EmptyRelation(NoSpan), NoSpan)
    upsert.category shouldBe StatementCategory.DML

    val merge = Merge(
      UnquotedIdentifier("table1", NoSpan),
      None,
      EmptyRelation(NoSpan),
      UnquotedIdentifier("true", NoSpan),
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
    val prepare = PrepareStatement(
      UnquotedIdentifier("stmt1", NoSpan),
      Query(EmptyRelation(NoSpan), NoSpan),
      NoSpan
    )
    prepare.category shouldBe StatementCategory.DCL

    val execute = ExecuteStatement(UnquotedIdentifier("stmt1", NoSpan), Nil, NoSpan)
    execute.category shouldBe StatementCategory.DCL

    val deallocate = DeallocateStatement(UnquotedIdentifier("stmt1", NoSpan), NoSpan)
    deallocate.category shouldBe StatementCategory.DCL
  }

  test("Utility commands should be classified as Utility") {
    val showQuery = ShowQuery(UnquotedIdentifier("table1", NoSpan), NoSpan)
    showQuery.category shouldBe StatementCategory.Utility

    val explainPlan = ExplainPlan(Query(EmptyRelation(NoSpan), NoSpan), Nil, false, false, NoSpan)
    explainPlan.category shouldBe StatementCategory.Utility

    val useSchema = UseSchema(UnquotedIdentifier("schema1", NoSpan), NoSpan)
    useSchema.category shouldBe StatementCategory.Utility

    val describeInput = DescribeInput(UnquotedIdentifier("table1", NoSpan), NoSpan)
    describeInput.category shouldBe StatementCategory.Utility

    val describeOutput = DescribeOutput(UnquotedIdentifier("table1", NoSpan), NoSpan)
    describeOutput.category shouldBe StatementCategory.Utility
  }

  test("Update trait should imply requiresExecuteUpdate") {
    val saveTo = SaveTo(EmptyRelation(NoSpan), UnquotedIdentifier("table1", NoSpan), Nil, NoSpan)
    saveTo.requiresExecuteUpdate shouldBe true

    val truncate = Truncate(UnquotedIdentifier("table1", NoSpan), NoSpan)
    truncate.requiresExecuteUpdate shouldBe true

    val query = Query(EmptyRelation(NoSpan), NoSpan)
    query.requiresExecuteUpdate shouldBe false
  }

  test("Helper methods should work correctly") {
    val ddl = CreateTable(
      UnquotedIdentifier("table1", NoSpan),
      ifNotExists = false,
      Nil,
      Nil,
      NoSpan
    )
    ddl.isDDL shouldBe true
    ddl.isDML shouldBe false
    ddl.isDQL shouldBe false

    val dml = SaveTo(EmptyRelation(NoSpan), UnquotedIdentifier("table1", NoSpan), Nil, NoSpan)
    dml.isDDL shouldBe false
    dml.isDML shouldBe true
    dml.isDQL shouldBe false

    val dql = Query(EmptyRelation(NoSpan), NoSpan)
    dql.isDDL shouldBe false
    dql.isDML shouldBe false
    dql.isDQL shouldBe true
  }

end StatementCategoryTest

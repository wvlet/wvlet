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
package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv

/**
  * Column defaults on `table` declarations (#1997): `name: type = expr` fields render a DEFAULT
  * clause into generated CREATE TABLE columns, on both explicit `create table` actions and
  * append-to auto-creation. Engines without column defaults (Trino) reject the create loudly
  * instead of dropping the semantics
  */
class TableColumnDefaultsTest extends UniTest:

  private val usersTable =
    """table du_users = {
      |  id: int
      |  name: string
      |  active: boolean = true
      |  created_at: timestamp = now()
      |}
      |""".stripMargin

  private def compileToSQL(
      typeDefs: String,
      query: String,
      dbType: DBType = DBType.DuckDB
  ): String =
    val queryUnit = CompilationUnit.fromWvletString(query)
    val compiler  = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val typeUnit  = CompilationUnit.fromWvletString(typeDefs)
    val result    = compiler.compileMultipleUnits(List(typeUnit), queryUnit)
    GenSQL.generateSQL(queryUnit)(using result.context)

  test("should render declared defaults into an explicit create table") {
    val sql = compileToSQL(usersTable, "create table du_users\n")
    sql shouldContain "create table"
    sql shouldContain "default true"
    sql shouldContain "default now()"
    // Columns without a default stay plain
    sql.contains("id int default") shouldBe false
  }

  test("should render declared defaults into append-to auto-creation") {
    val sql = compileToSQL(
      usersTable,
      "from [[1, 'a', false, now()]] as t(id, name, active, created_at)\nappend to du_users\n"
    )
    sql shouldContain "create table if not exists"
    sql shouldContain "default true"
    sql shouldContain "default now()"
    sql shouldContain "insert into"
  }

  test("should propagate defaults through extends mixins") {
    val sql = compileToSQL(
      """type du_audited = {
        |  deleted: boolean = false
        |}
        |table du_events extends du_audited = {
        |  id: int
        |}
        |""".stripMargin,
      "create table du_events\n"
    )
    sql shouldContain "default false"
  }

  test("should propagate defaults through like-based declarations") {
    val sql = compileToSQL(
      usersTable + "table du_users_backup like du_users\n",
      "create table du_users_backup\n"
    )
    sql shouldContain "default true"
    sql shouldContain "default now()"
  }

  test("should reject creating a table with defaults on engines without DEFAULT clauses") {
    val e = intercept[WvletLangException] {
      compileToSQL(usersTable, "create table du_users\n", dbType = DBType.Trino)
    }
    e.getMessage shouldContain "column default values is not supported on"
  }

  test("should reject append auto-creation with defaults on engines without DEFAULT clauses") {
    val e = intercept[WvletLangException] {
      compileToSQL(
        usersTable,
        "from [[1, 'a', false, now()]] as t(id, name, active, created_at)\nappend to du_users\n",
        dbType = DBType.Trino
      )
    }
    e.getMessage shouldContain "column default values is not supported on"
  }

  test("should keep default-free declarations working on Trino") {
    val sql = compileToSQL(
      "table du_plain = {\n  id: int\n  name: string\n}\n",
      "create table du_plain\n",
      dbType = DBType.Trino
    )
    sql shouldContain "create table"
    sql.contains("default") shouldBe false
  }

end TableColumnDefaultsTest

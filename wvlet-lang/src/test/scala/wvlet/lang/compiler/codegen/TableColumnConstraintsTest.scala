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
  * Column constraints on `table` declarations (#1997): `primary key`, `unique`, and `not null` as
  * soft words in field position render into generated CREATE TABLE columns on both explicit
  * `create table` actions and append-to auto-creation. Engines without key constraints (Trino)
  * reject the create loudly instead of dropping the semantics
  */
class TableColumnConstraintsTest extends UniTest:

  private val usersTable =
    """table dc_users = {
      |  id: int primary key
      |  email: string unique not null
      |  name: string
      |  active: boolean not null = true
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

  test("should render declared constraints into an explicit create table") {
    val sql = compileToSQL(usersTable, "create table dc_users\n")
    sql shouldContain "primary key"
    sql shouldContain "unique not null"
    sql shouldContain "not null default true"
    // Unconstrained columns stay plain
    sql.contains("name string primary") shouldBe false
  }

  test("should render declared constraints into append-to auto-creation") {
    val sql = compileToSQL(
      usersTable,
      "from [[1, 'a@x.com', 'a', true]] as t(id, email, name, active)\nappend to dc_users\n"
    )
    sql shouldContain "create table if not exists"
    sql shouldContain "primary key"
    sql shouldContain "unique not null"
  }

  test("should propagate constraints through extends and like") {
    val sql = compileToSQL(
      usersTable + "table dc_users_backup like dc_users\n",
      "create table dc_users_backup\n"
    )
    sql shouldContain "primary key"
    sql shouldContain "unique not null"
  }

  test("should keep a field named after a constraint word parsing as a field") {
    val sql = compileToSQL(
      """table dc_odd = {
        |  id: int primary key
        |  unique: string
        |  key: string
        |}
        |""".stripMargin,
      "create table dc_odd\n"
    )
    // SQL-keyword column names render quoted, proving they parsed as fields, not constraints
    sql shouldContain "\"unique\" string"
    sql shouldContain "\"key\" string"
  }

  test("should reject key constraints on engines without them") {
    val e = intercept[WvletLangException] {
      compileToSQL(
        "table dc_pk = {\n  id: int primary key\n  name: string\n}\n",
        "create table dc_pk\n",
        dbType = DBType.Trino
      )
    }
    e.getMessage shouldContain "key constraints"
  }

  test("should reject key constraints in append auto-creation on engines without them") {
    val e = intercept[WvletLangException] {
      compileToSQL(
        "table dc_pk = {\n  id: int primary key\n  name: string\n}\n",
        "from [[1, 'a']] as t(id, name)\nappend to dc_pk\n",
        dbType = DBType.Trino
      )
    }
    e.getMessage shouldContain "key constraints"
  }

end TableColumnConstraintsTest

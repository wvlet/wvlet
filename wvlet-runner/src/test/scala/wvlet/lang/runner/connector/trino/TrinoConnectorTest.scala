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
package wvlet.lang.runner.connector.trino

import wvlet.lang.compiler.connector.QueryResult
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.test.WvletDITest

import java.io.File

class TrinoConnectorTest extends WvletDITest:

  initDesign { d =>
    d.bindInstance[TestTrinoServer](new TestTrinoServer().withDeltaLakePlugin)
      .bindProvider { (server: TestTrinoServer) =>
        TrinoConfig(
          catalog = "memory",
          schema = "main",
          hostAndPort = server.address,
          useSSL = false,
          user = Some("test"),
          password = Some("")
        )
      }
  }

  // All test SQL flows through `asSqlConnector` so this suite no longer depends on
  // `trino-jdbc` — only `trino-testing`'s in-process `TestingTrinoServer` (PR-C). PR-D removes
  // the JDBC half of `TrinoConnector` along with the `trino-jdbc` dep.
  private given QueryProgressMonitor = QueryProgressMonitor.noOp

  private def render(r: QueryResult): String =
    val cols = r.columns.map(_.name.name)
    r.rows
      .map { row =>
        cols
          .iterator
          .zip(row.values.iterator)
          .map { case (n, v) =>
            s""""${n}":${v.map(s => s""""${s}"""").getOrElse("null")}"""
          }
          .mkString("{", ",", "}")
      }
      .mkString("[", ",", "]")

  test("Create an in-memory schema and table") {
    val trino = dep[TrinoConnector].asSqlConnector
    trino.execute("create schema if not exists memory.main")
    trino.execute("create table memory.main.a(id bigint)")

    test("describe the table") {
      val r = trino.execute("describe memory.main.a")
      r.rowCount shouldBe 1
      r.rows.head.values.head shouldBe Some("id")
    }

    test("drop table") {
      trino.execute("drop table if exists memory.main.a")
      val r = trino.execute(
        "select count(*) as c from information_schema.tables where table_schema = 'main' and table_name = 'a'"
      )
      r.rows.head.values.head shouldBe Some("0")
    }

    test("drop schema") {
      trino.execute("drop schema if exists memory.main")
    }

    test("Create delta Lake table") {
      val baseDir = new File(sys.props("user.dir")).getAbsolutePath
      trino.execute("create schema if not exists delta.delta_db")

      test("create a local delta lake file") {
        trino.execute("create table delta.delta_db.a as select 1 as id, 'leo' as name")
        trino.execute("insert into delta.delta_db.a values(2, 'yui')")
        val r = trino.execute("select * from delta.delta_db.a order by id")
        debug(render(r))
        r.rowCount shouldBe 2
      }

      test("register a local delta lake table") {
        trino.execute(
          s"call delta.system.register_table(schema_name => 'delta_db', table_name => 'www_access', table_location => 'file://${baseDir}/spec/delta/data/www_access')"
        )
        val r = trino.execute("select * from delta.delta_db.www_access limit 5")
        debug(render(r))
        r.rowCount shouldBe 5
      }
    }

    test("list functions") {
      val functions = dep[TrinoConnector].listFunctions("memory")
      debug(functions.mkString("\n"))
      functions.nonEmpty shouldBe true
    }

    test("asSqlConnector executes a trivial select") {
      val result = trino.execute("select 1 as one, 'http' as src")
      result.columnCount shouldBe 2
      result.rowCount shouldBe 1
      result.columns.map(_.name.name) shouldBe List("one", "src")
      result.rows.head.values shouldBe List(Some("1"), Some("http"))
    }
  }

end TrinoConnectorTest

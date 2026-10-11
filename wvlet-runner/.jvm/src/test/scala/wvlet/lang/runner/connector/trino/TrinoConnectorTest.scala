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

import wvlet.lang.connector.trino.TrinoConfig
import wvlet.lang.connector.trino.TrinoConnector

import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.test.WvletDITest

class TrinoConnectorTest extends WvletDITest:

  initDesign { d =>
    d.bindInstance[TestTrinoServer](new TestTrinoServer().withMemoryPlugin)
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

    test("stream query results in batches over the HTTP protocol") {
      val handle = trino.submit("select * from unnest(sequence(1, 1000)) as t(id)")
      try
        val batches = handle.batches().toList
        batches.map(_.rowCount).sum shouldBe 1000
        batches.foreach(b => b.columns.map(_.name.name) shouldBe List("id"))
      finally
        handle.close()
    }

    test("stream JSON rows through the paginated result") {
      val connector = dep[TrinoConnector]
      val rowCount  =
        connector.streamJsonRows("select * from unnest(sequence(1, 1000)) as t(id)")(_.size)
      rowCount shouldBe 1000
      // queryJsonRows keeps the materialized List behavior on top of the stream
      val rows = connector.queryJsonRows("select 1 as id")
      rows.size shouldBe 1
      rows.head shouldContain "id"
      rows.head shouldContain "1"
    }
  }

end TrinoConnectorTest

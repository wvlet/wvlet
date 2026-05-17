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

import wvlet.lang.runner.codec.JDBCCodec
import wvlet.lang.runner.codec.JDBCCodec.ResultSetCodec
import wvlet.lang.test.WvletDITest
import wvlet.uni.control.Control
import wvlet.uni.control.Control.withResource

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

  test("Create an in-memory schema and table") {
    val trino = dep[TrinoConnector]
    trino.createSchema("memory", "main")
    trino.getSchema("memory", "main") shouldBe defined

    trino.executeUpdate("create table a(id bigint)")

    trino.getTableDef("memory", "main", "a") shouldBe defined

    test("drop table") {
      val trino = dep[TrinoConnector]
      trino.dropTable("memory", "main", "a")
      trino.getTableDef("memory", "main", "a") shouldBe empty
    }

    test("drop schema") {
      val trino = dep[TrinoConnector]
      trino.dropSchema("memory", "main")
    }

    test("Create delta Lake table") {
      // The schema is created on the (shared) TestTrinoServer once here; nested tests below
      // derive their own delta-configured connector via dep[TrinoConnector].withConfig(...).
      val trino      = dep[TrinoConnector]
      val baseDir    = new File(sys.props("user.dir")).getAbsolutePath
      val trinoDelta = trino.withConfig(trino.config.copy(catalog = "delta", schema = "delta_db"))
      trinoDelta.createSchema("delta", "delta_db")

      test("create a local delta lake file") {
        val trinoDelta = dep[TrinoConnector].withConfig(
          dep[TrinoConfig].copy(catalog = "delta", schema = "delta_db")
        )
        trinoDelta.executeUpdate("create table a as select 1 as id, 'leo' as name")
        trinoDelta.executeUpdate("insert into a values(2, 'yui')")
        trinoDelta.runQuery("select * from a"): rs =>
          val queryResultJson = ResultSetCodec(rs).toJson
          debug(queryResultJson)
      }

      test("register a local delta lake table") {
        val trinoDelta = dep[TrinoConnector].withConfig(
          dep[TrinoConfig].copy(catalog = "delta", schema = "delta_db")
        )
        trinoDelta.execute(
          s"call delta.system.register_table(schema_name => 'delta_db', table_name => 'www_access', table_location => 'file://${baseDir}/spec/delta/data/www_access')"
        )
        trinoDelta.runQuery("select * from www_access limit 5") { rs =>
          val queryResultJson = ResultSetCodec(rs).toJson
          debug(queryResultJson)
        }
      }
    }

    test("list functions") {
      val trino     = dep[TrinoConnector]
      val functions = trino.listFunctions("memory")
      debug(functions.mkString("\n"))
    }

    // Sanity for PR-A: the new `asSqlConnector` accessor talks to the same in-process server
    // via the HTTP client (uni's `HttpSyncClient`) — no `trino-jdbc` on this path. PR-B will
    // switch `QueryExecutor` over to this; for now both code paths coexist.
    test("asSqlConnector executes against the same server via HTTP") {
      val trino                                            = dep[TrinoConnector]
      given wvlet.lang.compiler.query.QueryProgressMonitor =
        wvlet.lang.compiler.query.QueryProgressMonitor.noOp
      val result = trino.asSqlConnector.execute("select 1 as one, 'http' as src")
      result.columnCount shouldBe 2
      result.rowCount shouldBe 1
      result.columns.map(_.name.name) shouldBe List("one", "src")
      result.rows.head.values shouldBe List(Some("1"), Some("http"))
    }
  }

end TrinoConnectorTest

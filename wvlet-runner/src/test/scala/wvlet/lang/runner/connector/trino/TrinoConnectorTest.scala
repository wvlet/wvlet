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

import wvlet.airframe.Design
import wvlet.airframe.codec.JDBCCodec
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource
import wvlet.airspec.AirSpec

import java.io.File

class TrinoConnectorTest extends AirSpec:

  initDesign { d =>
    d.bindInstance[TestTrinoServer](new TestTrinoServer())
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

  test("Create an in-memory schema and table") { (trino: TrinoConnector) =>
    trino.createSchema("memory", "main")
    trino.getSchema("memory", "main") shouldBe defined

    trino.withConnection: conn =>
      conn.createStatement().execute("create table a(id bigint)")

    trino.getTableDef("memory", "main", "a") shouldBe defined

    test("drop table") {
      trino.dropTable("memory", "main", "a")
      trino.getTableDef("memory", "main", "a") shouldBe empty
    }

    test("drop schema") {
      trino.dropSchema("memory", "main")
    }

    test("Create delta Lake table") {
      val baseDir = new File(sys.props("user.dir")).getAbsolutePath

      val trinoDelta = trino.withConfig(trino.config.copy(catalog = "delta", schema = "delta_db"))
      trinoDelta.createSchema("delta", "delta_db")
      test("create a local delta lake file") {
        trinoDelta.withConnection { conn =>
          withResource(conn.createStatement()): stmt =>
            stmt.execute("create table a as select 1 as id, 'leo' as name")

            stmt.execute("insert into a values(2, 'yui')")

            withResource(stmt.executeQuery("select * from a")): rs =>
              val queryResultJson = ResultSetCodec(rs).toJson
              debug(queryResultJson)
        }
      }

      test("register a local delta lake table") {
        trinoDelta.withConnection: conn =>
          withResource(conn.createStatement()) { stmt =>
            stmt.execute(
              s"call delta.system.register_table(schema_name => 'delta_db', table_name => 'www_access', table_location => 'file://${baseDir}/spec/delta/data/www_access')"
            )
            withResource(stmt.executeQuery("select * from www_access limit 5")) { rs =>
              val queryResultJson = ResultSetCodec(rs).toJson
              debug(queryResultJson)
            }
          }
      }
    }

    test("list functions") {
      val functions = trino.listFunctions("memory")
      debug(functions.mkString("\n"))
    }
  }

end TrinoConnectorTest

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
package wvlet.lang.server

import wvlet.lang.runner.ErrorResult
import wvlet.lang.runner.TableRows
import wvlet.lang.runner.connector.WvletServerClient
import wvlet.lang.test.WvletDITest
import wvlet.uni.http.netty.NettyHttpServer

/**
  * End-to-end tests for [[WvletServerClient]] — the remote wvlet-server execution backend used by
  * `wvc run -t wvlet` and the Node CLI — against a live in-process server (#1963 phase 5)
  */
class WvletServerClientTest extends WvletDITest:

  initDesign:
    _.add(WvletServer.testDesign)

  private def withClient[U](body: WvletServerClient => U): U =
    val server = dep[NettyHttpServer]
    val client = WvletServerClient(s"http://localhost:${server.localPort}")
    try body(client)
    finally client.close()

  test("run a wvlet query remotely and adapt structured rows") {
    withClient { client =>
      val result = client.runQuery("from [[1, 'a'], [2, 'b']] as t(id, name) select id, name")
      result match
        case t: TableRows =>
          t.schema.fields.map(_.name.name) shouldBe List("id", "name")
          t.totalRows shouldBe 2
          t.rows.map(_.values.map(v => Option(v).map(_.toString).orNull).toList) shouldBe
            List(List("1", "a"), List("2", "b"))
        case other =>
          fail(s"Expected TableRows, got: ${other}")
    }
  }

  test("preserve session state across statements of one client") {
    withClient { client =>
      client.runQuery("execute sql\"create table remote_nums(n integer)\"")
      client.runQuery("execute sql\"insert into remote_nums values (1), (2), (3)\"")
      val result = client.runQuery("from remote_nums select count(*) as cnt")
      result match
        case t: TableRows =>
          t.rows.head.values.head.toString shouldBe "3"
        case other =>
          fail(s"Expected TableRows, got: ${other}")
    }
  }

  test("evaluate test statements server-side") {
    withClient { client =>
      val result = client.runQuery("""from [[1], [2]] as t(a) select a
                                     |test _.size should be 2""".stripMargin)
      result.hasError shouldBe false
    }
  }

  test("surface remote failures as errors") {
    withClient { client =>
      val result = client.runQuery("from table_that_does_not_exist_on_server")
      result match
        case e: ErrorResult =>
          Option(e.e.getMessage).getOrElse("") shouldContain "table_that_does_not_exist_on_server"
        case other =>
          fail(s"Expected ErrorResult, got: ${other}")
    }
  }

end WvletServerClientTest

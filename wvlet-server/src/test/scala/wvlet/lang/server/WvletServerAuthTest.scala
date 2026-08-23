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

import wvlet.lang.runner.TableRows
import wvlet.lang.runner.connector.WvletServerClient
import wvlet.lang.test.WvletDITest
import wvlet.uni.http.netty.NettyHttpServer

/**
  * End-to-end tests for bearer-token authentication on the wvlet server: RPC requests must carry
  * `Authorization: Bearer <token>` when the server is started with an auth token, while static Web
  * UI assets stay reachable without one.
  */
class WvletServerAuthTest extends WvletDITest:

  private val serverToken = "test-secret-token"

  initDesign:
    _.add(WvletServer.testDesign(WvletServerConfig(authToken = Some(serverToken))))

  private def withClient[U](token: Option[String])(body: WvletServerClient => U): U =
    val server = dep[NettyHttpServer]
    val client = WvletServerClient(s"http://localhost:${server.localPort}", token = token)
    try body(client)
    finally client.close()

  test("accept queries carrying the correct bearer token") {
    withClient(Some(serverToken)) { client =>
      val result = client.runQuery("from [[1], [2]] as t(a) select a")
      result match
        case t: TableRows =>
          t.totalRows shouldBe 2
        case other =>
          fail(s"Expected TableRows, got: ${other}")
    }
  }

  test("reject queries without a token") {
    withClient(None) { client =>
      val e = intercept[Exception] {
        client.runQuery("select 1")
      }
      e.getMessage shouldContain "bearer token"
    }
  }

  test("reject queries with a wrong token") {
    withClient(Some("wrong-token")) { client =>
      val e = intercept[Exception] {
        client.runQuery("select 1")
      }
      e.getMessage shouldContain "bearer token"
    }
  }

end WvletServerAuthTest

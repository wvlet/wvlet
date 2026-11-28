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
package wvlet.lang.ui.server

import org.scalajs.dom
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.uni.http.Http
import wvlet.uni.http.ServiceException
import wvlet.uni.http.ServiceStatus
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

import scala.util.Try

/**
  * The RPC client of the server UI. When the server requires a bearer token (`--auth-token`), the
  * first rejected call sets [[needsToken]], the UI asks for the token, and [[setToken]] keeps it in
  * the browser's session storage, so it lasts until the tab closes.
  */
class ServerConnection:
  private val storageKey = "wvlet.server.token"

  private var token: Option[String] =
    Try(Option(dom.window.sessionStorage.getItem(storageKey))).toOption.flatten.filter(_.nonEmpty)

  private var client: RPCAsyncClient = newClient(token)

  /** Whether the server rejected a call for a missing or wrong token */
  val needsToken: RxVar[Boolean] = Rx.variable(false)

  /** The current client; it changes when the token changes */
  def rpc: RPCAsyncClient = client

  def hasToken: Boolean = token.nonEmpty

  /** Use `t` as the bearer token of later calls */
  def setToken(t: String): Unit =
    token = Some(t.trim).filter(_.nonEmpty)
    Try(
      token match
        case Some(v) =>
          dom.window.sessionStorage.setItem(storageKey, v)
        case None =>
          dom.window.sessionStorage.removeItem(storageKey)
    )
    client = newClient(token)
    needsToken := false

  /** The message of a failed call, asking for a token when the server rejected it */
  def errorMessage(e: Throwable): String =
    e match
      case s: ServiceException if s.status == ServiceStatus.UNAUTHENTICATED_U13 =>
        needsToken := true
        "The server requires a token"
      case s: ServiceException =>
        Option(s.message).filter(_.nonEmpty).getOrElse(s.status.toString)
      case other =>
        Option(other.getMessage).getOrElse(other.getClass.getSimpleName)

  private def newClient(t: Option[String]): RPCAsyncClient = FrontendRPC.newRPCAsyncClient(
    Http
      .client
      .withRequestFilter(req => t.map(v => req.setHeader("Authorization", s"Bearer ${v}")).getOrElse(req))
      .newAsyncClient
  )

end ServerConnection

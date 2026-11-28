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
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.Input

/**
  * The web UI of `wvlet server`: a header with the pages (overview, queries, flow runs) and the
  * current page below it, chosen by the URL hash. Asks for the bearer token when the server
  * rejects a call for one
  */
class ServerUI(conn: ServerConnection) extends RxElement:

  private val tokenInput = Rx.variable("")

  private def navItem(name: String, target: Page, current: Page): RxElement =
    val selected =
      (target, current) match
        case (Page.Queries, Page.Query(_)) =>
          true
        case _ =>
          target == current
    a(
      cls -> (
        if selected then
          "wv-nav__item wv-nav__item--selected"
        else
          "wv-nav__item"
      ),
      href    -> target.hash,
      onclick -> { (e: dom.MouseEvent) =>
        e.preventDefault()
        Page.go(target)
      },
      name
    )

  private def headerBar: RxElement = wvlet.uni.dom.HtmlTags.tag("header")(
    cls -> "wv-header",
    a(
      cls  -> "wv-brand",
      href -> "#/",
      img(cls -> "wv-brand__logo", src -> "./img/wvlet-icon.svg", alt -> ""),
      span("Wvlet"),
      span(cls -> "wv-brand__sub", "server")
    ),
    Page
      .current
      .map { r =>
        nav(
          cls       -> "wv-nav",
          ariaLabel -> "Pages",
          navItem("Overview", Page.Overview, r),
          navItem("Queries", Page.Queries, r),
          navItem("Flow runs", Page.Flows, r)
        )
      },
    div(cls -> "wv-grow"),
    a(cls -> "wv-header__link", href -> "https://wvlet.org/wvlet/docs/usage/ui", target -> "_blank", "Docs")
  )

  private def tokenPrompt: RxElement = conn
    .needsToken
    .map { need =>
      if !need then
        span()
      else
        div(
          cls  -> "wv-token",
          role -> "dialog",
          ariaLabel -> "Server token",
          div(
            cls -> "wv-token__box",
            h2("This server requires a token"),
            p(
              cls -> "wv-muted",
              "Enter the server's --auth-token (or WVLET_SERVER_TOKEN). It is kept in this tab only."
            ),
            Input(tokenInput).withType("password").withLabel("Token").withPlaceholder("Token"),
            div(
              cls -> "wv-token__actions",
              Button("Connect")
                .withVariant(ButtonVariant.Primary)
                .onClick(() => conn.setToken(tokenInput.get))
            )
          )
        )
    }

  private def page(r: Page): RxElement =
    r match
      case Page.Overview =>
        OverviewPage(conn)
      case Page.Queries =>
        QueryListPage(conn)
      case Page.Query(id) =>
        QueryDetailPage(conn, id)
      case Page.Flows =>
        FlowRunsPage(conn)

  override def render: RxElement = div(
    cls -> "wv-app",
    headerBar,
    wvlet.uni.dom.HtmlTags.tag("main")(cls -> "wv-main", Page.current.map(page)),
    tokenPrompt
  )

end ServerUI

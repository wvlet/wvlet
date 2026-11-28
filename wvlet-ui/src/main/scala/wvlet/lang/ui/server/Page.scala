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
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar

/** A page of the server UI, addressed by the URL hash so each page can be bookmarked */
enum Page:
  case Overview
  case Queries
  case Query(queryId: String)
  case Flows

  /** The hash of this page, e.g. `#/queries/<id>` */
  def hash: String =
    this match
      case Overview =>
        "#/"
      case Queries =>
        "#/queries"
      case Query(id) =>
        s"#/queries/${scala.scalajs.js.URIUtils.encodeURIComponent(id)}"
      case Flows =>
        "#/flows"

object Page:

  /** The page of a URL hash; unknown hashes show the overview */
  def parse(hash: String): Page =
    hash.stripPrefix("#").stripPrefix("/").split("/").toList.filter(_.nonEmpty) match
      case "queries" :: Nil =>
        Queries
      case "queries" :: id :: _ =>
        Query(scala.scalajs.js.URIUtils.decodeURIComponent(id))
      case "flows" :: _ =>
        Flows
      case _ =>
        Overview

  /** The current page, following the browser's hash changes */
  lazy val current: RxVar[Page] =
    val v = Rx.variable(parse(dom.window.location.hash))
    dom.window.addEventListener("hashchange", (_: dom.Event) => v := parse(dom.window.location.hash))
    v

  /** Show another page */
  def go(r: Page): Unit =
    if dom.window.location.hash != r.hash then
      dom.window.location.hash = r.hash
    current := r

end Page

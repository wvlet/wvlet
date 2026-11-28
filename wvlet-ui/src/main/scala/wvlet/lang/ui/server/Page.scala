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

import scala.scalajs.js.URIUtils

/**
  * A screen of the server UI, addressed by the URL hash so a selected query can be bookmarked and
  * shared
  */
enum Page:
  /** The workload console, with the query `selected` open in its inspector */
  case Console(selected: Option[String] = None)
  case Flows

  /** The hash of this screen, e.g. `#/q/<id>` */
  def hash: String =
    this match
      case Console(None) =>
        "#/"
      case Console(Some(id)) =>
        s"#/q/${URIUtils.encodeURIComponent(id)}"
      case Flows =>
        "#/flows"

object Page:

  /**
    * The screen of a URL hash. `#/queries/<id>` links from the first UI still open the query;
    * unknown hashes show the console
    */
  def parse(hash: String): Page =
    hash.stripPrefix("#").stripPrefix("/").split("/").toList.filter(_.nonEmpty) match
      case ("q" | "queries") :: id :: _ =>
        Console(Some(URIUtils.decodeURIComponent(id)))
      case "flows" :: _ =>
        Flows
      case _ =>
        Console(None)

  /**
    * The current screen, following the browser's hash changes. Selecting another query in the
    * console rewrites the hash without changing this, so the console stays mounted
    */
  lazy val current: RxVar[Page] =
    val v = Rx.variable(parse(dom.window.location.hash))
    dom
      .window
      .addEventListener(
        "hashchange",
        (_: dom.Event) =>
          val p = parse(dom.window.location.hash)
          if v.get != p then
            v := p
      )
    v

  /**
    * Show another screen. Selecting a query replaces the history entry, so Back leaves the console
    * instead of stepping through every row the user looked at
    */
  def go(r: Page): Unit =
    (current.get, r) match
      case (Console(_), Console(_)) =>
        if dom.window.location.hash != r.hash then
          dom.window.history.replaceState(null, "", r.hash)
      case _ =>
        if dom.window.location.hash != r.hash then
          dom.window.location.hash = r.hash
        current := r

end Page

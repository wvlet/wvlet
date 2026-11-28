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
package wvlet.lang.ui

import org.scalajs.dom
import wvlet.lang.ui.server.ServerConnection
import wvlet.lang.ui.server.ServerUI
import wvlet.uni.dom.all.{*, given}

/** Entry point of the web UI that `wvlet server` serves */
object WvletUIMain:
  def main(args: Array[String]): Unit =
    // uni-ui's themes follow data-theme on <html>; start from the system's preference
    val dark = dom.window.matchMedia("(prefers-color-scheme: dark)").matches
    dom
      .document
      .documentElement
      .setAttribute(
        "data-theme",
        if dark then
          "dark"
        else
          "light"
      )
    ServerUI(ServerConnection()).renderTo("main")

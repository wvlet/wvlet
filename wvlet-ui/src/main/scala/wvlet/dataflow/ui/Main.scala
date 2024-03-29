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
package wvlet.dataflow.ui

import wvlet.airframe.rx.html.DOMRenderer
import wvlet.log.LogSupport
import org.scalajs.dom

object Main extends LogSupport:
  def main(args: Array[String]): Unit =
    renderMain()

  def renderMain(): Unit =
    val mainPanel = new MainPanel()
    mainPanel.renderTo("main")

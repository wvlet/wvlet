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

import wvlet.airframe.Design
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.MainFrame
import wvlet.lang.ui.editor.WvletEditor

object WvletUIMain:
  def main(args: Array[String]): Unit = render

  protected[ui] def design: Design = Design.newDesign.bindSingleton[WvletEditor]

  def render: Unit =
    val frame = MainFrame()

    // Let Airframe DI design build UI components for WvletEditor
    val editor = design.newSession.build[WvletEditor]
    frame(editor).renderTo("main")

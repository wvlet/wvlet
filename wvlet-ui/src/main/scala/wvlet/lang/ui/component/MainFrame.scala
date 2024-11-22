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
package wvlet.lang.ui.component

import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs
import wvlet.airframe.rx.html.{RxComponent, RxElement}

object MainFrame extends RxComponent:

  val navBarHeightPx = 44

  object NavBar extends RxElement:
    // Based on https://tailwindui.com/components/application-ui/navigation/navbars

    private def navItem(name: RxElement, isSelected: Boolean = false): RxElement = a(
      href -> "#",
      if isSelected then
        cls -> "rounded-md bg-gray-900 px-3 py-2 text-sm font-medium text-white"
      else
        cls ->
          "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white"
      ,
      name
    )

    override def render: RxElement = nav(
      cls -> "w-full bg-gray-800",
      div(
        cls -> " px-2 sm:px-6 lg:px-8",
        div(
          cls   -> "relative flex items-center justify-between",
          style -> s"height: ${navBarHeightPx}px;",
          div(
            cls -> "flex flex-1 items-center justify-center sm:items-stretch sm:justify-start",
            div(
              cls -> "flex flex-shrink-0 items-center",
              a(
                href -> "https://wvlet.org/",
                img(
                  cls   -> "h-8 w-auto",
                  src   -> "./img/apple-touch-icon.png",
                  alt   -> "Wvlet",
                  width -> 50
                )
              )
            ),
            div(
              cls -> "hidden sm:ml-6 sm:block",
              div(
                cls -> "flex space-x-4",
                navItem("Editor", isSelected = true),
                navItem(
                  a(
                    href   -> "https://wvlet.org/wvlet/docs/syntax",
                    target -> "_blank",
                    "Query Syntax"
                  )
                ),
                navItem(a(href -> "https://github.com/wvlet/wvlet", target -> "_blank", "GitHub"))
              )
            )
          )
        )
      )
    )

  end NavBar

  override def render(content: RxElement) = div(
    cls -> "h-screen max-h-screen",
    NavBar,
    content,
    URLParamManager()
  )

end MainFrame

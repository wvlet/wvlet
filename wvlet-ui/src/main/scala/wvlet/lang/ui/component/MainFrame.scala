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

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgTags.*
import wvlet.airframe.rx.html.svgAttrs.*

class MainFrame extends RxElement:
  override def render = div(
    cls -> "h-fill",
    nav(
      cls -> "bg-gray-800",
      div(
        cls -> "mx-auto max-w-7xl px-2 sm:px-6 lg:px-8",
        div(
          cls -> "relative flex h-16 items-center justify-between",
          div(cls -> "absolute inset-y-0 left-0 flex items-center sm:hidden", button()),
          div(
            cls -> "flex flex-1 items-center justify-center sm:items-stretch sm:justify-start",
            div(
              cls -> "flex flex-shrink-0 items-center",
              img(
                cls   -> "h-8 w-auto",
                src   -> "./img/apple-touch-icon.png",
                alt   -> "Your Company",
                width -> 50
              )
            ),
            div(
              cls -> "hidden sm:ml-6 sm:block",
              div(
                cls -> "flex space-x-4",
                a(
                  href -> "#",
                  cls  -> "rounded-md bg-gray-900 px-3 py-2 text-sm font-medium text-white",
                  // aria("current") -> "page",
                  "Dashboard"
                ),
                a(
                  href -> "#",
                  cls ->
                    "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white",
                  "Team"
                ),
                a(
                  href -> "#",
                  cls ->
                    "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white",
                  "Projects"
                ),
                a(
                  href -> "#",
                  cls ->
                    "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:bg-gray-700 hover:text-white",
                  "Calendar"
                )
              )
            )
          )
        )
      )
    )
  )

end MainFrame

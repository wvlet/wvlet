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
package wvlet.lang.ui.playground

import wvlet.lang.compiler.lsp.StdLibReference
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}

/**
  * The Docs tab of the output panel: every stdlib function by category. A function opens as a
  * notebook of its docs and examples.
  */
class DocsPanel(state: PlaygroundState) extends RxElement:

  override def render: RxElement = div(
    cls -> "pg-output__view pg-docs-view",
    div(
      cls -> "pg-docs",
      h2(cls -> "pg-docs__title", "Standard library"),
      p(
        cls -> "pg-docs__lead",
        "Call a function on a value with dot syntax (",
        code("n_name.upper"),
        ", ",
        code("price.round(2)"),
        "). Pick a function to open its docs and examples as a notebook, with results computed in your browser. The ",
        a(
          href   -> "https://wvlet.org/wvlet/docs/syntax/stdlib-reference",
          target -> "_blank",
          "docs"
        ),
        " list the same functions."
      ),
      state
        .selectedFunction
        .map { selected =>
          StdLibReference
            .sections
            .map { sec =>
              div(
                cls -> "pg-docs__section",
                h3(cls -> "pg-docs__section-title", sec.category.title),
                p(cls  -> "pg-docs__section-intro", sec.category.intro),
                div(
                  cls -> "pg-docs__chips",
                  sec
                    .entries
                    .map { e =>
                      button(
                        cls             -> "pg-docs__chip",
                        data("current") -> selected.contains(e.key).toString,
                        titleAttr       -> e.description,
                        onclick         -> { () =>
                          state.openFunction(e)
                        },
                        e.label
                      )
                    }
                )
              )
            }
        }
    )
  )

end DocsPanel

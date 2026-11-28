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

import wvlet.lang.compiler.lsp.StdLibEntry
import wvlet.lang.compiler.lsp.StdLibReference
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}

/**
  * The Docs tab of the output panel: the selected stdlib function (its signature, description,
  * examples with their results, and the bundled examples that call it), or the index of all
  * functions by category when none is selected
  */
class DocsPanel(state: PlaygroundState) extends RxElement:

  private def select(e: StdLibEntry): Unit = state.selectedFunction := Some(e.key)

  private def tryIt(example: String): Unit =
    val q = Reference.queryOf(example)
    state.example := None
    state.mode    := InputMode.Wvlet
    state.text    := q
    state.run(RunRequest(q, InputMode.Wvlet))

  /** Every function by category, like the docs page's index */
  private def index: RxElement = div(
    cls -> "pg-docs",
    h2(cls -> "pg-docs__title", "Standard library"),
    p(
      cls -> "pg-docs__lead",
      "Call a function on a value with dot syntax (",
      code("n_name.upper"),
      ", ",
      code("price.round(2)"),
      "). Pick a function to see its signature and examples with results computed in your browser. The ",
      a(href -> "https://wvlet.org/wvlet/docs/syntax/stdlib-reference", target -> "_blank", "docs"),
      " list the same functions."
    ),
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
                  cls       -> "pg-docs__chip",
                  titleAttr -> e.description,
                  onclick   -> { () =>
                    select(e)
                  },
                  e.label
                )
              }
          )
        )
      }
  )

  private def detail(e: StdLibEntry): RxElement =
    val used = Reference.examplesUsing(e)
    div(
      cls -> "pg-docs",
      div(
        cls -> "pg-docs__crumbs",
        button(
          cls     -> "pg-docs__back",
          onclick -> { () =>
            state.selectedFunction := None
          },
          "← All functions"
        ),
        span(cls -> "pg-docs__category", e.category.title)
      ),
      h2(cls  -> "pg-docs__title pg-docs__title--code", e.label),
      pre(cls -> "pg-docs__sig", e.signature),
      e.typeParam.map(t => p(cls -> "pg-docs__types", s"T is one of: ${t}")),
      if e.description.nonEmpty then
        p(cls -> "pg-docs__desc", e.description)
      else
        p(cls -> "pg-docs__desc pg-docs__muted", "No description yet.")
      ,
      Option.when(e.engines.nonEmpty)(
        p(cls -> "pg-docs__muted", s"Specialized for these engines only: ${e.engines}")
      ),
      if e.examples.isEmpty then
        p(cls -> "pg-docs__muted", "No example yet.")
      else
        div(
          h3(cls -> "pg-docs__section-title", "Examples"),
          table(
            cls -> "pg-docs__examples",
            thead(tr(th("Example"), th("Result"), th())),
            tbody(
              e.examples
                .map { ex =>
                  tr(
                    td(code(ex)),
                    state.engine.runExample(ex) match
                      case Right(r) =>
                        td(code(cls -> "pg-docs__result", r))
                      case Left(err) =>
                        td(cls -> "pg-docs__error", err)
                    ,
                    td(
                      button(
                        cls       -> "pg-docs__try",
                        titleAttr -> "Open this example in the editor and run it",
                        onclick   -> { () =>
                          tryIt(ex)
                        },
                        "Try"
                      )
                    )
                  )
                }
            )
          )
        )
      ,
      Option.when(used.nonEmpty)(
        div(
          h3(cls -> "pg-docs__section-title", "Used in"),
          ul(
            cls -> "pg-docs__used",
            used.map { ex =>
              li(
                button(
                  cls       -> "pg-docs__link",
                  titleAttr -> ex.description,
                  onclick   -> { () =>
                    state.open(ex)
                  },
                  s"${Examples.categoryLabel(ex.category)} › ${ex.title}"
                )
              )
            }
          )
        )
      ),
      // Other functions of the category, to browse on
      div(
        h3(cls -> "pg-docs__section-title", s"More in ${e.category.title}"),
        div(
          cls -> "pg-docs__chips",
          StdLibReference
            .sections
            .find(_.category == e.category)
            .toSeq
            .flatMap(_.entries)
            .filterNot(_ == e)
            .map { o =>
              button(
                cls       -> "pg-docs__chip",
                titleAttr -> o.description,
                onclick   -> { () =>
                  select(o)
                },
                o.label
              )
            }
        )
      )
    )

  end detail

  override def render: RxElement = div(
    cls -> "pg-output__view pg-docs-view",
    state
      .selectedFunction
      .map(_.flatMap(StdLibReference.find))
      .map {
        case Some(e) =>
          detail(e)
        case None =>
          index
      }
  )

end DocsPanel

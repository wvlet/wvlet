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

import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonSize
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.Select
import wvlet.uni.ui.Tab
import wvlet.uni.ui.TabList

import scala.scalajs.js

/**
  * The panel beside the editor: the translation of the query (Wvlet to SQL in a chosen dialect, or
  * SQL to Wvlet), its logical plan, and the engine's physical plan.
  */
class OutputPanel(state: PlaygroundState) extends RxElement:

  // The translation editor: SQL for a Wvlet query, Wvlet for a SQL query
  private val translationText = Rx.variable("")
  private val translation     = CodeEditor(
    "pg-translation",
    translationText,
    language = state
      .mode
      .map {
        case InputMode.Wvlet =>
          "sql"
        case InputMode.SQL =>
          "wvlet"
      },
    theme = state.theme,
    readOnly = true
  )

  private def translated(out: RunOutput): String =
    state.mode.get match
      case InputMode.Wvlet =>
        out.sql
      case InputMode.SQL =>
        out.wvlet

  private def copyButton(text: => String) = Button("Copy")
    .withVariant(ButtonVariant.Ghost)
    .withSize(ButtonSize.Small)
    .withLabel("Copy to clipboard")
    .onClick(() => js.Dynamic.global.navigator.clipboard.writeText(text))

  override def onMount(node: Any): Unit =
    // Keep the last successful translation while the query has errors
    state
      .output
      .run { out =>
        if !out.hasError || translated(out).nonEmpty then
          translationText := translated(out)
      }

  override def render: RxElement =
    val tabs = TabList(
      state.outputTab,
      label = "Output",
      tabs = OutputTab
        .values
        .toSeq
        .map {
          case OutputTab.Translation =>
            Tab(
              OutputTab.Translation,
              state
                .mode
                .map {
                  case InputMode.Wvlet =>
                    "SQL"
                  case InputMode.SQL =>
                    "Wvlet"
                }
            )
          case t =>
            Tab(t, t.label)
        }
    )
    div(
      cls -> "pg-output",
      div(
        cls -> "pg-panel-header",
        tabs,
        span(cls -> "pg-grow"),
        state
          .outputTab
          .map {
            case OutputTab.Translation =>
              span(
                cls -> "pg-toolbar",
                state
                  .mode
                  .map {
                    case InputMode.Wvlet =>
                      Select(
                        state.dialect,
                        options = SqlDialect.values.toSeq,
                        optionLabel = _.label
                      ).withLabel("SQL dialect")
                    case InputMode.SQL =>
                      span()
                  },
                copyButton(translationText.get)
              )
            case OutputTab.LogicalPlan =>
              copyButton(state.output.get.logicalPlan)
            case OutputTab.PhysicalPlan =>
              copyButton(state.output.get.physicalPlan)
          }
      ),
      div(
        cls -> "pg-output__body",
        // The editor stays mounted (hidden) so Monaco keeps its state across tabs
        div(
          cls       -> "pg-output__view",
          styleAttr ->
            state
              .outputTab
              .map(t =>
                if t == OutputTab.Translation then
                  ""
                else
                  "display: none"
              ),
          translation
        ),
        state
          .outputTab
          .map {
            case OutputTab.Translation =>
              span()
            case OutputTab.LogicalPlan =>
              planView(state.output.map(_.logicalPlan), "The logical plan the compiler builds")
            case OutputTab.PhysicalPlan =>
              planView(
                state.output.map(_.physicalPlan),
                "The physical plan of the in-browser columnar engine"
              )
          }
      )
    )

  end render

  private def planView(plan: Rx[String], caption: String): RxElement = div(
    cls -> "pg-output__view pg-plan",
    div(cls -> "pg-plan__caption", caption),
    plan.map(p =>
      if p.isEmpty then
        div(cls -> "pg-result__empty", "No plan: fix the query errors first.")
      else
        pre(cls -> "pg-plan__text", p)
    )
  )

end OutputPanel

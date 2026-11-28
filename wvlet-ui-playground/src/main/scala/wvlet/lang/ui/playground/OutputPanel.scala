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

import org.scalajs.dom
import wvlet.lang.engine.plan.Optimization

import scala.scalajs.js

/**
  * The panel beside the editor: the translation of the query (Wvlet to SQL in a chosen dialect, or
  * SQL to Wvlet), its logical plan, the engine's physical plan, and the lineage of the file's
  * models.
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
          case OutputTab.Debug =>
            Tab(
              OutputTab.Debug,
              OutputTab.Debug.label,
              state
                .output
                .map { out =>
                  val n = out.debugBlocks.size
                  if n == 0 then
                    span()
                  else
                    span(cls -> "pg-badge pg-badge--muted", n.toString)
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
              span(cls -> "pg-toolbar", columnsToggle, copyButton(state.output.get.logicalPlan))
            case OutputTab.PhysicalPlan =>
              span(cls -> "pg-toolbar", columnsToggle, copyButton(enginePlanText))
            case OutputTab.Lineage | OutputTab.Debug | OutputTab.Docs =>
              span()
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
              div(
                cls -> "pg-output__view pg-plan",
                div(
                  cls -> "pg-plan__caption",
                  "The compiler's logical plan, read top-down in data-flow order. Click a line number to select its source."
                ),
                PlanView(
                  state.output.map(_.logicalPlan),
                  "No plan: fix the query errors first.",
                  onSourceLine = n => state.revealLine(n)
                )
              )
            case OutputTab.PhysicalPlan =>
              enginePanel
            case OutputTab.Lineage =>
              LineageView(state)
            case OutputTab.Debug =>
              DebugPanel(state)
            case OutputTab.Docs =>
              DocsPanel(state)
          }
      )
    )

  end render

  private def columnsToggle: RxElement = label(
    cls       -> "pg-check",
    titleAttr -> "Show the output columns of each plan node",
    input(
      tpe      -> "checkbox",
      checked  -> state.showPlanColumns,
      onchange -> { (e: dom.Event) =>
        state.showPlanColumns := e.target.asInstanceOf[dom.html.Input].checked
      }
    ),
    "Columns"
  )

  private def enginePlanText: String =
    val out = state.output.get
    state.engineStage.get match
      case EngineStage.Optimized =>
        out.runtimePlan
      case EngineStage.Bound =>
        out.boundPlan
      case EngineStage.Operators =>
        out.physicalPlan

  // The engine's planning stages, the rules that fired, and switches for its optimizations
  private def enginePanel: RxElement =
    val stages = TabList(
      state.engineStage,
      label = "Engine plan stage",
      tabs = EngineStage.values.toSeq.map(s => Tab(s, s.label))
    )
    div(
      cls -> "pg-output__view pg-plan",
      div(cls -> "pg-engine-stages", stages),
      div(cls -> "pg-plan__caption", state.engineStage.map(_.description)),
      PlanView(
        Rx.join(state.output, state.engineStage)
          .map { (out, stage) =>
            stage match
              case EngineStage.Optimized =>
                out.runtimePlan
              case EngineStage.Bound =>
                out.boundPlan
              case EngineStage.Operators =>
                out.physicalPlan
          },
        "No engine plan: fix the query errors first.",
        showOutputs = state.showPlanColumns
      ),
      div(
        cls -> "pg-engine-section",
        div(cls -> "pg-engine-section__title", "Optimizer rules that changed the plan"),
        state
          .output
          .map { out =>
            if out.firedRules.isEmpty then
              div(cls -> "pg-plan__caption", "None: the bound plan was already optimal.")
            else
              div(
                cls -> "pg-chips",
                out
                  .firedRules
                  .map((name, n) =>
                    span(cls -> "pg-chip", name, span(cls -> "pg-chip__count", s"×${n}"))
                  )
              )
          }
      ),
      optimizationSwitches
    )

  end enginePanel

  private def optimizationSwitches: RxElement =
    val all = Optimization.values.toSeq.sortBy(_.name)
    details(
      cls -> "pg-engine-section",
      summary(
        cls -> "pg-engine-section__title",
        state
          .disabledOptimizations
          .map { d =>
            if d.isEmpty then
              "Optimizations (all on)"
            else
              s"Optimizations (${d.size} off)"
          }
      ),
      div(
        cls -> "pg-plan__caption",
        "Turn an optimization off to see how the plan and the run time change.",
        Button("Turn all on")
          .withVariant(ButtonVariant.Ghost)
          .withSize(ButtonSize.Small)
          .onClick(() => state.disabledOptimizations := Set.empty)
      ),
      div(
        cls -> "pg-switches",
        all.map { o =>
          label(
            cls -> "pg-check",
            input(
              tpe      -> "checkbox",
              checked  -> state.disabledOptimizations.map(d => !d.contains(o)),
              onchange -> { (e: dom.Event) =>
                val on = e.target.asInstanceOf[dom.html.Input].checked
                state
                  .disabledOptimizations
                  .update(d =>
                    if on then
                      d - o
                    else
                      d + o
                  )
              }
            ),
            span(cls -> "pg-mono", o.name)
          )
        }
      )
    )

  end optimizationSwitches

end OutputPanel

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
package wvlet.lang.ui.component.flow

import org.scalajs.dom
import wvlet.lang.api.v1.flow.FlowApi.FlowRunDetail
import wvlet.lang.api.v1.flow.FlowApi.FlowRunListRequest
import wvlet.lang.api.v1.flow.FlowApi.FlowRunRequest
import wvlet.lang.api.v1.flow.FlowApi.FlowRunSummary
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.ui.component.MainFrame
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

import scala.scalajs.js

/**
  * A read-only list of recorded flow runs with per-run stage details, backed by [[FlowApi]].
  * Mutations (cancel, resume) stay in the `wvlet flow session` CLI
  */
class FlowRunsPage(rpcClient: RPCAsyncClient) extends RxElement:

  private val runs        = Rx.variable(List.empty[FlowRunSummary])
  private val selectedRun = Rx.variable(Option.empty[FlowRunDetail])
  private val loadError   = Rx.variable(Option.empty[String])

  private def refresh(): Unit = rpcClient
    .FlowApi
    .listRuns(FlowRunListRequest())
    .map { lst =>
      loadError := None
      runs      := lst
      // Keep the selected run's details in sync with the refreshed list
      selectedRun.get.foreach(d => showRun(d.run.runId))
    }
    .recover { case e: Throwable =>
      loadError := Some(e.getMessage)
    }
    .run()

  private def showRun(runId: String): Unit = rpcClient
    .FlowApi
    .getRun(FlowRunRequest(runId))
    .map(detail => selectedRun := Some(detail))
    .recover { case e: Throwable =>
      loadError := Some(e.getMessage)
    }
    .run()

  private def stateBadge(state: String): RxElement =
    val color =
      state match
        case "running" =>
          "bg-sky-800 text-sky-100"
        case "success" =>
          "bg-emerald-800 text-emerald-100"
        case "failed" =>
          "bg-red-800 text-red-100"
        case "cancelled" =>
          "bg-amber-800 text-amber-100"
        case _ =>
          "bg-gray-600 text-gray-100"
    span(cls -> s"rounded-md px-2 py-0.5 text-xs font-medium ${color}", state)

  private def fmtTime(millis: Long): String = new js.Date(millis.toDouble).toISOString()

  private def fmtElapsed(r: FlowRunSummary): String = r
    .finishedAtMillis
    .map(f => s"${f - r.startedAtMillis}ms")
    .getOrElse("-")

  private def headerCell(name: String) = th(
    cls -> "px-3 py-2 text-left text-xs font-semibold text-gray-400",
    name
  )

  private def cell(content: RxElement) = td(
    cls -> "px-3 py-1.5 text-sm text-gray-200 whitespace-nowrap",
    content
  )

  private def runList = runs.map { lst =>
    if lst.isEmpty then
      div(cls -> "text-sm text-gray-400 px-3 py-4", "No flow runs are recorded yet")
    else
      table(
        cls -> "w-full",
        thead(
          tr(
            headerCell("run id"),
            headerCell("flow"),
            headerCell("state"),
            headerCell("run time"),
            headerCell("started"),
            headerCell("elapsed")
          )
        ),
        tbody(
          lst.map { r =>
            tr(
              cls     -> "cursor-pointer hover:bg-zinc-700",
              onclick -> { (_: dom.MouseEvent) =>
                showRun(r.runId)
              },
              cell(span(cls -> "font-mono text-xs", r.runId)),
              cell(span(r.flowCall)),
              cell(stateBadge(r.state)),
              cell(span(r.runTimeMillis.map(fmtTime).getOrElse("-"))),
              cell(span(fmtTime(r.startedAtMillis))),
              cell(span(fmtElapsed(r)))
            )
          }
        )
      )
  }

  private def runDetail = selectedRun.map {
    case None =>
      div(cls -> "text-sm text-gray-400 px-3 py-4", "Select a run to see its stages")
    case Some(detail) =>
      div(
        div(
          cls -> "px-3 py-2 text-sm text-gray-200 flex items-center gap-x-2",
          span(cls -> "font-mono text-xs text-gray-400", detail.run.runId),
          span(detail.run.flowCall),
          stateBadge(detail.run.state)
        ),
        table(
          cls -> "w-full",
          thead(
            tr(
              headerCell("stage"),
              headerCell("state"),
              headerCell("attempts"),
              headerCell("error")
            )
          ),
          tbody(
            detail
              .stages
              .map { s =>
                tr(
                  cell(span(s.name)),
                  cell(stateBadge(s.state)),
                  cell(span(s.attempts.toString)),
                  td(cls -> "px-3 py-1.5 text-sm text-red-300", span(s.error.getOrElse("")))
                )
              }
          )
        )
      )
  }

  override def render: RxElement = div(
    cls       -> "h-full bg-zinc-800 text-gray-100 overflow-y-auto",
    styleAttr -> s"height: calc(100vh - ${MainFrame.navBarHeightPx}px);",
    // Reload the runs whenever this page becomes visible
    MainFrame
      .currentPage
      .map { page =>
        if page == MainFrame.PAGE_FLOW_RUNS then
          refresh()
        span()
      },
    div(
      cls -> "px-3 py-2 flex items-center gap-x-3",
      h1(cls -> "text-sm font-light tracking-tight text-slate-300", "Flow Runs"),
      button(
        cls     -> "rounded-md bg-gray-700 px-2 py-1 text-xs text-gray-200 hover:bg-gray-600",
        onclick -> { (_: dom.MouseEvent) =>
          refresh()
        },
        "Refresh"
      ),
      loadError.map {
        case Some(msg) =>
          span(cls -> "text-xs text-red-400", msg)
        case None =>
          span()
      }
    ),
    div(cls -> "px-1", runList),
    div(cls -> "px-1 mt-4 border-t border-zinc-700", runDetail)
  )

end FlowRunsPage

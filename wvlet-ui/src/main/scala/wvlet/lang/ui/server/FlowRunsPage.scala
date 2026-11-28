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
package wvlet.lang.ui.server

import wvlet.lang.api.v1.flow.FlowApi.FlowRunDetail
import wvlet.lang.api.v1.flow.FlowApi.FlowRunListRequest
import wvlet.lang.api.v1.flow.FlowApi.FlowRunRequest
import wvlet.lang.api.v1.flow.FlowApi.FlowRunSummary
import wvlet.lang.api.v1.flow.FlowApi.StageRunInfo
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Button
import wvlet.uni.ui.ButtonVariant
import wvlet.uni.ui.Input

import java.time.Instant
import scala.scalajs.js

/**
  * Recorded flow runs of the server's working folder with per-run stage details, backed by
  * `FlowApi`. The list refreshes while a run is in flight, can be filtered by flow name, and offers
  * cancel/resume actions (with a confirmation step) that use the same run-store paths as the
  * `wvlet flow session` CLI
  */
class FlowRunsPage(conn: ServerConnection) extends Polling:
  import Widgets.*

  override protected def intervalMillis: Long = 3000L

  private val runs           = Rx.variable(Option.empty[List[FlowRunSummary]])
  private val selectedRun    = Rx.variable(Option.empty[FlowRunDetail])
  private val loadError      = Rx.variable(Option.empty[String])
  private val flowNameFilter = Rx.variable("")
  // A requested mutation ("cancel" or "resume", run id) awaiting confirmation
  private val pendingAction = Rx.variable(Option.empty[(String, String)])
  private val actionMessage = Rx.variable(Option.empty[String])

  // The first call loads the list; later ones only while a listed run is in flight
  override protected def refresh(): Unit =
    if runs.get.forall(_.exists(_.state == "running")) then
      load()

  private def load(): Unit = conn
    .rpc
    .FlowApi
    .listRuns(FlowRunListRequest(flowName = Some(flowNameFilter.get.trim).filter(_.nonEmpty)))
    .map { lst =>
      loadError := None
      runs      := Some(lst)
      // Terminal runs never change, so only a still-running selection is re-fetched
      selectedRun
        .get
        .foreach(d =>
          if d.run.state == "running" then
            showRun(d.run.runId)
        )
    }
    .recover { case e: Throwable =>
      loadError := Some(conn.errorMessage(e))
    }
    .run()

  private def showRun(runId: String): Unit = conn
    .rpc
    .FlowApi
    .getRun(FlowRunRequest(runId))
    .map(detail => selectedRun := Some(detail))
    .recover { case e: Throwable =>
      loadError := Some(conn.errorMessage(e))
    }
    .run()

  private def performAction(action: String, runId: String): Unit =
    val call =
      action match
        case "cancel" =>
          conn.rpc.FlowApi.cancelRun(FlowRunRequest(runId))
        case _ =>
          conn.rpc.FlowApi.resumeRun(FlowRunRequest(runId))
    pendingAction := None
    call
      .map { result =>
        actionMessage := Some(result.message)
        load()
        showRun(runId)
      }
      .recover { case e: Throwable =>
        actionMessage := Some(conn.errorMessage(e))
      }
      .run()

  private def stateBadge(state: String): RxElement =
    val tone =
      state match
        case "running" =>
          "running"
        case "success" =>
          "finished"
        case "failed" =>
          "failed"
        case "cancelled" =>
          "canceled"
        case "waiting" =>
          "queued"
        case _ =>
          "other"
    span(cls -> s"wv-status wv-status--${tone}", state)

  private def time(millis: Long): String = Fmt.dateTime(Instant.ofEpochMilli(millis))

  /** A polling `wait until` sensor shows how long it has waited, so it isn't taken for a query */
  private def stageStateCell(s: StageRunInfo): RxElement =
    s.waitingSinceMillis match
      case Some(since) =>
        val now      = js.Date.now().toLong
        val lastPoll = s
          .lastPollAtMillis
          .map(p => s", last poll ${Fmt.duration(now - p)} ago")
          .getOrElse("")
        span(
          stateBadge("waiting"),
          span(cls -> "wv-muted", s" for ${Fmt.duration(now - since)}${lastPoll}")
        )
      case None =>
        stateBadge(s.state)

  private def runList = runs.map {
    case None =>
      div(cls -> "wv-empty", "Loading…")
    case Some(lst) if lst.isEmpty =>
      div(cls -> "wv-empty", "No flow runs are recorded in the server's working folder.")
    case Some(lst) =>
      table(
        cls -> "wv-table wv-table--select",
        thead(
          tr(th("Run id"), th("Flow"), th("State"), th("Run time"), th("Started"), th("Elapsed"))
        ),
        tbody(
          lst.map { r =>
            tr(
              onclick -> { (_: org.scalajs.dom.MouseEvent) =>
                showRun(r.runId)
              },
              td(span(cls -> "wv-mono", r.runId)),
              td(r.flowCall),
              td(stateBadge(r.state)),
              td(r.runTimeMillis.map(time).getOrElse("-")),
              td(time(r.startedAtMillis)),
              td(
                cls -> "wv-num",
                r.finishedAtMillis.map(f => Fmt.duration(f - r.startedAtMillis)).getOrElse("-")
              )
            )
          }
        )
      )
  }

  private def actionButton(label: String, action: String, runId: String): RxElement = Button(label)
    .withVariant(ButtonVariant.Ghost)
    .onClick { () =>
      actionMessage := None
      pendingAction := Some((action, runId))
    }

  private def confirmStrip = pendingAction.map {
    case Some((action, runId)) =>
      span(
        cls -> "wv-confirm",
        span(s"${action.capitalize} run ${runId}?"),
        Button("Confirm")
          .withVariant(ButtonVariant.Danger)
          .onClick(() => performAction(action, runId)),
        Button("Dismiss").withVariant(ButtonVariant.Ghost).onClick(() => pendingAction := None)
      )
    case None =>
      span()
  }

  private def runDetail = selectedRun.map {
    case None =>
      div(cls -> "wv-empty", "Select a run to see its stages.")
    case Some(detail) =>
      div(
        div(
          cls -> "wv-section__head",
          span(cls -> "wv-mono wv-muted", detail.run.runId),
          span(detail.run.flowCall),
          stateBadge(detail.run.state),
          // Cancel applies to in-flight runs, resume to failed or cancelled ones
          detail.run.state match
            case "running" =>
              actionButton("Cancel", "cancel", detail.run.runId)
            case "failed" | "cancelled" =>
              actionButton("Resume", "resume", detail.run.runId)
            case _ =>
              span()
          ,
          confirmStrip,
          actionMessage.map(_.map(m => span(cls -> "wv-muted", m)).getOrElse(span()))
        ),
        table(
          cls -> "wv-table",
          thead(tr(th("Stage"), th("State"), th("Attempts"), th("Error"))),
          tbody(
            detail
              .stages
              .map { s =>
                tr(
                  td(s.name),
                  td(stageStateCell(s)),
                  td(cls -> "wv-num", s.attempts.toString),
                  td(cls -> "wv-error-text", s.error.getOrElse(""))
                )
              }
          )
        )
      )
  }

  override def render: RxElement = div(
    cls -> "wv-page",
    div(
      cls -> "wv-page__head",
      h1("Flow runs"),
      Button("Refresh").withVariant(ButtonVariant.Ghost).onClick(() => load())
    ),
    div(
      cls -> "wv-filters",
      div(
        cls -> "wv-filter-text",
        Input(flowNameFilter)
          .withPlaceholder("Filter by flow name")
          .withLabel("Filter by flow name")
          .onChange(_ => js.timers.setTimeout(0)(load()))
      )
    ),
    errorLine(loadError),
    div(cls -> "wv-section", runList),
    div(cls -> "wv-section", runDetail)
  )

end FlowRunsPage

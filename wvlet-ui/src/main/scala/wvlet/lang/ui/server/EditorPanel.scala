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

import org.scalajs.dom
import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.frontend.FrontendApi.QueryInfoRequest
import wvlet.lang.api.v1.query.QueryDetailRequest
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.ui.component.monaco.MonacoEditor
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar
import wvlet.uni.ui.Column
import wvlet.uni.ui.DataTable

import scala.scalajs.js
import scala.util.Try

/** A run from the editor: the submitted query, then its state until it finishes */
case class EditorRun(
    label: String,
    queryId: Option[String] = None,
    info: Option[QueryInfo] = None,
    error: Option[String] = None
):
  def isDone: Boolean = error.nonEmpty || info.exists(_.status.isFinished)

/** One result row with its position, the key of the result table */
case class EditorResultRow(index: Int, values: IndexedSeq[Option[String]])

/**
  * The editor's state, kept outside the panel so the console can fill it ("Open in editor") while
  * the panel is closed. Runs use a session id of this browser tab, so `use` statements in the
  * editor affect only its own later runs
  */
class EditorState(val state: ConsoleState):
  val open: RxVar[Boolean]                              = Rx.variable(false)
  val text: RxVar[String]                               = Rx.variable(EditorState.Welcome)
  val run: RxVar[Option[EditorRun]]                     = Rx.variable(None)
  val sessionId: String                                 = EditorState.tabSessionId()
  private var poll: Option[js.timers.SetIntervalHandle] = None

  /** The editor panel's height in CSS pixels, changed by dragging its top edge */
  val height: RxVar[Int] = Rx.variable(EditorState.DefaultHeight)

  // The text last loaded or run, so replacing it never drops unsaved edits silently
  private var saved: String = EditorState.Welcome

  /** Whether the editor holds edits that were neither loaded nor run */
  def hasEdits: Boolean = text.get.trim.nonEmpty && text.get.trim != saved.trim

  /** Load the text of query `id` and open the editor */
  def openQuery(id: String): Unit = state
    .conn
    .rpc
    .FrontendApi
    .getQueryDetail(QueryDetailRequest(id))
    .map { d =>
      if !hasEdits ||
        dom.window.confirm(s"Replace the editor text with ${id}? Your edits will be lost.")
      then
        saved = d.query
        text := d.query
        open := true
    }
    .recover { case e: Throwable =>
      state.notice := Some(s"Could not open ${id}: ${state.conn.errorMessage(e)}")
    }
    .run()

  /**
    * Submit `query`; the run shows up in the list (and is selected) and its result below the editor
    * once it finishes
    */
  def submit(
      label: String,
      query: String,
      selection: QuerySelection,
      position: LinePosition = LinePosition.NoPosition
  ): Unit =
    if query.trim.nonEmpty then
      stopPolling()
      saved = text.get
      run := Some(EditorRun(label))
      state
        .conn
        .rpc
        .FrontendApi
        .submitQuery(
          QueryRequest(
            query = query,
            querySelection = selection,
            linePosition = position,
            isDebugRun = false,
            sessionId = Some(sessionId)
          )
        )
        .map { r =>
          run := Some(EditorRun(label, queryId = Some(r.queryId)))
          state.select(Some(r.queryId))
          state.reloadList()
          startPolling(label, r.queryId)
        }
        .recover { case e: Throwable =>
          run := Some(EditorRun(label, error = Some(state.conn.errorMessage(e))))
        }
        .run()

  private def startPolling(label: String, id: String): Unit =
    def check(): Unit = state
      .conn
      .rpc
      .FrontendApi
      .getQueryInfo(QueryInfoRequest(id, "0"))
      .map { info =>
        if run.get.flatMap(_.queryId).contains(id) then
          run := Some(EditorRun(label, Some(id), Some(info)))
          if info.status.isFinished then
            stopPolling()
      }
      .recover { case e: Throwable =>
        stopPolling()
        run := Some(EditorRun(label, Some(id), error = Some(state.conn.errorMessage(e))))
      }
      .run()
    check()
    poll = Some(js.timers.setInterval(EditorState.PollMillis.toDouble)(check()))

  private def stopPolling(): Unit =
    poll.foreach(js.timers.clearInterval)
    poll = None

end EditorState

object EditorState:
  val PollMillis: Int = 500

  /** The first text of the editor; the toolbar explains the keys */
  val Welcome: String = "show tables\n"

  val DefaultHeight: Int = 300

  /** The height range of the editor panel; the list above keeps the rest */
  val MinHeight: Int = 160

  /** A session id for this browser tab, kept across reloads of the tab */
  def tabSessionId(): String =
    val key = "wvlet.server.editorSession"
    Try(Option(dom.window.sessionStorage.getItem(key))).toOption.flatten.filter(_.nonEmpty) match
      case Some(id) =>
        id
      case None =>
        val id = s"ui-${js.Math.random().toString.drop(2).take(6)}"
        Try(dom.window.sessionStorage.setItem(key, id))
        id

/**
  * The editor below the query list: a Monaco editor on the left and the last run's result on the
  * right. ⌘↵ runs the selection, or the whole text when nothing is selected; ⇧⌘↵ runs the statement
  * at the cursor
  */
class EditorPanel(editor: EditorState) extends RxElement:
  private val editorId = "wv-editor"

  private var monaco: Option[MonacoEditor]   = None
  private var subscriptions: Seq[Cancelable] = Nil

  private def runAll(): Unit = monaco.foreach { m =>
    val sel = m.getSelectedText()
    if sel.trim.nonEmpty then
      editor.submit("selection", sel, QuerySelection.All)
    else
      editor.submit("all", m.getText(), QuerySelection.All)
  }

  private def runAtCursor(): Unit = monaco.foreach { m =>
    editor.submit(
      "statement at cursor",
      m.getText(),
      QuerySelection.Single,
      LinePosition(m.getLinePosition().toInt, m.getColumnPosition().toInt)
    )
  }

  private def onAction(cmd: String): Unit =
    cmd match
      case "run-query" =>
        runAll()
      case "run-production-query" | "run-subquery" =>
        runAtCursor()
      case _ =>

  override def onMount(node: Any): Unit =
    val m = new MonacoEditor(editorId, "wvlet", editor.text.get, action = cmd => onAction(cmd))
    monaco = Some(m)
    m.render()
    m.setDarkMode(dom.document.documentElement.getAttribute("data-theme") == "dark")
    subscriptions = Seq(
      editor
        .text
        .run { t =>
          if m.getText() != t then
            m.setText(t)
        },
      // The facade has no change callback; copy the text back while mounted
      Rx.intervalMillis(200)
        .map { _ =>
          val t = m.getText()
          if t != editor.text.get then
            editor.text := t
        }
        .subscribe()
    )
    m.focus()

  override def beforeUnmount: Unit =
    subscriptions.foreach(_.cancel)
    subscriptions = Nil
    monaco = None

  private given Ordering[Option[BigDecimal]] = Ordering.Option(using Ordering.BigDecimal)

  private def resultTable(r: QueryResult): RxElement =
    if r.schema.isEmpty then
      div(cls -> "wv-empty wv-empty--small", "The statement returned no relation.")
    else
      val rows = r
        .rows
        .zipWithIndex
        .map((vs, i) => EditorResultRow(i, vs.map(v => Option(v).map(_.toString)).toIndexedSeq))
      val cols =
        Column[EditorResultRow](
          "row",
          "#",
          cell = r => span(cls -> "wv-muted", (r.index + 1).toString)
        ).sortBy(_.index).alignEnd.withWidth("48px") +:
          r.schema
            .zipWithIndex
            .map { (c, i) =>
              val col = Column[EditorResultRow](
                s"c${i}",
                c.name,
                cell =
                  row =>
                    row.values.lift(i).flatten match
                      case None =>
                        span(cls -> "wv-null", "null")
                      case Some(v) =>
                        span(titleAttr -> v, v)
              )
              val sorted =
                if EditorPanel.isNumeric(c.typeName) then
                  col
                    .sortBy(row =>
                      row.values.lift(i).flatten.flatMap(s => Try(BigDecimal(s)).toOption)
                    )
                    .alignEnd
                else
                  col.sortBy(row => row.values.lift(i).flatten.getOrElse(""))
              sorted.withWidth(EditorPanel.columnWidth(c.name, rows.map(_.values.lift(i).flatten)))
            }
      DataTable[EditorResultRow, Int](Rx.const(rows), key = _.index, columns = cols)
        .withLabel("Query result")
        .withRowLabel(r => r.values.map(_.getOrElse("null")).mkString(", "))

  private def result: RxElement = editor
    .run
    .map {
      case None =>
        div(cls -> "wv-empty wv-empty--small", "Run a query to see its result here.")
      case Some(run) =>
        val status = run
          .info
          .map(_.status.toString.toLowerCase)
          .getOrElse(
            if run.error.nonEmpty then
              "failed"
            else
              "queued"
          )
        div(
          cls -> "wv-result",
          div(
            cls -> "wv-result__head",
            span(cls -> s"wv-dot wv-dot--${status}"),
            span(s"Ran ${run.label}"),
            run
              .queryId
              .map { id =>
                a(
                  cls     -> "wv-mono",
                  href    -> Page.Console(Some(id)).hash,
                  onclick -> { (e: dom.MouseEvent) =>
                    e.preventDefault()
                    editor.state.select(Some(id))
                  },
                  id
                )
              }
              .getOrElse(span()),
            run
              .info
              .flatMap(_.result)
              .map(r => span(cls -> "wv-muted", s"${Fmt.count(r.totalRows)} rows"))
              .getOrElse(span())
          ),
          run.error match
            case Some(e) =>
              div(cls -> "wv-error", role -> "alert", pre(cls -> "wv-pre", e))
            case None =>
              run.info match
                case Some(info) if info.errors.nonEmpty =>
                  div(
                    info
                      .errors
                      .map { e =>
                        div(
                          cls  -> "wv-error-box",
                          role -> "alert",
                          div(cls -> "wv-error-box__code", e.statusCode.toString),
                          pre(cls -> "wv-pre", e.message)
                        )
                      }
                  )
                case Some(info) if info.status.isFinished =>
                  info.result match
                    case Some(r) =>
                      div(cls -> "wv-result__table", resultTable(r))
                    case None =>
                      info
                        .preview
                        .map(p => pre(cls -> "wv-pre", p))
                        .getOrElse(div(cls -> "wv-empty wv-empty--small", "Done."))
                case _ =>
                  div(cls -> "wv-empty wv-empty--small", "Running…")
        )
    }

  /** The top edge of the panel: drag it, or focus it and press Up / Down, to resize */
  private def grip: RxElement =
    def clamp(h: Int): Int =
      val max = (dom.window.innerHeight * 0.75).toInt
      h.max(EditorState.MinHeight).min(max.max(EditorState.MinHeight))
    div(
      cls                 -> "wv-editor-panel__grip",
      role                -> "separator",
      aria("orientation") -> "horizontal",
      ariaLabel           -> "Resize the editor",
      tabindex            -> "0",
      ondblclick          -> { (_: dom.MouseEvent) =>
        editor.height := EditorState.DefaultHeight
      },
      onkeydown -> { (e: dom.KeyboardEvent) =>
        e.key match
          case "ArrowUp" =>
            e.preventDefault()
            editor.height := clamp(editor.height.get + 32)
          case "ArrowDown" =>
            e.preventDefault()
            editor.height := clamp(editor.height.get - 32)
          case _ =>
      },
      onpointerdown -> { (e: dom.PointerEvent) =>
        e.preventDefault()
        val el     = e.currentTarget.asInstanceOf[dom.html.Element]
        val startY = e.clientY
        val startH = editor.height.get
        el.setPointerCapture(e.pointerId)
        lazy val move: js.Function1[dom.PointerEvent, Unit] =
          (m: dom.PointerEvent) => editor.height := clamp(startH + (startY - m.clientY).toInt)
        lazy val up: js.Function1[dom.PointerEvent, Unit] =
          (_: dom.PointerEvent) =>
            el.removeEventListener("pointermove", move)
            el.removeEventListener("pointerup", up)
            el.removeEventListener("pointercancel", up)
        el.addEventListener("pointermove", move)
        el.addEventListener("pointerup", up)
        el.addEventListener("pointercancel", up)
      }
    )

  end grip

  override def render: RxElement = div(
    cls       -> "wv-editor-panel",
    role      -> "region",
    ariaLabel -> "Query editor",
    styleAttr -> editor.height.map(h => s"flex-basis: ${h}px"),
    grip,
    div(
      cls -> "wv-editor-panel__bar",
      span(cls -> "wv-editor-panel__title", "Editor"),
      button(
        tpe       -> "button",
        cls       -> "wv-button wv-button--primary",
        titleAttr -> "Run the selection, or everything ( ⌘↵ )",
        onclick   -> { (_: dom.MouseEvent) =>
          runAll()
        },
        "Run ⌘↵"
      ),
      button(
        tpe       -> "button",
        cls       -> "wv-button",
        titleAttr -> "Run the statement at the cursor ( ⇧⌘↵ )",
        onclick   -> { (_: dom.MouseEvent) =>
          runAtCursor()
        },
        "Run at cursor ⇧⌘↵"
      ),
      span(
        cls       -> "wv-muted wv-mono",
        titleAttr -> "This tab's session",
        s"session ${editor.sessionId}"
      ),
      div(cls -> "wv-grow"),
      button(
        tpe       -> "button",
        cls       -> "wv-icon-button",
        titleAttr -> "Hide the editor ( e )",
        ariaLabel -> "Hide the editor",
        onclick   -> { (_: dom.MouseEvent) =>
          editor.open := false
        },
        "▾"
      )
    ),
    div(
      cls -> "wv-editor-panel__body",
      div(cls -> "wv-editor", id -> editorId),
      div(cls -> "wv-editor-panel__result", result)
    )
  )

end EditorPanel

object EditorPanel:
  private val numericTypes = Set(
    "int",
    "integer",
    "bigint",
    "smallint",
    "tinyint",
    "long",
    "double",
    "float",
    "real",
    "decimal",
    "hugeint"
  )

  /**
    * A fixed width per result column from its longest header or value (first 200 rows), so columns
    * never overlap; long values end with an ellipsis and show in full on hover
    */
  def columnWidth(name: String, values: Seq[Option[String]]): String =
    val chars =
      values
        .iterator
        .take(200)
        .map(_.map(_.length).getOrElse(4))
        .foldLeft(name.length + 2)(math.max)
    s"${math.min(320, math.max(56, chars * 8 + 24))}px"

  /** Whether a column type sorts as a number */
  def isNumeric(typeName: String): Boolean =
    val t = typeName.toLowerCase.takeWhile(c => c.isLetter)
    numericTypes.contains(t)

end EditorPanel

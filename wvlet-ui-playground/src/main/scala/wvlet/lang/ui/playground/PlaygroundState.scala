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

import org.scalajs.dom
import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.engine.plan.Optimization
import wvlet.uni.dom.SizeRange
import wvlet.uni.dom.Storage
import wvlet.uni.dom.StorageCodec
import wvlet.uni.log.LogSupport
import wvlet.uni.rx.Rx
import wvlet.uni.rx.RxVar
import wvlet.uni.ui.ColorScheme

import scala.scalajs.js
import scala.util.Try

/** The tabs of the output panel beside the editor */
enum OutputTab(val label: String) derives StorageCodec:
  case Translation  extends OutputTab("SQL")
  case LogicalPlan  extends OutputTab("Plan")
  case PhysicalPlan extends OutputTab("Engine")
  case Lineage      extends OutputTab("Lineage")

/** The stage of the engine's plan shown in the Engine tab */
enum EngineStage(val label: String, val description: String) derives StorageCodec:
  case Optimized
      extends EngineStage(
        "Optimized",
        "The engine's runtime plan after its logical passes (filter pushdown, join ordering, column pruning, ...), read top-down in data-flow order."
      )

  case Bound
      extends EngineStage(
        "Bound",
        "The runtime plan as bound from the query, before any optimization. Compare it with Optimized."
      )

  case Operators
      extends EngineStage(
        "Operators",
        "The physical operators that run: hash joins and aggregations, sorts, and scans with the filters pushed into them."
      )

/** The tabs of the sidebar */
enum SidebarTab(val label: String) derives StorageCodec:
  case Examples extends SidebarTab("Examples")
  case Tables   extends SidebarTab("Tables")

/** What the user asked to run: the editor text, where the cursor was, and what to select */
case class RunRequest(
    text: String,
    mode: InputMode,
    position: LinePosition = LinePosition.NoPosition,
    selection: QuerySelection = QuerySelection.Single
)

/**
  * The state of the playground: layout (saved in localStorage), the current example and editor
  * text, and the last run's output. Running is synchronous, on the in-browser engine.
  */
class PlaygroundState extends LogSupport:
  import PlaygroundState.*

  // Layout, kept across visits
  val theme: RxVar[ColorScheme]     = Storage.local(s"${prefix}theme", ColorScheme.Dark)
  val sidebarOpen: RxVar[Boolean]   = Storage.local(s"${prefix}sidebarOpen", true)
  val sidebarWidth: RxVar[Int]      = Storage.local(s"${prefix}sidebarWidth", sidebarRange.default)
  val outputOpen: RxVar[Boolean]    = Storage.local(s"${prefix}outputOpen", true)
  val outputWidth: RxVar[Int]       = Storage.local(s"${prefix}outputWidth", outputRange.default)
  val resultHeight: RxVar[Int]      = Storage.local(s"${prefix}resultHeight", 320)
  val outputTab: RxVar[OutputTab]   = Storage.local(s"${prefix}outputTab", OutputTab.Translation)
  val sidebarTab: RxVar[SidebarTab] = Rx.variable(SidebarTab.Examples)
  val dialect: RxVar[SqlDialect]    = Rx.variable(SqlDialect.DuckDB)
  val engineStage: RxVar[EngineStage] = Storage.local(
    s"${prefix}engineStage",
    EngineStage.Optimized
  )

  val showPlanColumns: RxVar[Boolean] = Storage.local(s"${prefix}showPlanColumns", false)
  // Engine optimizations turned off in the Engine tab
  val disabledOptimizations: RxVar[Set[Optimization]] = Rx.variable(Set.empty)

  /** Select a line of the query in the editor; set by the editor when it mounts */
  var revealLine: Int => Unit    = _ => ()
  val scaleFactor: RxVar[Double] = Rx.variable(PlaygroundEngine.defaultScaleFactor)

  // The query
  val mode: RxVar[InputMode]                 = Rx.variable(InputMode.Wvlet)
  val example: RxVar[Option[Example]]        = Rx.variable(None)
  val text: RxVar[String]                    = Rx.variable("")
  val lastRequest: RxVar[RunRequest]         = Rx.variable(RunRequest("", InputMode.Wvlet))
  val output: RxVar[RunOutput]               = Rx.variable(RunOutput())
  val status: RxVar[String]                  = Rx.variable("Generating TPC-H tables...")
  val filter: RxVar[String]                  = Rx.variable("")
  val selectedExample: RxVar[Option[String]] = Rx.variable(None)

  private var engineOpt: Option[PlaygroundEngine] = None

  /** The engine over tables of the current scale factor, generated on first use */
  def engine: PlaygroundEngine =
    engineOpt match
      case Some(e) if e.scaleFactor == scaleFactor.get =>
        e
      case _ =>
        val start = System.nanoTime()
        val e     = PlaygroundEngine(scaleFactor.get)
        engineOpt = Some(e)
        val millis = (System.nanoTime() - start) / 1e6
        info(f"Generated TPC-H SF${scaleFactor.get} in ${millis}%.0f ms")
        e

  /** Close the sidebar drawer after a pick on a narrow screen, so the editor shows */
  def closeSidebarOnNarrow(): Unit =
    if PlaygroundState.isNarrow then
      sidebarOpen := false

  /** Show an example in the editor and run it */
  def open(e: Example): Unit =
    example         := Some(e)
    selectedExample := Some(e.path)
    mode            := e.mode
    text            := e.text
    setHash(s"example=${e.path}")
    run(RunRequest(e.text, e.mode))

  /** Run a request and publish its output */
  def run(req: RunRequest): Unit =
    lastRequest := req
    val out = engine.run(
      req.text,
      req.mode,
      dialect.get,
      req.position,
      req.selection,
      PlaygroundEngine.defaultRowLimit,
      disabledOptimizations.get
    )
    output := out
    status := (
      if out.hasError then
        "Error"
      else
        out.result.map(r => f"${r.totalRows}%,d rows").getOrElse("Done")
    )

  /** Run the last request again, e.g., after the dialect or scale factor changed */
  def rerun(): Unit = run(lastRequest.get.copy(text = text.get, mode = mode.get))

  /** Switch the editor language; the text is converted between Wvlet and SQL when it can be */
  def switchMode(m: InputMode): Unit =
    if m != mode.get then
      val current   = text.get
      val converted =
        m match
          case InputMode.Wvlet =>
            Try(engine.toWvlet(current)).toOption
          case InputMode.SQL =>
            val out = engine.run(current, InputMode.Wvlet, SqlDialect.DuckDB)
            Option.when(!out.hasError && out.sql.nonEmpty)(out.sql)
      mode := m
      converted.foreach(t => text := t)
      example := None
      run(RunRequest(text.get, m))

  /** A link to the current query, to share */
  def shareUrl: String =
    val base = dom.window.location.href.takeWhile(_ != '#')
    example.get match
      case Some(e) if e.text == text.get =>
        s"${base}#example=${js.URIUtils.encodeURIComponent(e.path)}"
      case _ =>
        val m = mode.get.label.toLowerCase
        s"${base}#${m}=${js.URIUtils.encodeURIComponent(text.get)}"

  /** Open what the URL hash names: an example, or a query text */
  def openFromHash(): Unit =
    val hash                                = dom.window.location.hash.stripPrefix("#")
    def param(name: String): Option[String] = hash
      .split('&')
      .collectFirst {
        case kv if kv.startsWith(s"${name}=") =>
          js.URIUtils.decodeURIComponent(kv.drop(name.length + 1))
      }
    param("example").flatMap(Examples.find) match
      case Some(e) =>
        open(e)
      case None =>
        (param("wvlet").map(_ -> InputMode.Wvlet) orElse param("sql").map(_ -> InputMode.SQL)) match
          case Some((t, m)) =>
            example := None
            mode    := m
            text    := t
            run(RunRequest(t, m))
          case None =>
            open(Examples.default)

  private def setHash(h: String): Unit = Try(dom.window.history.replaceState(null, "", s"#${h}"))

end PlaygroundState

object PlaygroundState:
  private val prefix = "wvlet-playground."

  /** The widest screen that gets the stacked phone layout; matches `index.css` */
  val narrowMaxWidth = 760

  /** True on a phone-sized screen, where the panels stack and the sidebar is a drawer */
  def isNarrow: Boolean = dom.window.matchMedia(s"(max-width: ${narrowMaxWidth}px)").matches

  val sidebarRange = SizeRange(min = 200, default = 280, max = 520)
  val outputRange  = SizeRange(min = 280, default = 520, max = 1200)
  val resultRange  = SizeRange(min = 120, default = 320, max = 1200)

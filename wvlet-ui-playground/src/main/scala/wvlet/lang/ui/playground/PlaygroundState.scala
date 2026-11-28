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
import wvlet.lang.compiler.lsp.StdLibEntry
import wvlet.lang.compiler.lsp.StdLibReference
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
  case Debug        extends OutputTab("Debug")
  case Docs         extends OutputTab("Docs")

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
  case Examples  extends SidebarTab("Examples")
  case Tables    extends SidebarTab("Tables")
  case Reference extends SidebarTab("Reference")

/**
  * The state of the playground: layout (saved in localStorage), the notebook's cells, and the
  * output of the focused cell for the right panel. Running is synchronous, on the in-browser
  * engine.
  */
class PlaygroundState extends LogSupport:
  import PlaygroundState.*

  // Layout, kept across visits
  val theme: RxVar[ColorScheme]     = Storage.local(s"${prefix}theme", ColorScheme.Dark)
  val sidebarOpen: RxVar[Boolean]   = Storage.local(s"${prefix}sidebarOpen", true)
  val sidebarWidth: RxVar[Int]      = Storage.local(s"${prefix}sidebarWidth", sidebarRange.default)
  val outputOpen: RxVar[Boolean]    = Storage.local(s"${prefix}outputOpen", true)
  val outputWidth: RxVar[Int]       = Storage.local(s"${prefix}outputWidth", outputRange.default)
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

  val scaleFactor: RxVar[Double] = Rx.variable(PlaygroundEngine.defaultScaleFactor)

  // The notebook
  val mode: RxVar[InputMode]          = Rx.variable(InputMode.Wvlet)
  val example: RxVar[Option[Example]] = Rx.variable(None)
  val cells: RxVar[Seq[Cell]]         = Rx.variable(Nil)
  // The cell whose output the right panel shows
  val focused: RxVar[Option[Int]]            = Rx.variable(None)
  val output: RxVar[RunOutput]               = Rx.variable(RunOutput())
  val status: RxVar[String]                  = Rx.variable("Generating TPC-H tables...")
  val filter: RxVar[String]                  = Rx.variable("")
  val selectedExample: RxVar[Option[String]] = Rx.variable(None)
  // The Reference tab: its search text and the selected function's StdLibEntry.key
  val referenceFilter: RxVar[String]          = Rx.variable("")
  val selectedFunction: RxVar[Option[String]] = Rx.variable(None)

  private var nextCellId = 0

  private var engineOpt: Option[PlaygroundEngine] = None
  // Whether the last run that compiled had debug blocks
  private var hadDebugBlocks = false

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

  private def newCell(src: CellSource): Cell =
    nextCellId += 1
    Cell(nextCellId, src.kind, src.text)

  /** Replace the notebook with these cells and run them */
  def openNotebook(sources: Seq[CellSource]): Unit =
    val cs =
      if sources.exists(_.kind == CellKind.Code) then
        sources
      else
        sources :+ CellSource(CellKind.Code, "")
    cells := cs.map(newCell)
    runAll(opened = true)

  /** Show an example as a notebook and run it */
  def open(e: Example): Unit =
    example          := Some(e)
    selectedExample  := Some(e.path)
    selectedFunction := None
    mode             := e.mode
    setHash(s"example=${e.path}")
    openNotebook(NotebookText.ofExample(e))

  /** Show a stdlib function as a notebook: its docs, then a cell per example */
  def openFunction(e: StdLibEntry): Unit =
    example          := None
    selectedFunction := Some(e.key)
    mode             := InputMode.Wvlet
    setHash(s"function=${e.key}")
    openNotebook(NotebookText.ofFunction(e))

  /** Show a query text (with or without cell markers) as a notebook */
  def openText(t: String, m: InputMode): Unit =
    example := None
    mode    := m
    openNotebook(NotebookText.parse(t))

  /** The notebook as text: what a share link and a saved file hold */
  def notebookText: String = NotebookText.render(cells.get.map(_.source)).text

  /** Run every code cell in order, then show the first one's output */
  def runAll(opened: Boolean = false): Unit =
    val code = cells.get.filter(_.isCode)
    code.foreach(c => runCell(c, opened = opened))
    code
      .headOption
      .foreach { c =>
        focus(c)
      }

  /** Run the focused cell again, e.g., after the dialect changed */
  def rerun(): Unit = focusedCell.foreach(c => runCell(c))

  def focusedCell: Option[Cell] = focused.get.flatMap(id => cells.get.find(_.id == id))

  /** Make a cell the one the right panel follows */
  def focus(c: Cell): Unit =
    if !focused.get.contains(c.id) then
      focused := Some(c.id)
      c.output.get.foreach(o => output := o)

  /**
    * Run a code cell. It compiles with the cells above it, so their vals and models are in scope,
    * and runs the statement at `position` (a line of the cell), by default the cell's last one.
    *
    * @param opened
    *   true when the notebook was just opened (an example or a shared link), not edited
    */
  def runCell(
      c: Cell,
      position: Option[LinePosition] = None,
      selection: QuerySelection = QuerySelection.Single,
      opened: Boolean = false
  ): Unit =
    val cs = cells.get
    val i  = cs.indexWhere(_.id == c.id)
    if i >= 0 && c.isCode then
      // SQL cells run on their own; Wvlet cells see the definitions above them
      val (text, offset) =
        mode.get match
          case InputMode.SQL =>
            (c.text.get, 0)
          case InputMode.Wvlet =>
            val layout = NotebookText.render(cs.take(i + 1).map(_.source))
            (layout.text, layout.startLines(i) - 1)
      c.lineOffset = offset
      val local = position.filter(_.line > 0).getOrElse(LinePosition(c.lastLine, 1))
      val ran   = engine.run(
        text,
        mode.get,
        dialect.get,
        LinePosition(local.line + offset, local.column),
        selection,
        PlaygroundEngine.defaultRowLimit,
        disabledOptimizations.get
      )
      // While an edit doesn't compile, keep the last checks: the markers would otherwise blink
      // out on every half-typed line
      val out =
        if !opened && ran.hasError && ran.checks.isEmpty then
          ran.copy(checks = c.output.get.map(_.checks).getOrElse(Nil))
        else
          ran
      c.lastRunText = c.text.get
      c.output := Some(out)
      c.runCount.update(_ + 1)
      focused := Some(c.id)
      output  := out
      showDebugTab(c, out, opened)
      val failed =
        c.checksOf(out).count(_.isFailure) match
          case 0 =>
            ""
          case 1 =>
            " · 1 check failed"
          case n =>
            s" · ${n} checks failed"
      status := (
        if out.hasError then
          "Error"
        else
          out.result.map(r => f"${r.totalRows}%,d rows").getOrElse("Done") + failed
      )
    end if

  end runCell

  // The right panel shows the Debug tab when a newly opened cell has `debug` blocks, or when an
  // edit adds a cell's first one
  private def showDebugTab(c: Cell, out: RunOutput, opened: Boolean): Unit =
    if opened || !out.hasError then
      val hasDebug = c.checksOf(out).exists(_.isInstanceOf[Check.Debug])
      if hasDebug && (opened || !hadDebugBlocks) then
        outputOpen := true
        outputTab  := OutputTab.Debug
      else if opened && !hasDebug && outputTab.get == OutputTab.Debug then
        outputTab := OutputTab.Translation
      hadDebugBlocks = hasDebug

  /** Select a 1-origin line of the notebook text in its cell's editor */
  def revealLine(line: Int): Unit =
    val cs     = cells.get
    val starts = NotebookText.render(cs.map(_.source)).startLines
    cs.indices
      .reverse
      .find(i => starts(i) <= line)
      .foreach { i =>
        val c = cs(i)
        c.reveal(line - starts(i) + 1)
      }

  /** Add a cell after `after` (or at the end), and focus its editor */
  def insertCell(kind: CellKind, after: Option[Cell] = None): Cell =
    val c  = newCell(CellSource(kind, ""))
    val cs = cells.get
    val i  = after.map(a => cs.indexWhere(_.id == a.id) + 1).getOrElse(cs.size)
    if kind == CellKind.Markdown then
      c.editing := true
    cells       := cs.patch(i, Seq(c), 0)
    c

  def moveCell(c: Cell, delta: Int): Unit =
    val cs = cells.get
    val i  = cs.indexWhere(_.id == c.id)
    val j  = i + delta
    if i >= 0 && j >= 0 && j < cs.size then
      cells := cs.updated(i, cs(j)).updated(j, c)

  def deleteCell(c: Cell): Unit =
    cells.update(_.filterNot(_.id == c.id))
    if focused.get.contains(c.id) then
      focused := None

  /** Switch the language; each code cell is converted between Wvlet and SQL when it can be */
  def switchMode(m: InputMode): Unit =
    if m != mode.get then
      cells
        .get
        .filter(_.isCode)
        .foreach { c =>
          val current   = c.text.get
          val converted =
            m match
              case InputMode.Wvlet =>
                Try(engine.toWvlet(current)).toOption
              case InputMode.SQL =>
                val out = engine.run(current, InputMode.Wvlet, SqlDialect.DuckDB)
                Option.when(!out.hasError && out.sql.nonEmpty)(out.sql)
          converted.foreach(t => c.text := t)
        }
      mode    := m
      example := None
      runAll()

  /** A link to the current notebook, to share */
  def shareUrl: String =
    val base                        = dom.window.location.href.takeWhile(_ != '#')
    val text                        = notebookText
    def same(srcs: Seq[CellSource]) = NotebookText.render(srcs).text == text
    val fn                          = selectedFunction.get.flatMap(StdLibReference.find)
    example.get match
      case Some(e) if same(NotebookText.ofExample(e)) =>
        s"${base}#example=${js.URIUtils.encodeURIComponent(e.path)}"
      case _ if fn.exists(f => same(NotebookText.ofFunction(f))) =>
        s"${base}#function=${js.URIUtils.encodeURIComponent(fn.get.key)}"
      case _ =>
        val m = mode.get.label.toLowerCase
        s"${base}#${m}=${js.URIUtils.encodeURIComponent(text)}"

  /** Open what the URL hash names: an example, a function, or a notebook text */
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
        param("function").flatMap(StdLibReference.find) match
          case Some(f) =>
            openFunction(f)
          case None =>
            (
              param("wvlet").map(_ -> InputMode.Wvlet) orElse param("sql").map(_ -> InputMode.SQL)
            ) match
              case Some((t, m)) =>
                openText(t, m)
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

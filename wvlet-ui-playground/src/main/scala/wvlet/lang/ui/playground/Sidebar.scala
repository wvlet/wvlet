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
import wvlet.lang.compiler.lsp.StdLibReferenceSection
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx
import wvlet.uni.ui.Input
import wvlet.uni.ui.Tab
import wvlet.uni.ui.TabList
import wvlet.uni.ui.TabPanel
import wvlet.uni.ui.Tree

/** A row of the example tree: a category or an example in it */
enum ExampleNode:
  case Category(c: ExampleCategory, matches: Seq[Example])
  case Leaf(e: Example)

  def key: String =
    this match
      case Category(c, _) =>
        s"category:${c.id}"
      case Leaf(e) =>
        e.path

/** A row of the table browser: a table or one of its columns */
enum TableNode:
  case Table(name: String, rows: Long, columns: Seq[ResultColumn])
  case Column(table: String, column: ResultColumn)

  def key: String =
    this match
      case Table(n, _, _) =>
        n
      case Column(t, c) =>
        s"${t}.${c.name}"

/** A row of the function reference: a receiver-type section or a function in it */
enum ReferenceNode:
  case Section(s: StdLibReferenceSection)
  case Function(e: StdLibEntry)

  def key: String =
    this match
      case Section(s) =>
        s"category:${s.category.id}"
      case Function(e) =>
        e.key

/**
  * The left side panel: the example library by category (filtered by a search box), the TPC-H
  * tables with their columns, and the stdlib function reference.
  */
class Sidebar(state: PlaygroundState) extends RxElement:

  private val expanded = Rx.variable(Examples.categories.map(c => s"category:${c.id}").toSet)

  // Titles and descriptions match by substring; query text only by whole word, so `lag` finds
  // the lag example but not every `l_returnflag`
  private def matches(e: Example, q: String): Boolean =
    q.isEmpty || {
      val s = q.toLowerCase
      e.title.toLowerCase.contains(s) || e.description.toLowerCase.contains(s) ||
      e.text.toLowerCase.split("[^a-z0-9_]+").contains(s)
    }

  private val roots: Rx[Seq[ExampleNode]] = state
    .filter
    .map { q =>
      Examples
        .categories
        .flatMap { c =>
          val ms = c.examples.filter(matches(_, q.trim))
          Option.when(ms.nonEmpty)(ExampleNode.Category(c, ms))
        }
    }

  private def exampleTree = Tree[ExampleNode, String](
    roots,
    children = {
      case ExampleNode.Category(_, ms) =>
        ms.map(ExampleNode.Leaf(_))
      case _ =>
        Nil
    },
    key = _.key,
    render = {
      case ExampleNode.Category(c, ms) =>
        span(cls -> "pg-tree-category", c.label, span(cls -> "pg-tree-count", ms.size.toString))
      case ExampleNode.Leaf(e) =>
        span(cls -> "pg-tree-leaf", titleAttr -> e.description, e.title)
    }
  ).withLabel("Examples")
    .withExpanded(expanded)
    .withSelected(state.selectedExample)
    .withRowHeight(26)
    .withTypeahead {
      case ExampleNode.Category(c, _) =>
        c.label
      case ExampleNode.Leaf(e) =>
        e.title
    }
    .onSelect {
      case ExampleNode.Leaf(e) =>
        state.open(e)
        state.closeSidebarOnNarrow()
      case c: ExampleNode.Category =>
        expanded.update(s =>
          if s.contains(c.key) then
            s - c.key
          else
            s + c.key
        )
    }

  private val tableExpanded = Rx.variable(Set.empty[String])

  private def tableTree = Tree[TableNode, String](
    state
      .scaleFactor
      .map { _ =>
        val engine = state.engine
        engine.tableSchemas.map((name, cols) => TableNode.Table(name, engine.tableRows(name), cols))
      },
    children = {
      case TableNode.Table(n, _, cols) =>
        cols.map(TableNode.Column(n, _))
      case _ =>
        Nil
    },
    key = _.key,
    render = {
      case TableNode.Table(n, rows, _) =>
        span(cls -> "pg-tree-category", n, span(cls -> "pg-tree-count", f"${rows}%,d"))
      case TableNode.Column(_, c) =>
        span(cls -> "pg-tree-leaf pg-column", c.name, span(cls -> "pg-column-type", c.typeName))
    }
  ).withLabel("Tables")
    .withExpanded(tableExpanded)
    .withRowHeight(24)
    .onSelect {
      case TableNode.Table(n, _, _) =>
        val q = s"from ${n}\nlimit 100\n"
        state.example := None
        state.mode    := InputMode.Wvlet
        state.text    := q
        state.run(RunRequest(q, InputMode.Wvlet))
        state.closeSidebarOnNarrow()
      case _ =>
    }

  private val referenceExpanded = Rx.variable(Set.empty[String])

  private def referenceTree = Tree[ReferenceNode, String](
    state
      .referenceFilter
      .map { q =>
        val secs = StdLibReference.filter(q)
        // A search opens every section with a match; clearing it folds them again
        referenceExpanded := (
          if q.trim.isEmpty then
            Set.empty
          else
            secs.map(s => ReferenceNode.Section(s).key).toSet
        )
        secs.map(ReferenceNode.Section(_))
      },
    children = {
      case ReferenceNode.Section(s) =>
        s.entries.map(ReferenceNode.Function(_))
      case _ =>
        Nil
    },
    key = _.key,
    render = {
      case ReferenceNode.Section(s) =>
        span(
          cls -> "pg-tree-category",
          s.category.title,
          span(cls -> "pg-tree-count", s.entries.size.toString)
        )
      case ReferenceNode.Function(e) =>
        span(cls -> "pg-tree-leaf pg-function", titleAttr -> e.description, e.label)
    }
  ).withLabel("Functions")
    .withExpanded(referenceExpanded)
    .withSelected(state.selectedFunction)
    .withRowHeight(24)
    .withTypeahead {
      case ReferenceNode.Section(s) =>
        s.category.title
      case ReferenceNode.Function(e) =>
        e.name
    }
    .onSelect {
      case ReferenceNode.Function(e) =>
        // The details need room: show them in the output panel's Docs tab
        state.selectedFunction := Some(e.key)
        state.outputTab        := OutputTab.Docs
        state.closeSidebarOnNarrow()
      case s: ReferenceNode.Section =>
        referenceExpanded.update(x =>
          if x.contains(s.key) then
            x - s.key
          else
            x + s.key
        )
    }

  override def render: RxElement =
    val tabs = TabList(
      state.sidebarTab,
      label = "Sidebar",
      tabs = SidebarTab.values.toSeq.map(t => Tab(t, t.label))
    )
    div(
      cls -> "pg-sidebar",
      div(cls -> "pg-sidebar__tabs", tabs),
      TabPanel(
        tabs,
        render = {
          case SidebarTab.Examples =>
            div(
              cls -> "pg-sidebar__panel",
              div(
                cls -> "pg-sidebar__search",
                Input(state.filter)
                  .withType("search")
                  .withPlaceholder("Search syntax and functions")
                  .withLabel("Search examples")
              ),
              div(cls -> "pg-sidebar__tree", exampleTree)
            )
          case SidebarTab.Tables =>
            div(
              cls -> "pg-sidebar__panel",
              div(
                cls -> "pg-sidebar__note",
                state
                  .scaleFactor
                  .map(sf =>
                    s"TPC-H tables at scale factor ${sf}, generated in your browser. Click a table to read it."
                  )
              ),
              div(cls -> "pg-sidebar__tree", tableTree)
            )
          case SidebarTab.Reference =>
            div(
              cls -> "pg-sidebar__panel",
              div(
                cls -> "pg-sidebar__search",
                Input(state.referenceFilter)
                  .withType("search")
                  .withPlaceholder("Search functions")
                  .withLabel("Search functions")
              ),
              div(cls -> "pg-sidebar__tree", referenceTree)
            )
        }
      )
    )

  end render

end Sidebar

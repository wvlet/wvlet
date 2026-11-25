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

/**
  * The left side panel: the example library by category (filtered by a search box), and the TPC-H
  * tables with their columns.
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
      case _ =>
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
        }
      )
    )

  end render

end Sidebar

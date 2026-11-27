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
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx
import wvlet.lang.compiler.pipeline.ModelOrder

import scala.language.implicitConversions

/** Positions of the lineage graph's nodes: rows by dependency depth, top to bottom */
case class LineageLayout(lineage: ModelLineage):
  import LineageLayout.*

  /** The width of each node, fitted to its text */
  val widths: Map[String, Int] = lineage.nodes.map(n => n.id -> nodeWidth(n)).toMap

  private val rowWidths: Seq[Int] = lineage
    .layers
    .map(r => r.map(n => widths(n.id)).sum + (r.size - 1).max(0) * colGap)

  private val maxWidth: Int = rowWidths.maxOption.getOrElse(0)

  /** The top-left corner of each node; a short row is centered against the widest one */
  val positions: Map[String, (Int, Int)] =
    lineage
      .layers
      .zipWithIndex
      .flatMap { (row, r) =>
        val top  = pad + r * (nodeH + rowGap)
        var left = pad + (maxWidth - rowWidths(r)) / 2
        row.map { n =>
          val p = n.id -> (left, top)
          left += widths(n.id) + colGap
          p
        }
      }
      .toMap

  val width: Int  = pad * 2 + maxWidth
  val height: Int =
    pad * 2 + lineage.layers.size * nodeH + (lineage.layers.size - 1).max(0) * rowGap

  /** A curve from the bottom of `from` to the top of `to` */
  def edgePath(from: String, to: String): Option[String] =
    for
      (x1, y1) <- positions.get(from)
      (x2, y2) <- positions.get(to)
    yield
      val sx = x1 + widths(from) / 2
      val sy = y1 + nodeH
      val ex = x2 + widths(to) / 2
      val ey = y2 - arrow
      val dy = ((ey - sy) / 2).max(16)
      s"M ${sx} ${sy} C ${sx} ${sy + dy}, ${ex} ${ey - dy}, ${ex} ${ey}"

end LineageLayout

object LineageLayout:
  val minNodeW = 96
  val maxNodeW = 176
  val nodeH    = 42
  val colGap   = 14
  val rowGap   = 40
  val pad      = 8
  val arrow    = 6
  val textX    = 18

  /** Characters of a label that fit in a node */
  val maxLabel  = 21
  val maxDetail = 27

  /** The smallest scale the graph shrinks to fit the panel; a wider graph scrolls */
  val minScale = 0.8

  def truncate(s: String, n: Int = maxLabel): String =
    if s.length <= n then
      s
    else
      s"${s.take(n - 1)}…"

  // Approximate text widths: 12px monospace labels and 11px sans-serif details
  private def nodeWidth(n: LineageNode): Int =
    val label  = truncate(n.label).length * 7.3
    val detail = truncate(n.detail, maxDetail).length * 6.0
    (textX + math.max(label, detail).toInt + 12).max(minNodeW).min(maxNodeW)

/**
  * The Lineage tab: the file's tables and seeds, the models built from them, and the queries that
  * read the models, as a top-down graph. Hovering a node highlights what it reads and what reads
  * it; clicking it selects its definition in the editor.
  */
class LineageView(state: PlaygroundState) extends RxElement:
  import LineageLayout.*

  // Keep the last graph while the query has compile errors, as the SQL tab does
  private val lineage = Rx.variable(ModelLineage())
  private val hovered = Rx.variable(Option.empty[String])

  private var subscription: Option[Cancelable] = None

  override def onMount(node: Any): Unit =
    subscription = Some(
      state
        .output
        .run { out =>
          if !out.hasError || out.lineage.nonEmpty then
            lineage := out.lineage
        }
    )

  // The panel makes a new view each time the tab opens: release the old one's subscription
  override def beforeUnmount: Unit =
    subscription.foreach(_.cancel)
    subscription = None

  extension (l: ModelLineage)
    private def nonEmpty: Boolean = !l.isEmpty

  override def render: RxElement = div(
    cls -> "pg-output__view pg-plan pg-lineage",
    div(
      cls -> "pg-plan__caption",
      "How tables and seeds flow into models and queries, the dependency graph wvlet build derives from model references. Hover a node to trace it; click it to select its definition."
    ),
    legend,
    Rx.join(lineage, hovered, state.output.map(_.statementLine))
      .map { (l, h, current) =>
        if l.isEmpty then
          div(
            cls -> "pg-plan__caption pg-lineage__empty",
            "No lineage: define a model with model name = { from ... } and read it in a query. The dbt-style Models examples show a few."
          )
        else
          div(cls -> "pg-lineage__canvas", graph(l, h, current))
      },
    lineage.map(buildOrder)
  )

  private def legend: RxElement = div(
    cls -> "pg-lineage__legend",
    LineageKind
      .values
      .toSeq
      .map { k =>
        span(cls -> s"pg-lineage__key pg-lineage__key--${k.label}", k.label)
      },
    span(cls -> "pg-lineage__key pg-lineage__key--materialized", "materialized")
  )

  private def graph(l: ModelLineage, hoveredId: Option[String], current: Option[Int]): RxElement =
    val layout = LineageLayout(l)
    // While a node is hovered, dim everything outside its lineage and light the edges on paths
    // through it: those among what it reads, and those among what reads it
    val upstream                        = hoveredId.map(h => ModelOrder.closure(h, l.upstreamsOf))
    val downstream                      = hoveredId.map(h => ModelOrder.closure(h, l.downstreamsOf))
    val related                         = hoveredId.map(l.related)
    def dimmed(id: String): Boolean     = related.exists(!_.contains(id))
    def onPath(e: LineageEdge): Boolean =
      upstream.exists(u => u(e.from) && u(e.to)) || downstream.exists(d => d(e.from) && d(e.to))
    svg(
      cls -> "pg-lineage__svg",
      // Shrink to the panel width down to minScale, and never scale up past the natural size
      styleAttr -> s"max-width: ${layout.width}px; min-width: ${(layout.width * minScale).toInt}px",
      viewBox   -> s"0 0 ${layout.width} ${layout.height}",
      defs(
        marker(
          id           -> "pg-lineage-arrow",
          viewBox      -> "0 0 8 8",
          refX         -> 1,
          refY         -> 4,
          markerWidth  -> arrow,
          markerHeight -> arrow,
          orient       -> "auto",
          path(cls -> "pg-lineage__arrow", d -> "M 0 0 L 8 4 L 0 8 z")
        )
      ),
      g(
        l.edges
          .flatMap { e =>
            layout
              .edgePath(e.from, e.to)
              .map { p =>
                val on = onPath(e)
                path(
                  cls ->
                    s"pg-lineage__edge${
                        if on then
                          " is-on"
                        else if hoveredId.nonEmpty then
                          " is-dim"
                        else
                          ""
                      }",
                  d         -> p,
                  markerEnd -> "url(#pg-lineage-arrow)"
                )
              }
          }
      ),
      g(
        l.nodes
          .flatMap { n =>
            layout
              .positions
              .get(n.id)
              .map { (nx, ny) =>
                nodeOf(
                  n,
                  nx,
                  ny,
                  layout.widths(n.id),
                  dimmed(n.id),
                  n.line.isDefined && n.line == current
                )
              }
          }
      )
    )

  end graph

  private def nodeOf(
      n: LineageNode,
      nx: Int,
      ny: Int,
      w: Int,
      dim: Boolean,
      current: Boolean
  ): RxElement =
    val classes = Seq(
      "pg-lineage__node",
      s"pg-lineage__node--${n.kind.label}",
      if n.materialized then
        "is-materialized"
      else
        ""
      ,
      if dim then
        "is-dim"
      else
        ""
      ,
      if current then
        "is-current"
      else
        ""
      ,
      if n.line.isDefined then
        "is-link"
      else
        ""
    ).filter(_.nonEmpty).mkString(" ")
    g(
      cls          -> classes,
      svgTransform -> s"translate(${nx}, ${ny})",
      onmouseenter -> { (_: dom.MouseEvent) =>
        hovered := Some(n.id)
      },
      onmouseleave -> { (_: dom.MouseEvent) =>
        hovered := None
      },
      onclick -> { (_: dom.MouseEvent) =>
        n.line.foreach(state.revealLine)
      },
      svgTitle(
        Seq(
          Some(n.label),
          Some(n.detail),
          n.target.map(t => s"persisted to ${t}"),
          n.line.map(l => s"line ${l}")
        ).flatten.mkString("\n")
      ),
      rect(cls -> "pg-lineage__box", width  -> w, height -> nodeH, rx       -> 8),
      rect(cls -> "pg-lineage__band", width -> 4, height -> (nodeH - 12), x -> 6, y -> 6, rx -> 2),
      svgText(cls -> "pg-lineage__label", x  -> textX, y -> 19, truncate(n.label)),
      svgText(cls -> "pg-lineage__detail", x -> textX, y -> 34, truncate(n.detail, maxDetail))
    )

  end nodeOf

  private def buildOrder(l: ModelLineage): RxElement =
    if !l.hasModels then
      span()
    else
      div(
        cls -> "pg-engine-section",
        div(cls -> "pg-engine-section__title", "wvlet build order"),
        l.buildError match
          case Some(err) =>
            div(cls -> "pg-plan__caption pg-lineage__error", err)
          case None if l.buildOrder.isEmpty =>
            div(
              cls -> "pg-plan__caption",
              "Nothing to build: every model is ephemeral and inlined where it is read. Add with { materialized: 'table' } to persist one."
            )
          case None =>
            div(
              div(
                cls -> "pg-plan__caption",
                "wvlet build persists the materialized models in this order; ephemeral models are inlined into them."
              ),
              ol(
                cls -> "pg-lineage__order",
                l.buildOrder
                  .map { name =>
                    val node = l.nodes.find(_.id == s"model:${name}")
                    li(
                      span(cls -> "pg-mono", name),
                      node.map { n =>
                        span(
                          cls -> "pg-lineage__order-detail",
                          s"${n.detail.split(' ').head} → ${n.target.getOrElse(name)}"
                        )
                      }
                    )
                  }
              )
            )
      )

end LineageView

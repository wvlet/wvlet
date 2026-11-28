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
import wvlet.lang.api.v1.frontend.ClusterInfo
import wvlet.lang.api.v1.query.QueryStatus
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.Rx

/**
  * The server's health in one row of tiles: query slots, the queue, failures, engine memory,
  * threads, heap, and low-memory kills, each with a line of its last two minutes. The query tiles
  * filter the list below by their status
  */
class HealthStrip(state: ConsoleState) extends RxElement:
  import Widgets.*

  private def tile(
      name: String,
      value: String,
      sub: String,
      percent: Option[Int] = None,
      line: Seq[Double] = Nil,
      lineMax: Double = 0,
      filter: Option[QueryStatus] = None,
      tone: String = ""
  ): RxElement =
    val active = filter.exists(s => state.statuses.get == Set(s))
    val body   = Seq[DomNode](
      div(cls -> "wv-tile__name", name),
      div(cls -> "wv-tile__value", value),
      div(cls -> "wv-tile__sub", sub),
      percent.map(p => meter(p, s"${name} used")).getOrElse(span()),
      if line.size > 1 then
        sparkline(line, lineMax, s"${name}, last 2 minutes")
      else
        span()
    )
    val classes =
      Seq("wv-tile") ++ Option.when(tone.nonEmpty)(s"wv-tile--${tone}") ++
        Option.when(filter.nonEmpty)("wv-tile--filter") ++ Option.when(active)("wv-tile--active")
    filter match
      case Some(s) =>
        button(
          cls             -> classes.mkString(" "),
          tpe             -> "button",
          aria("pressed") -> active.toString,
          titleAttr       -> s"Show only ${s.toString.toLowerCase} queries",
          onclick         -> { (_: dom.MouseEvent) =>
            state.onlyStatus(s)
          },
          body
        )
      case None =>
        div(cls -> classes.mkString(" "), body)

  end tile

  private def tiles(c: ClusterInfo, hist: Vector[ClusterInfo], failed: Int): RxElement =
    val q    = c.queries
    val pool = c.memoryPool
    val eng  = c.engineThreads
    val heap = c.heap
    div(
      cls       -> "wv-health",
      role      -> "group",
      ariaLabel -> "Server health",
      tile(
        "Running",
        s"${q.running}",
        s"of ${q.maxRunning} slots",
        Some(Fmt.percent(q.running, q.maxRunning)),
        hist.map(_.queries.running.toDouble),
        q.maxRunning,
        Some(QueryStatus.RUNNING)
      ),
      tile(
        "Queued",
        s"${q.queued}",
        s"limit ${Fmt.count(q.maxQueued)}",
        line = hist.map(_.queries.queued.toDouble),
        filter = Some(QueryStatus.QUEUED),
        tone =
          if q.queued > 0 then
            "warn"
          else
            ""
      ),
      tile(
        "Failed",
        s"${failed}",
        // The count follows the session, profile, and text filters, like the chips
        if state.session.get.nonEmpty || state.profile.get.nonEmpty || state.text.get.trim.nonEmpty
        then
          "matching the filters"
        else
          "kept in history"
        ,
        filter = Some(QueryStatus.FAILED),
        tone =
          if failed > 0 then
            "danger"
          else
            ""
      ),
      tile(
        "Engine memory",
        Fmt.bytes(pool.reservedBytes),
        pool
          .capacityBytes
          .map(cap => s"of ${Fmt.bytes(cap)} · ${pool.openQueries} open")
          .getOrElse(s"no pool limit · ${pool.openQueries} open"),
        pool.capacityBytes.map(cap => Fmt.percent(pool.reservedBytes, cap)),
        hist.map(_.memoryPool.reservedBytes.toDouble),
        pool.capacityBytes.map(_.toDouble).getOrElse(0.0)
      ),
      tile(
        "Engine threads",
        s"${eng.busy}/${eng.threads}",
        s"${Fmt.count(eng.waitingTasks)} tasks waiting",
        Some(Fmt.percent(eng.busy, eng.threads)),
        hist.map(_.engineThreads.busy.toDouble),
        eng.threads
      ),
      tile(
        "Heap",
        Fmt.bytes(heap.usedBytes),
        s"of ${Fmt.bytes(heap.maxBytes)}",
        Some(Fmt.percent(heap.usedBytes, heap.maxBytes)),
        hist.map(_.heap.usedBytes.toDouble),
        heap.maxBytes.toDouble
      ),
      tile(
        "Memory kills",
        s"${pool.killedQueries}",
        pool
          .lowMemoryKillerDelayMillis
          .map(d => s"after ${Fmt.duration(d)} over the pool")
          .getOrElse("low-memory killer off"),
        tone =
          if pool.killedQueries > 0 then
            "danger"
          else
            ""
      )
    )

  end tiles

  override def render: RxElement = Rx
    .join(state.history, state.list, state.statuses)
    .map { (hist, l, _) =>
      hist.lastOption match
        case None =>
          div(cls -> "wv-health wv-health--loading", "Connecting to the server…")
        case Some(c) =>
          val failed = l
            .flatMap(_.facets.statuses.find(_.value == QueryStatus.FAILED.toString))
            .map(_.count)
            .getOrElse(0)
          tiles(c, hist, failed)
    }

end HealthStrip

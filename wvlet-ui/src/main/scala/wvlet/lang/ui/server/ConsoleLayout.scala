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

import wvlet.lang.api.v1.query.QuerySummary

import java.time.Instant

/** Widths of a row's time bar, in percent of the slowest row in view */
case class TimeBar(queuedPct: Double, runPct: Double)

/**
  * One query on the timeline, with offsets and widths in percent of the time window, and the
  * sub-row of its lane, so queries overlapping in time sit on separate rows
  */
case class TimelineBar(
    query: QuerySummary,
    startPct: Double,
    queuedPct: Double,
    runPct: Double,
    row: Int = 0
):
  def endPct: Double = startPct + queuedPct + runPct

/** A timeline lane: the queries of one session, oldest first, on `rows` sub-rows */
case class TimelineLane(session: String, bars: Seq[TimelineBar]):
  def rows: Int = bars.iterator.map(_.row + 1).maxOption.getOrElse(1)

/** The timeline of the queries in view: a time window and one lane per session */
case class TimelineLayout(start: Instant, end: Instant, lanes: Seq[TimelineLane]):
  def spanMillis: Long = (end.toEpochMilli - start.toEpochMilli).max(1L)

/** Layout math of the console's rows and timeline, kept free of the DOM so it can be tested */
object ConsoleLayout:

  /** The label of the session lane of queries without a session id */
  val DefaultSession: String = "default"

  /** Queued plus running time of a query, so far for one that has not finished */
  def totalMillis(q: QuerySummary): Long = q.queuedMillis + q.runMillis.getOrElse(0L)

  /** The time bars of `qs`, scaled so the slowest query fills its bar */
  def timeBars(qs: Seq[QuerySummary]): Map[String, TimeBar] =
    val slowest = qs.iterator.map(totalMillis).maxOption.getOrElse(0L).max(1L).toDouble
    qs.iterator
      .map { q =>
        q.queryId ->
          TimeBar(q.queuedMillis * 100.0 / slowest, q.runMillis.getOrElse(0L) * 100.0 / slowest)
      }
      .toMap

  /** Memory bar widths of `qs` in percent of the largest peak in view; None when not tracked */
  def memoryBars(qs: Seq[QuerySummary]): Map[String, Double] =
    val tracked = qs.filter(_.memory.tracked)
    val largest = tracked.iterator.map(peakOrReserved).maxOption.getOrElse(0L).max(1L).toDouble
    tracked.iterator.map(q => q.queryId -> peakOrReserved(q) * 100.0 / largest).toMap

  /** The memory a query holds now when running, or held at most when finished */
  def peakOrReserved(q: QuerySummary): Long =
    if q.status.isFinished then
      q.memory.peakBytes
    else
      q.memory.reservedBytes.max(q.memory.peakBytes)

  /**
    * The timeline of `qs` up to `now`: the window starts at the oldest query and ends at `now` (or
    * the last completion when every query finished), lanes are sessions in order of their first
    * query
    */
  def timeline(qs: Seq[QuerySummary], now: Instant): TimelineLayout =
    if qs.isEmpty then
      TimelineLayout(now, now, Nil)
    else
      val start = qs.iterator.map(_.createdAt).minBy(_.toEpochMilli)
      val end   =
        if qs.forall(_.status.isFinished) then
          qs.iterator.map(q => q.completedAt.getOrElse(now)).maxBy(_.toEpochMilli)
        else
          now
      val window                    = TimelineLayout(start, end, Nil)
      val span                      = window.spanMillis.toDouble
      def pct(millis: Long): Double = (millis * 100.0 / span).max(0.0)
      val lanes                     = qs
        .sortBy(_.createdAt.toEpochMilli)
        .groupBy(_.sessionId.getOrElse(DefaultSession))
        .toSeq
        .map { (s, lane) =>
          val bars = lane.map { q =>
            val offset = q.createdAt.toEpochMilli - start.toEpochMilli
            TimelineBar(q, pct(offset), pct(q.queuedMillis), pct(q.runMillis.getOrElse(0L)))
          }
          TimelineLane(s, packRows(bars))
        }
        .sortBy(_.bars.head.query.createdAt.toEpochMilli)
      window.copy(lanes = lanes)

  /** Put each bar (oldest first) on the first sub-row free at its start */
  private def packRows(bars: Seq[TimelineBar]): Seq[TimelineBar] =
    val rowEnds = scala.collection.mutable.ArrayBuffer.empty[Double]
    bars.map { b =>
      val free = rowEnds.indexWhere(_ <= b.startPct)
      val row  =
        if free >= 0 then
          free
        else
          rowEnds += 0.0
          rowEnds.size - 1
      rowEnds(row) = b.endPct
      b.copy(row = row)
    }

end ConsoleLayout

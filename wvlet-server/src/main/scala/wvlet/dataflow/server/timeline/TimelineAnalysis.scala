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
package wvlet.dataflow.server.timeline

import wvlet.dataflow.api.v1.timeline.TimeInterval
import wvlet.log.LogSupport

import scala.collection.mutable

object TimelineAnalysis extends LogSupport:

  case class PeakChange(pos: Long, count: Int)

  def findPeakTimes[U](
      list: Seq[TimeInterval],
      reporter: PeakChange => U = { (x: PeakChange) =>
        info(s"peak change at ${x.pos}: ${x.count}")
      }
  ): Unit =
    if list.isEmpty then {
      // do nothing
    } else
      val sorted = list.sortBy(_.startAt).toIndexedSeq
      val length = sorted.length
      // sorted intervals on endAt values
      val overlappedIntervals =
        new mutable.PriorityQueue[TimeInterval]()(TimeInterval.intervalSweepOrdering)
      overlappedIntervals += sorted(0)
      var sweepLine = sorted(0).startAt
      var index     = 1

      val peakCountReport = mutable.TreeMap.empty[Long, Int]

      def report: Unit =
        val precedingIntervals = peakCountReport.range(Long.MinValue, sweepLine).toIndexedSeq
        precedingIntervals.sortBy(_._1).foreach { (pos, count) =>
          reporter(PeakChange(pos, count))
          // Clean up reported peak count
          peakCountReport.remove(pos)
        }

      def sweepUpto(to: Long): Unit =
        // Report before updating the sweepline
        report
        sweepLine = to

      while index < length do
        peakCountReport += sweepLine -> overlappedIntervals.size

        // Add the next interval to the queue
        val x = sorted(index)
        sweepUpto(x.startAt)

        // Remove all intervals that end before x.startAt
        while overlappedIntervals.nonEmpty && overlappedIntervals.head.endAt <= sweepLine do
          val r = overlappedIntervals.dequeue()
          peakCountReport += r.endAt -> overlappedIntervals.size
        overlappedIntervals += x
        peakCountReport += sweepLine -> overlappedIntervals.size
        index += 1
      // Sweep remaining intervals
      while overlappedIntervals.nonEmpty do
        val r = overlappedIntervals.dequeue()
        peakCountReport += r.endAt -> overlappedIntervals.size
        sweepUpto(r.endAt)
      // final report
      sweepLine = Long.MaxValue
      report

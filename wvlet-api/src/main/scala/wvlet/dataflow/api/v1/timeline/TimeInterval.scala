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
package wvlet.dataflow.api.v1.timeline

import scala.concurrent.duration.TimeUnit

/**
  * Represent [startAt, endAt) interval range without specifying time unit
  */
case class TimeInterval(
    name: String,
    startAt: Long,
    endAt: Long,
    color: Option[String] = None,
    tags: Map[String, Any] = Map.empty
) extends Ordered[TimeInterval]:
  require(startAt <= endAt, s"startAt (${startAt}) must be <= endAt (${endAt})")

  override def toString: String = s"${name}:[$startAt, $endAt)"

  def interval: (Long, Long) = (startAt, endAt)

  def length: Long = endAt - startAt

  def contains(other: TimeInterval): Boolean =
    startAt <= other.startAt && other.endAt <= endAt
  def contains(pos: Long): Boolean =
    startAt <= pos && pos < endAt

  def precedes(other: TimeInterval): Boolean =
    endAt <= other.startAt

  def follows(other: TimeInterval): Boolean =
    other.endAt <= startAt

  def overlaps(other: TimeInterval): Boolean =
    if startAt <= other.startAt then other.startAt < endAt
    else startAt < other.endAt

  override def compare(other: TimeInterval): Int =
    val diff = startAt - other.startAt
    if diff == 0 then endAt.compareTo(other.endAt)
    else if diff < 0 then -1
    else 1

object TimeInterval:

  /**
    * Order intervals first by the endAt, then by the startAt
    */
  def intervalSweepOrdering: Ordering[TimeInterval] = new Ordering[TimeInterval]:
    override def compare(x: TimeInterval, y: TimeInterval): Int =
      val diff = y.endAt - x.endAt
      if diff == 0 then y.startAt.compareTo(x.startAt)
      else if diff < 0 then -1
      else 1

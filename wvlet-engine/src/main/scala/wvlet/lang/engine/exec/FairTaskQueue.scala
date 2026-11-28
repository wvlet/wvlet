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
package wvlet.lang.engine.exec

import scala.collection.mutable

/**
  * The queued tasks of several [[TaskGroup]]s (queries), handed out fairly across groups by a
  * multilevel feedback queue on each group's accumulated run time, like Trino's time-sharing task
  * executor:
  *
  *   - A group's run time is that of its finished tasks plus how long its running tasks have run so
  *     far. A group is at the highest level whose threshold its run time has reached
  *     ([[FairTaskQueue.LevelThresholdNanos]]), so a long query moves down as it runs.
  *   - The next task comes from the non-empty level that has run the least time relative to its
  *     share. Each level's share is `levelShareRatio` times the next lower one's, so short (new)
  *     queries take most of the pool, while a long query still gets its level's share and is never
  *     starved by a stream of short ones.
  *   - Within a level, the group that has run the least goes first (the earliest queued on ties),
  *     so a query of short tasks isn't outrun by one of long tasks that takes the same number of
  *     turns, nor by a new query that has just taken every thread. Each group's tasks run in FIFO
  *     order.
  *
  * Not thread-safe: the pool guards it with its lock. Tasks are not preempted, so the shares hold
  * over many tasks, not within one.
  *
  * With `coordinators`, run time is that of the groups' coordinator tasks
  * ([[TaskGroup.coordinatorNanos]]), kept apart from their pool tasks' run time
  */
final class FairTaskQueue[T](
    levelThresholdNanos: IndexedSeq[Long] = FairTaskQueue.LevelThresholdNanos,
    levelShareRatio: Int = 2,
    coordinators: Boolean = false
):
  import FairTaskQueue.Picked

  require(levelThresholdNanos.nonEmpty && levelThresholdNanos.head == 0L)
  require(levelShareRatio >= 1)

  // Kept while the group has queued or running tasks
  private final class GroupTasks(val group: TaskGroup):
    val tasks         = mutable.Queue.empty[T]
    var running       = 0
    var runningStarts = 0L // The sum of the running tasks' start times

    /** The run time of the finished tasks and of the running ones until `now` */
    def runNanos(now: Long): Long = finishedNanos(group) + running * now - runningStarts

  private def finishedNanos(g: TaskGroup): Long =
    if coordinators then
      g.coordinatorNanos
    else
      g.runNanos

  private val numLevels = levelThresholdNanos.size
  private val groups    = mutable.HashMap.empty[TaskGroup, GroupTasks]
  private val levels    = Array.fill(numLevels)(mutable.ArrayBuffer.empty[GroupTasks])
  // Run time of the tasks picked at each level, times the level's weight (the inverse of its share)
  private val levelTime = Array.fill(numLevels)(0L)
  private val weights   = Array.tabulate(numLevels)(l => math.pow(levelShareRatio, l).toLong)
  private var queued    = 0

  /** The number of queued tasks */
  def size: Int = queued

  /** Whether no task is queued */
  def isEmpty: Boolean = queued == 0

  /** The level of a group whose finished tasks ran `nanos` in total */
  def levelOf(nanos: Long): Int =
    var l = numLevels - 1
    while levelThresholdNanos(l) > nanos do
      l -= 1
    l

  /** Queue a task of `group`, after its earlier queued tasks, at time `now` (nanoseconds) */
  def add(group: TaskGroup, task: T, now: Long): Unit =
    val g = groups.getOrElseUpdate(group, GroupTasks(group))
    if g.tasks.isEmpty then
      enqueue(g, levelOf(g.runNanos(now)))
    g.tasks += task
    queued += 1

  /**
    * Take the next task at time `now` (nanoseconds), or null when none is queued. Pass the result
    * to [[finished]] after the task has run
    */
  def poll(now: Long): Picked[T] =
    var picked: Picked[T] = null
    while picked == null && queued > 0 do
      val l     = pickLevel()
      val level = levels(l)
      val i     = leastRun(level, now)
      val g     = level(i)
      // The group's level is checked when its turn comes: its tasks may have run long enough since
      // it was queued to move it down
      val actual = levelOf(g.runNanos(now))
      if actual != l then
        level.remove(i)
        enqueue(g, actual)
      else
        val t = g.tasks.dequeue()
        queued -= 1
        if g.tasks.isEmpty then
          level.remove(i)
        g.running += 1
        g.runningStarts += now
        picked = Picked(t, g.group, l, now)
    picked

  /** Account the run time of a task [[poll]] returned */
  def finished(p: Picked[T], nanos: Long): Unit =
    if coordinators then
      p.group.addCoordinatorNanos(nanos)
    else
      p.group.addRunNanos(nanos)
    levelTime(p.level) += nanos * weights(p.level)
    groups
      .get(p.group)
      .foreach { g =>
        g.running -= 1
        g.runningStarts -= p.startNanos
        if g.running == 0 && g.tasks.isEmpty then
          groups.remove(p.group)
      }

  private def enqueue(g: GroupTasks, level: Int): Unit =
    if levels(level).isEmpty then
      catchUp(level)
    levels(level) += g

  /**
    * A level that has been idle starts at the least weighted time of the non-empty levels, so that
    * its old run time neither makes it win every pick until the others catch up nor wait for them
    */
  private def catchUp(level: Int): Unit =
    var min = Long.MaxValue
    var l   = 0
    while l < numLevels do
      if l != level && levels(l).nonEmpty then
        min = math.min(min, levelTime(l))
      l += 1
    if min != Long.MaxValue then
      levelTime(level) = min

  /** The index of the group that has run the least, the first one on ties */
  private def leastRun(level: mutable.ArrayBuffer[GroupTasks], now: Long): Int =
    var best    = 0
    var bestRun = level(0).runNanos(now)
    var i       = 1
    while i < level.size do
      val r = level(i).runNanos(now)
      if r < bestRun then
        best = i
        bestRun = r
      i += 1
    best

  /** The non-empty level with the least weighted run time, the lower level on ties */
  private def pickLevel(): Int =
    var best = -1
    var l    = 0
    while l < numLevels do
      if levels(l).nonEmpty && (best < 0 || levelTime(l) < levelTime(best)) then
        best = l
      l += 1
    best

end FairTaskQueue

object FairTaskQueue:
  /**
    * Run time at which a group enters each level: 0, 1 s, 10 s, 60 s, and 300 s, summed over the
    * group's tasks (Trino's time-sharing executor levels)
    */
  val LevelThresholdNanos: IndexedSeq[Long] = IndexedSeq(0L, 1L, 10L, 60L, 300L).map(
    _ * 1000000000L
  )

  /** A task taken from the queue, with its group, the level it was picked at, and when */
  final case class Picked[T](task: T, group: TaskGroup, level: Int, startNanos: Long)

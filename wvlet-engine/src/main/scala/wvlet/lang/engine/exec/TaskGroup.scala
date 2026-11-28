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

import java.util.concurrent.atomic.AtomicLong

/**
  * The tasks of one query, which a [[TaskRunner]] schedules fairly against other queries' tasks
  * ([[FairTaskQueue]]). A task belongs to the group current on the thread that submits it
  * ([[TaskGroup.current]]); the pool thread running it makes that group current while it runs, so
  * tasks submitted from tasks stay in the same group
  */
final class TaskGroup private (
    /** A process-wide sequence number, for logs */
    val id: Long
):
  private val nanos = AtomicLong(0)
  private val tasks = AtomicLong(0)

  /** The run time of this group's finished pool tasks */
  def runNanos: Long = nanos.get()

  /** The number of this group's finished pool tasks */
  def finishedTasks: Long = tasks.get()

  private[exec] def addRunNanos(n: Long): Unit =
    nanos.addAndGet(n)
    tasks.incrementAndGet()

  override def toString: String = s"TaskGroup(${id})"

object TaskGroup:
  private val ids          = AtomicLong(0)
  private val currentGroup = ThreadLocal[TaskGroup]()

  /** A new group, e.g. for one query */
  def newGroup(): TaskGroup = TaskGroup(ids.incrementAndGet())

  /** The group of tasks this thread submits, or null outside of any group */
  def current: TaskGroup = currentGroup.get()

  /**
    * Run `body` with `g` (null for none) as this thread's group, restoring the previous one after
    */
  def withGroup[A](g: TaskGroup)(body: => A): A =
    val prev = currentGroup.get()
    if prev eq g then
      body
    else
      currentGroup.set(g)
      try body
      finally currentGroup.set(prev)

  /**
    * Run `body` in a new group unless this thread is already in one (a nested query, e.g. a
    * [[wvlet.lang.engine.TableWriter]] statement's inner reads, stays in its caller's group)
    */
  def asQuery[A](body: => A): A =
    if currentGroup.get() != null then
      body
    else
      withGroup(newGroup())(body)

end TaskGroup

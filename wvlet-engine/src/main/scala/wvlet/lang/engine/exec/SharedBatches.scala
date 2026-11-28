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

import wvlet.lang.engine.vector.*

/**
  * A value computed at most once, by the first thread that asks for it ([[get]]) or starts it
  * ([[run]]). Other threads asking meanwhile wait for that computation. A failure is kept like a
  * value and raised to every thread that asks, so a result started early fails a query only when a
  * reader reads it.
  */
final class ComputeOnce[A](compute: () => A):
  // 0: not started, 1: running, 2: done (guarded by this)
  private var state                       = 0
  private var value: Either[Throwable, A] = null

  /** Whether a thread has started the computation */
  def isStarted: Boolean = synchronized(state != 0)

  /** Compute the value on this thread, unless another thread has started; doesn't wait */
  def run(): Unit = runUnless(() => false)

  /**
    * [[run]], unless `stop` holds when it would start. `stop` is checked under the same lock as
    * [[awaitIfStarted]], so a caller that makes `stop` hold and then waits either sees the
    * computation started (and waits for it) or knows it never starts
    */
  def runUnless(stop: () => Boolean): Unit =
    val mine = synchronized {
      if state == 0 && !stop() then
        state = 1
        true
      else
        false
    }
    if mine then
      val v =
        try
          Right(compute())
        catch
          case e: Throwable =>
            Left(e)
      synchronized {
        value = v
        state = 2
        notifyAll()
      }

  /** Wait until the computation is done if a thread has started it; never starts it */
  def awaitIfStarted(): Unit = synchronized {
    while state == 1 do
      TaskRunner.awaitNotify(this)
  }

  /**
    * Replace a computed value with the failure `e`, so that the value can be collected and later
    * reads raise `e`. Whether the value was computed (nothing changes otherwise)
    */
  def discard(e: Throwable): Boolean = synchronized {
    if state == 2 then
      value = Left(e)
      true
    else
      false
  }

  /** The value, computed on this thread if no thread has started it; raises its failure */
  def get(): A =
    run()
    val v = synchronized {
      while state != 2 do
        TaskRunner.awaitNotify(this)
      value
    }
    v match
      case Right(a) =>
        a
      case Left(e) =>
        throw e

end ComputeOnce

/** Batches computed once and read by several readers (e.g. a CTE read more than once) */
trait SharedBatches:
  /** The plan that computes the batches */
  def op: Operator

  /** Whether the computation has started (or is done) */
  def isComputed: Boolean

  /**
    * Compute the batches on this thread unless another thread has started, or `stop` holds when it
    * would start ([[ComputeOnce.runUnless]]); doesn't wait
    */
  def compute(stop: () => Boolean): Unit

  /** Wait until the batches are computed if a thread has started them; never starts them */
  def awaitIfStarted(): Unit

/**
  * Starts the computation of `shared` results on coordinator threads of `runner` when the plan
  * starts running, so that independent results are computed at the same time instead of one by one
  * when a reader first reads each. `shared` lists the results a result reads before it, so that
  * those start first. A reader that reaches a result no coordinator has started yet computes it on
  * its own thread, so a limit on coordinators can't make readers wait forever.
  *
  * Closing it cancels the coordinators that haven't started, and waits for the running ones, so no
  * work outlives the query. A failure stays with its result and is raised only when a reader reads
  * that result. EXPLAIN shows only the child.
  */
class StartSharedOp(val child: Operator, shared: Seq[SharedBatches], runner: TaskRunner)
    extends Operator:
  override def schema: Schema          = child.schema
  override def name: String            = child.name
  override def children: Seq[Operator] = Seq(child)
  private var handles                  = Seq.empty[TaskRunner.Handle[Unit]]
  private var started                  = false
  @volatile
  private var closed = false

  override def explain(indent: Int): String = child.explain(indent)

  def next(): Batch =
    if !started then
      started = true
      handles = shared
        .filterNot(_.isComputed)
        .map(s => runner.coordinate(() => s.compute(() => closed)))
    child.next()

  override def close(): Unit =
    if handles.nonEmpty then
      // Coordinators that haven't started computing never will; wait for those that have. A
      // future's cancel also succeeds on a running task, so it only drops queued ones early
      closed = true
      handles.foreach(_.cancel())
      shared.foreach(_.awaitIfStarted())
      handles = Nil
    child.close()

end StartSharedOp

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

/**
  * Runs independent tasks, concurrently on platforms with threads (JVM, Scala Native) and one at a
  * time on Scala.js. The engine's parallel operators submit work here and wait for the results on
  * the calling thread, so tasks must not themselves wait for other tasks of the same runner.
  */
trait TaskRunner:
  /** The number of tasks that run at the same time */
  def parallelism: Int

  def submit[A](task: () => A): TaskRunner.Handle[A]

  /**
    * Run a task that may itself wait for tasks of this runner (e.g. drain a plan with parallel
    * operators), on a coordinator thread outside the pool, so that pool threads never wait for pool
    * tasks. Runs the task when it is submitted, on the calling thread, where there are no threads
    */
  def coordinate[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

object TaskRunner extends TaskRunnerCompatImpl:

  /** A submitted task */
  trait Handle[A]:
    /** Wait for the result, rethrowing the task's exception */
    def get(): A

    /** Cancel the task if it hasn't started */
    def cancel(): Unit

  /** Runs each task when it is submitted, on the calling thread */
  object Sequential extends TaskRunner:
    def parallelism: Int = 1

    def submit[A](task: () => A): Handle[A] =
      val result: Either[Throwable, A] =
        try
          Right(task())
        catch
          case e: Throwable =>
            Left(e)
      new Handle[A]:
        def get(): A =
          result match
            case Right(v) =>
              v
            case Left(e) =>
              throw e
        def cancel(): Unit = ()

  /**
    * Run `tasks` that may each wait for tasks of `runner` (e.g. the hash join builds of one
    * pipeline) at the same time: the first on the calling thread, the others on coordinator threads
    * ([[TaskRunner.coordinate]]). A task no coordinator has started yet runs on the calling thread
    * when its turn comes, so a limit on coordinators never makes the caller wait for queued work.
    * Returns after every task has finished, raising the first failure
    */
  def runTogether(runner: TaskRunner, tasks: Seq[() => Unit]): Unit =
    if tasks.size <= 1 || runner.parallelism <= 1 then
      tasks.foreach(_())
    else
      val failed = java.util.concurrent.atomic.AtomicBoolean(false)
      val once   = tasks.map { t =>
        ComputeOnce { () =>
          try
            t()
          catch
            case e: Throwable =>
              failed.set(true)
              throw e
        }
      }
      // After a failure, tasks that haven't started never start
      val handles = once.tail.map(c => runner.coordinate(() => c.runUnless(() => failed.get())))
      var error: Throwable = null
      once.foreach { c =>
        if error == null then
          try
            c.get()
          catch
            case e: Throwable =>
              error = e
        else
          c.runUnless(() => true)
          c.awaitIfStarted()
      }
      // Queued coordinator entries of tasks run here would keep their builds alive until dequeued
      handles.foreach(_.cancel())
      if error != null then
        throw error

  /**
    * The parallelism to use: the WVLET_ENGINE_PARALLELISM environment variable or the
    * wvlet.engine.parallelism system property when set, otherwise the number of processors
    */
  def configuredParallelism(processors: Int): Int = sys
    .props
    .get("wvlet.engine.parallelism")
    .orElse(sys.env.get("WVLET_ENGINE_PARALLELISM"))
    .flatMap(_.trim.toIntOption)
    .getOrElse(processors)
    .max(1)

end TaskRunner

/** The platform's default task runner */
trait TaskRunnerCompat:
  def default: TaskRunner

  /**
    * Wait on `lock`'s monitor (held by the caller) until another thread notifies it. Scala.js has a
    * single thread, so there is never another thread to wait for
    */
  def awaitNotify(lock: AnyRef): Unit

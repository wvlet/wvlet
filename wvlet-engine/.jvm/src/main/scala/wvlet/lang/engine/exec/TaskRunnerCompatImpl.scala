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

import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** A fixed pool of daemon threads, shared by every query */
trait TaskRunnerCompatImpl extends TaskRunnerCompat:
  lazy val default: TaskRunner =
    val n = TaskRunner.configuredParallelism(Runtime.getRuntime.availableProcessors())
    if n <= 1 then
      TaskRunner.Sequential
    else
      ThreadPoolRunner(n)

  def awaitNotify(lock: AnyRef): Unit = lock.wait()

/**
  * Runs tasks on a fixed thread pool. Coordinator tasks ([[TaskRunner.coordinate]]) run on a
  * second, smaller pool of daemon threads (at most half the parallelism), which exit when idle
  */
class ThreadPoolRunner(val parallelism: Int) extends TaskRunner:
  private val counter = AtomicInteger(0)

  private def daemons(prefix: String): ThreadFactory =
    new ThreadFactory:
      def newThread(r: Runnable): Thread =
        val t = Thread(r, s"${prefix}-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t

  private val pool: ExecutorService = Executors.newFixedThreadPool(
    parallelism,
    daemons("wvlet-engine")
  )

  private val coordinators: ThreadPoolExecutor =
    val n = math.max(1, parallelism / 2)
    val e = ThreadPoolExecutor(
      n,
      n,
      30L,
      TimeUnit.SECONDS,
      LinkedBlockingQueue[Runnable](),
      daemons("wvlet-engine-coordinator")
    )
    e.allowCoreThreadTimeOut(true)
    e

  def submit[A](task: () => A): TaskRunner.Handle[A] = handle(pool.submit(() => task()))

  override def coordinate[A](task: () => A): TaskRunner.Handle[A] = handle(
    coordinators.submit(() => task())
  )

  private def handle[A](f: Future[A]): TaskRunner.Handle[A] =
    new TaskRunner.Handle[A]:
      def get(): A =
        try
          f.get()
        catch
          case e: ExecutionException if e.getCause != null =>
            throw e.getCause
      def cancel(): Unit = f.cancel(false)

  def shutdown(): Unit =
    pool.shutdownNow()
    coordinators.shutdownNow()

end ThreadPoolRunner

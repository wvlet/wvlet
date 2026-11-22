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
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

/** A fixed pool of daemon threads, shared by every query */
trait TaskRunnerCompatImpl extends TaskRunnerCompat:
  lazy val default: TaskRunner =
    val n = TaskRunner.configuredParallelism(Runtime.getRuntime.availableProcessors())
    if n <= 1 then
      TaskRunner.Sequential
    else
      ThreadPoolRunner(n)

/** Runs tasks on a fixed thread pool */
class ThreadPoolRunner(val parallelism: Int) extends TaskRunner:
  private val counter               = AtomicInteger(0)
  private val pool: ExecutorService = Executors.newFixedThreadPool(
    parallelism,
    new ThreadFactory:
      def newThread(r: Runnable): Thread =
        val t = Thread(r, s"wvlet-engine-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t
  )

  def submit[A](task: () => A): TaskRunner.Handle[A] =
    val f: Future[A] = pool.submit(() => task())
    new TaskRunner.Handle[A]:
      def get(): A =
        try
          f.get()
        catch
          case e: ExecutionException if e.getCause != null =>
            throw e.getCause
      def cancel(): Unit = f.cancel(false)

  def shutdown(): Unit = pool.shutdownNow()

end ThreadPoolRunner

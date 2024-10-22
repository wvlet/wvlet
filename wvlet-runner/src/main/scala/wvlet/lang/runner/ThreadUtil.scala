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
package wvlet.lang.runner

import org.apache.arrow.flatbuf.TimeUnit
import wvlet.airframe.ulid.ULID

import java.util.concurrent
import java.util.concurrent.{Executors, Future}

object ThreadUtil:
  def runBackgroundTask(f: () => Unit): Thread =
    val t =
      new Thread:
        override def run(): Unit = f()

    t.setName(s"wvlet-background-task-${ULID.newULID}")
    t.setDaemon(true)
    t.start()
    t

class ThreadManager() extends AutoCloseable:
  private val executor = Executors.newCachedThreadPool(
    wvlet.airframe.control.ThreadUtil.newDaemonThreadFactory(s"wvlet-task-${ULID.newULID}")
  )

  override def close(): Unit =
    executor.shutdownNow()
    while !executor.awaitTermination(10, concurrent.TimeUnit.MILLISECONDS) do
      Thread.sleep(10, TimeUnit.MILLISECOND)

  def runBackgroundTask(f: () => Unit): Future[?] = executor.submit(
    new Runnable:
      override def run(): Unit = f()
  )

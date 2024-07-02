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
package wvlet.dataflow.server.util

import wvlet.log.LogSupport

import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ForkJoinPool, ForkJoinWorkerThread, ThreadFactory, TimeUnit}

class ThreadManager(name: String, numThreads: Int = Runtime.getRuntime.availableProcessors() * 2)
    extends AutoCloseable
    with LogSupport:
  private val executorService =
    new ForkJoinPool(
      // The number of threads
      numThreads,
      new ForkJoinWorkerThreadFactory():
        private val threadCount = new AtomicInteger()
        override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread =
          val thread     = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
          val threadName = s"${name}-${threadCount.getAndIncrement()}"
          // Using damon threads
          thread.setDaemon(true)
          thread.setName(threadName)
          thread
      ,
      // Use the default behavior for unrecoverable exceptions
      null,
      // Enable asyncMode
      true
    )

  def submit[U](body: => U): Unit =
    executorService.submit(
      new Runnable:
        override def run(): Unit = body
    )

  override def close(): Unit =
    info(s"[${name}] Terminating the thread manager")
    executorService.shutdownNow()

/**
  * Scheduled thread manager
  */
class ScheduledThreadManager(name: String, numThreads: Int = 1) extends AutoCloseable with LogSupport:
  private val scheduledExecutorService = Executors.newScheduledThreadPool(numThreads)

  def scheduleOneshotTask[U](delay: Long, unit: TimeUnit)(body: => U): Unit =
    scheduledExecutorService.schedule(
      new Runnable:
        override def run(): Unit =
          body
      ,
      delay,
      unit
    )

  def scheduleWithFixedDelay[U](initialDelay: Long, period: Long, unit: TimeUnit)(
      body: () => U
  ): Unit =
    scheduledExecutorService.scheduleWithFixedDelay(
      new Runnable:
        override def run(): Unit =
          body()
      ,
      initialDelay,
      period,
      unit
    )

  override def close(): Unit =
    info(s"[${name}] Terminating the thread manager")
    scheduledExecutorService.shutdownNow()

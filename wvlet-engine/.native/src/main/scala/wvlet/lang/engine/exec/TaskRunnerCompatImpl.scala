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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
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
  * second, smaller pool of daemon threads (at most half the parallelism), which exit when idle.
  *
  * A task submitted from a thread in a [[TaskGroup]] (a query, with `fair_task_scheduling`) runs on
  * a pool that picks tasks fairly across groups ([[FairTaskQueue]]); other tasks run on a pool with
  * one FIFO queue. With `fair_coordinators`, a group's coordinator tasks likewise run on a fair
  * pool of coordinator threads, by the groups' coordinator run time. Each pool is started on its
  * first task
  */
class ThreadPoolRunner(val parallelism: Int) extends TaskRunner:
  private val counter = AtomicInteger(0)

  private def daemons(prefix: String): ThreadFactory =
    new ThreadFactory:
      def newThread(r: Runnable): Thread =
        val t = Thread(r, s"${prefix}-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t

  // Set when a pool starts, so that shutdown doesn't start the other one
  @volatile
  private var poolStarted = false

  @volatile
  private var fairStarted = false

  @volatile
  private var fairCoordinatorsStarted = false

  @volatile
  private var shutDown = false

  private def checkRunning(): Unit =
    if shutDown then
      throw RejectedExecutionException("The task runner is shut down")

  private lazy val pool: ThreadPoolExecutor =
    poolStarted = true
    ThreadPoolExecutor(
      parallelism,
      parallelism,
      0L,
      TimeUnit.MILLISECONDS,
      LinkedBlockingQueue[Runnable](),
      daemons("wvlet-engine")
    )

  private lazy val fair: FairPool =
    fairStarted = true
    FairPool(parallelism, daemons("wvlet-engine-fair"))

  private def coordinatorThreads: Int = math.max(1, parallelism / 2)

  private lazy val fairCoordinators: FairPool =
    fairCoordinatorsStarted = true
    FairPool(coordinatorThreads, daemons("wvlet-engine-coordinator-fair"), coordinators = true)

  private val coordinators: ThreadPoolExecutor =
    val n = coordinatorThreads
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

  def submit[A](task: () => A): TaskRunner.Handle[A] =
    checkRunning()
    val g = TaskGroup.current
    val s = StartGate()
    if g == null then
      handle(pool.submit(() => s.run(task)), s)
    else
      handle(fair.submit(g, () => s.run(task)), s)

  override def coordinate[A](task: () => A): TaskRunner.Handle[A] =
    val g = TaskGroup.current
    val s = StartGate()
    if g != null && g.fairCoordinators then
      checkRunning()
      // The fair pool runs the task in its group
      handle(fairCoordinators.submit(g, () => s.run(task)), s)
    else
      // The coordinator's own tasks stay in the submitter's group
      handle(coordinators.submit(() => TaskGroup.withGroup(g)(s.run(task))), s)

  /**
    * A handle whose cancel drops the task only if it hasn't started: Future.cancel would also mark
    * a running task cancelled, so that get() returned at once while the task ran on, and a closing
    * operator (e.g. a failed query's MorselOp) would stop waiting for it
    */
  private def handle[A](f: Future[A], s: StartGate): TaskRunner.Handle[A] =
    new TaskRunner.Handle[A]:
      def get(): A =
        try
          f.get()
        catch
          case e: ExecutionException if e.getCause != null =>
            throw e.getCause
      def cancel(): Unit =
        if s.cancel() then
          // Drops the queued task's closure now instead of when a thread dequeues it
          f.cancel(false)

  /**
    * Busy threads and waiting tasks of the task pools (not the coordinator pools), against
    * `parallelism` threads: queries use the fair pool with `fair_task_scheduling` and the FIFO pool
    * without it, so only one is busy at a time
    */
  override def load: TaskRunner.Load =
    var l = TaskRunner.Load(threads = parallelism, busy = 0, waitingTasks = 0)
    if poolStarted then
      l = l.add(busy = pool.getActiveCount, waitingTasks = pool.getQueue.size)
    if fairStarted then
      val f = fair.load
      l = l.add(busy = f.busy, waitingTasks = f.waitingTasks)
    l

  def shutdown(): Unit =
    shutDown = true
    if poolStarted then
      pool.shutdownNow()
    if fairStarted then
      fair.shutdown()
    if fairCoordinatorsStarted then
      fairCoordinators.shutdown()
    coordinators.shutdownNow()

end ThreadPoolRunner

/**
  * `n` threads running the tasks of a [[FairTaskQueue]], each in its task's group, and accounting
  * each task's wall time to its group (to its coordinator time with `coordinators`). Submitters
  * only append to an inbox and release a permit, so they never wait for the workers' lock on the
  * queue
  */
private final class FairPool(n: Int, threads: ThreadFactory, coordinators: Boolean = false):
  private val inbox   = ConcurrentLinkedQueue[(TaskGroup, FutureTask[?])]()
  private val permits = Semaphore(0) // One per submitted task not taken yet
  private val queue = FairTaskQueue[FutureTask[?]](coordinators = coordinators) // Guarded by itself
  @volatile
  private var shutDown = false

  // Workers blocked waiting for a task; counted only on that slow path, so busy workers pay nothing
  private val idle = AtomicInteger(0)

  private val workers = Seq.fill(n)(threads.newThread(() => work()))
  workers.foreach(_.start())

  def submit[A](g: TaskGroup, task: () => A): FutureTask[A] =
    if shutDown then
      throw RejectedExecutionException("The task runner is shut down")
    val f = FutureTask[A](() => task())
    inbox.add((g, f))
    permits.release()
    f

  /**
    * Account the last task (when not null) and take the next one, whose permit the caller holds:
    * that task is in the inbox or already in the queue
    */
  private def next(
      last: FairTaskQueue.Picked[FutureTask[?]],
      lastNanos: Long
  ): FairTaskQueue.Picked[FutureTask[?]] = queue.synchronized {
    if last != null then
      queue.finished(last, lastNanos)
    val now = System.nanoTime()
    var e   = inbox.poll()
    while e != null do
      queue.add(e._1, e._2, now)
      e = inbox.poll()
    queue.poll(now)
  }

  /** Busy workers and tasks no worker has taken yet */
  def load: TaskRunner.Load = TaskRunner.Load(
    threads = n,
    busy = math.max(0, n - idle.get()),
    waitingTasks = permits.availablePermits()
  )

  private def awaitTask(): Unit =
    idle.incrementAndGet()
    try permits.acquire()
    finally idle.decrementAndGet()

  private def work(): Unit =
    try
      awaitTask()
      var p = next(null, 0L)
      while true do
        // FutureTask keeps the task's exception for Handle.get
        TaskGroup.withGroup(p.group)(p.task.run())
        val nanos = System.nanoTime() - p.startNanos
        // A task's interrupt must not end the worker, as ThreadPoolExecutor clears it too
        if !shutDown then
          Thread.interrupted()
        // One lock round per task while tasks are queued
        if permits.tryAcquire() then
          p = next(p, nanos)
        else
          queue.synchronized(queue.finished(p, nanos))
          awaitTask()
          p = next(null, 0L)
    catch
      case _: InterruptedException =>
        ()

  def shutdown(): Unit =
    shutDown = true
    workers.foreach(_.interrupt())

end FairPool

/**
  * Whether a submitted task has started or was cancelled first: [[ThreadPoolRunner]]'s handles
  * cancel only tasks that haven't started
  */
private final class StartGate extends AtomicInteger(0):
  /** Run `task` unless it was cancelled first */
  def run[A](task: () => A): A =
    if compareAndSet(0, 1) then
      task()
    else
      throw java.util.concurrent.CancellationException("The task was cancelled before it started")

  /** Cancel the task unless it has started; whether it was cancelled */
  def cancel(): Boolean = compareAndSet(0, 2) || get() == 2

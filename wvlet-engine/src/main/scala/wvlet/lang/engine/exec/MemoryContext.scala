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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.vector.*

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
  * The memory budget shared by the queries of one process (e.g. a server): the bytes every open
  * [[MemoryContext]] reserved, against a soft `capacityBytes`. Reservations past the capacity still
  * succeed; [[isOverCapacity]] tells an admission check to hold new queries until running ones
  * release memory. `queryLimitBytes` is the limit of each query of an engine that sets none.
  *
  * With a `killDelay`, a pool that stays over its capacity that long while more than one query is
  * open kills the query holding the most bytes ([[killLargestIfStillOver]], Trino's low-memory
  * killer): it fails with RESOURCE_EXHAUSTED at its next reservation, morsel part, or result batch
  */
final class MemoryPool(
    val capacityBytes: Long = Long.MaxValue,
    val queryLimitBytes: Long = Long.MaxValue,
    val killDelay: Option[FiniteDuration] = None
):
  private val reserved = AtomicLong(0L)
  // System.nanoTime() since reservations went over the capacity; NotOver while under it
  private val overSince = AtomicLong(MemoryPool.NotOver)
  // The open tracking contexts; guarded by `this`
  private val open = mutable.LinkedHashSet.empty[MemoryContext]
  // The killed query until it closes, and the open count, read without the lock on reservations
  @volatile
  private var victim: MemoryContext = null

  @volatile
  private var openCount = 0

  /** Bytes the open queries hold */
  def reservedBytes: Long = reserved.get()

  /** Whether the open queries hold more than the capacity */
  def isOverCapacity: Boolean = reserved.get() > capacityBytes

  /** Queries with an open context */
  def openQueries: Int = openCount

  /** A context for one query, failing it past `limitBytes` */
  def newQuery(limitBytes: Long = queryLimitBytes): MemoryContext =
    val c = MemoryContext(this, limitBytes)
    synchronized {
      open += c
      openCount = open.size
    }
    c

  private[exec] def closed(c: MemoryContext): Unit = synchronized {
    open -= c
    openCount = open.size
    if victim eq c then
      victim = null
      // The delay starts again once the killed query has released its memory
      if isOverCapacity then
        overSince.set(System.nanoTime())
  }

  /** Add (or with a negative value, return) bytes; returns whether the pool is over capacity */
  private[exec] def add(bytes: Long): Boolean =
    val over = reserved.addAndGet(bytes) > capacityBytes
    if over then
      if overSince.get() == MemoryPool.NotOver then
        overSince.compareAndSet(MemoryPool.NotOver, System.nanoTime())
    else if overSince.get() != MemoryPool.NotOver then
      overSince.set(MemoryPool.NotOver)
    over

  /**
    * Kill the open query holding the most bytes when the pool has been over its capacity for at
    * least `killDelay` and more than one query is open: a single query frees nothing for others,
    * and its own limit bounds it. One query at a time: none while a killed one is still open, and
    * the delay starts again when it closes. Returns the killed context, if any. Called on
    * reservations past the capacity and by a server's timer, since a query that holds without
    * growing never reserves
    */
  def killLargestIfStillOver(): Option[MemoryContext] =
    killDelay match
      case Some(delay) if victim == null && openCount > 1 && isOverCapacity =>
        val now = System.nanoTime()
        // A release racing a reservation across the capacity can clear the stamp while the pool
        // is over: start the delay here, so a pool that holds without reserving is still checked
        if overSince.get() == MemoryPool.NotOver then
          overSince.compareAndSet(MemoryPool.NotOver, now)
        val since = overSince.get()
        if since == MemoryPool.NotOver || now - since < delay.toNanos then
          None
        else
          synchronized {
            if open.size <= 1 || victim != null || !isOverCapacity then
              None
            else
              val v = open.maxBy(_.reservedBytes)
              victim = v
              v.kill(
                s"Query was killed to free memory: it held ${MemoryContext.mb(
                    v.reservedBytes
                  )} of the ${MemoryContext.mb(reservedBytes)} that ${open
                    .size} queries held, over the memory pool's capacity of ${MemoryContext.mb(
                    capacityBytes
                  )} for ${delay
                    .toMillis} ms. Retry when fewer queries run, or raise the memory pool size"
              )
              Some(v)
          }
      case _ =>
        None

  override def toString: String =
    s"MemoryPool(reserved=${MemoryContext.mb(reservedBytes)}, capacity=${
        if capacityBytes == Long.MaxValue then
          "unbounded"
        else
          MemoryContext.mb(capacityBytes)
      }${killDelay.map(d => s", kill after ${d.toMillis} ms").getOrElse("")})"

end MemoryPool

object MemoryPool:
  private val NotOver = Long.MinValue

  @volatile
  private var processPool = MemoryPool()

  /**
    * The pool of the engines of this process, unless an engine is given another: unbounded until a
    * server sets one with [[setProcessPool]], since every engine of a process shares its heap
    */
  def process: MemoryPool = processPool

  /** Make `pool` the [[process]] pool of engines created from now on */
  def setProcessPool(pool: MemoryPool): Unit = processPool = pool

/**
  * The bytes one query holds in its large in-memory structures (join build tables, buffered
  * results), reserved from a [[MemoryPool]]. A reservation past `limitBytes` fails the query with
  * RESOURCE_EXHAUSTED. Reservations are kept until [[close]], which returns them all to the pool:
  * the holders keep their rows until the query's operators are dropped. Thread-safe: build steps
  * reserve from worker threads.
  *
  * Operators capture the context of the thread that builds them ([[MemoryContext.current]]), set by
  * `WvletEngine.execute` and re-entered by the planner, so no constructor takes one.
  *
  * A holder that drops its rows before the query ends reserves through a [[child]] and calls
  * [[release]] once it no longer references them: the child's bytes return to the query and the
  * pool then, instead of at [[close]]
  */
final class MemoryContext private[exec] (
    pool: MemoryPool,
    val limitBytes: Long,
    private val parent: MemoryContext = null
):
  private val reserved = AtomicLong(0L)
  // The query's context: limits, kills, and closing are the root's
  private val root: MemoryContext =
    if parent == null then
      this
    else
      parent.root

  // Guarded by the root: releases after the query closed (or after a released ancestor, whose
  // release covered this child's bytes) return nothing
  private var closed = false
  // The most bytes the query held at once (the root's)
  private val peak = AtomicLong(0L)
  // Set by the pool's low-memory killer: the query fails at its next check
  @volatile
  private var killedMessage: String = null

  /** Whether reservations are counted (false for [[MemoryContext.NoOp]]) */
  def isTracking: Boolean = pool != null

  /** Bytes this query holds */
  def reservedBytes: Long = reserved.get()

  /**
    * Reserve `bytes` for a structure about to be allocated or just received. Throws
    * RESOURCE_EXHAUSTED when the query's total exceeds its limit; the bytes stay reserved until
    * [[close]]
    */
  def reserve(bytes: Long): Unit =
    if pool != null then
      root.checkKilled()
      if bytes > 0 then
        if parent == null then
          add(bytes)
        else
          parent.reserve(bytes)
          reserved.addAndGet(bytes)

  /**
    * The most bytes the query held at once, counting releases before it ended; kept after [[close]]
    */
  def peakBytes: Long = root.peak.get()

  private def add(bytes: Long): Unit =
    val total = reserved.addAndGet(bytes)
    var p     = peak.get()
    while total > p && !peak.compareAndSet(p, total) do
      p = peak.get()
    if pool.add(bytes) then
      pool.killLargestIfStillOver()
      checkKilled()
    if total > limitBytes then
      throw StatusCode
        .RESOURCE_EXHAUSTED
        .newException(
          s"Query exceeded its memory limit of ${MemoryContext.mb(
              limitBytes
            )} (reserved ${MemoryContext.mb(total)})"
        )

  /** Whether the pool's low-memory killer chose this query */
  def isKilled: Boolean = root.killedMessage != null

  private[exec] def kill(message: String): Unit = killedMessage = message

  /**
    * Fail the query with RESOURCE_EXHAUSTED if the low-memory killer chose it: called where the
    * engine regains control (reservations, morsel parts, result batches), since operators don't
    * check interrupts
    */
  def checkKilled(): Unit =
    val m = root.killedMessage
    if m != null then
      throw StatusCode.RESOURCE_EXHAUSTED.newException(m)

  /** Reserve the estimated size of a batch ([[MemoryContext.estimatedBytes]]) */
  def reserve(b: Batch): Unit =
    if pool != null then
      reserve(MemoryContext.estimatedBytes(b))

  /** Drain every remaining batch of `op`, compacted, reserving each as it arrives */
  def drain(op: Operator): Seq[Batch] =
    if pool == null then
      op.drain()
    else
      val b = Seq.newBuilder[Batch]
      var x = op.next()
      while x != null do
        val c = x.compact()
        reserve(c)
        b += c
        x = op.next()
      b.result()

  /** The rows of `op` in one batch, as [[Operator.materialize]], reserving each batch */
  def materialize(op: Operator): Batch =
    val batches = drain(op)
    if batches.isEmpty then
      Batch.empty(op.schema)
    else
      Batch.concat(op.schema, batches)

  /**
    * A reservation for a structure that grows by units of `unitBytes` (e.g. groups of a hash
    * aggregation), reserving a quarter of its size (at least 64 KB) ahead
    */
  def growing(unitBytes: Long): MemoryContext.Growing = MemoryContext.Growing(this, unitBytes)

  /**
    * A context for one holder of this query: its reservations count in this context (and the
    * query's limit) until the holder calls [[release]]. [[MemoryContext.NoOp]] for a no-op context
    */
  def child(): MemoryContext =
    if pool == null then
      this
    else
      MemoryContext(pool, limitBytes, this)

  /**
    * Return a child's reservations to the query and the pool, once its holder no longer references
    * the rows they counted. Idempotent; a no-op after the query closed. On the query's own context,
    * the same as [[close]]
    */
  def release(): Unit =
    if pool != null then
      if parent == null then
        close()
      else
        root.synchronized {
          val r       = reserved.getAndSet(0L)
          var covered = root.closed
          var a       = parent
          while !covered && a != null do
            covered = a.closed
            a = a.parent
          closed = true
          if r != 0 && !covered then
            var c = parent
            while c != null do
              c.reserved.addAndGet(-r)
              if c.parent == null then
                pool.add(-r)
              c = c.parent
        }

  /** Return every reservation of the query to the pool. Idempotent; on a child, [[release]] */
  def close(): Unit =
    if pool != null then
      if parent != null then
        release()
      else
        synchronized {
          closed = true
          val r = reserved.getAndSet(0L)
          pool.add(-r)
        }
        pool.closed(this)

  override def toString: String =
    if pool == null then
      "MemoryContext(no-op)"
    else
      s"MemoryContext(reserved=${MemoryContext.mb(reservedBytes)})"

end MemoryContext

object MemoryContext:
  /** A context that counts nothing: operators built outside a query, or `memory_accounting` off */
  val NoOp: MemoryContext = MemoryContext(null, Long.MaxValue)

  private val ambient =
    new ThreadLocal[MemoryContext]:
      override def initialValue(): MemoryContext = NoOp

  /** The context of the query the current thread plans or runs ([[NoOp]] outside one) */
  def current: MemoryContext = ambient.get()

  /** Run `body` with `c` as the current thread's context, restoring the previous one after */
  def withCurrent[A](c: MemoryContext)(body: => A): A =
    val prev = ambient.get()
    if prev eq c then
      body
    else
      ambient.set(c)
      try body
      finally ambient.set(prev)

  /**
    * Whether `e` failed a query over its memory limit: plan-time fallbacks that plan around errors
    * rethrow it instead
    */
  def exceededLimit(e: Throwable): Boolean =
    e match
      case w: WvletLangException =>
        w.statusCode == StatusCode.RESOURCE_EXHAUSTED
      case _ =>
        false

  /**
    * The RESOURCE_EXHAUSTED error of a query that ran the process out of heap (`e`), naming its
    * peak tracked memory and how to give the engine more heap or fail such queries earlier
    */
  def outOfHeap(memory: MemoryContext, e: OutOfMemoryError): WvletLangException =
    val tracked =
      if memory.isTracking then
        s" (peak tracked memory ${mb(memory.peakBytes)})"
      else
        ""
    val detail = Option(e.getMessage).filter(_.nonEmpty).map(m => s" (${m})").getOrElse("")
    StatusCode
      .RESOURCE_EXHAUSTED
      .newException(
        s"Query ran out of heap memory${detail}${tracked}. Give the process more heap " +
          "(-Xmx in JAVA_OPTS on the JVM, GC_MAXIMUM_HEAP_SIZE on Scala Native); a server can " +
          "also fail such queries earlier with --max-query-memory",
        e
      )

  // Bytes a growing structure reserves ahead of its size, at least
  private val MinAheadBytes = 64L << 10

  /**
    * The reservation of one growing structure, used by one thread: [[ensure]] costs a comparison
    * until the structure outgrows what it reserved
    */
  final class Growing private[MemoryContext] (memory: MemoryContext, unitBytes: Long):
    private val minAheadUnits = math.max(1L, MinAheadBytes / math.max(1L, unitBytes))
    private var reservedUnits = 0L

    /** Reserve for `units` units, before they are allocated */
    def ensure(units: Long): Unit =
      if units > reservedUnits && memory.isTracking then
        val next = units + math.max(units / 4, minAheadUnits)
        memory.reserve((next - reservedUnits) * unitBytes)
        reservedUnits = next

  /** Estimated bytes of one value of type `t` in a vector or a group's key or state */
  private[exec] def valueBytes(t: PType): Long = width(t)

  private[exec] def mb(bytes: Long): String = f"${bytes.toDouble / (1024 * 1024)}%.1f MB"

  // Strings sampled per vector to estimate their sizes
  private val StringSamples = 16
  // Bytes of a JVM String around its characters: object header, fields, and array header
  private val StringOverhead = 40

  /**
    * The estimated bytes of a batch's vectors: primitive arrays by width, strings from a sample of
    * their lengths. Deferred columns count their positions and a value width, without gathering
    */
  def estimatedBytes(b: Batch): Long =
    var total = 0L
    var i     = 0
    while i < b.numColumns do
      val src = b.deferredSource(i)
      total += (
        if src != null then
          b.size.toLong * (4 + width(src.ptype))
        else
          estimatedBytes(b.column(i))
      )
      i += 1
    total

  /** The estimated bytes of one vector */
  def estimatedBytes(v: Vec): Long =
    val n     = v.size.toLong
    val nulls =
      if v.nulls != null then
        v.nulls.length.toLong
      else
        0L
    val values =
      v match
        case s: StrVec =>
          if s.holdsEntries then
            // Codes into shared entries
            4L * n
          else if s.hasCodes then
            // Per-row references to the dictionary's strings, and their codes
            12L * n
          else
            n * (8L + sampledStringBytes(s))
        case o: ObjVec =>
          n * 64L
        case _ =>
          n * width(v.ptype)
    values + nulls

  private def width(t: PType): Long =
    t match
      case PType.Bool =>
        1L
      case PType.Int32 | PType.Date =>
        4L
      case PType.Str =>
        48L
      case _ =>
        8L

  private def sampledStringBytes(s: StrVec): Long =
    val n = s.size
    if n == 0 then
      0L
    else
      val step  = math.max(1, n / StringSamples)
      var sum   = 0L
      var count = 0
      var i     = 0
      while i < n && count < StringSamples do
        val x = s.str(i)
        if x != null then
          sum += x.length
        count += 1
        i += step
      StringOverhead + sum / count

end MemoryContext

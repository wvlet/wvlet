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

/**
  * The memory budget shared by the queries of one process (e.g. a server): the bytes every open
  * [[MemoryContext]] reserved, against a soft `capacityBytes`. Reservations past the capacity still
  * succeed; [[isOverCapacity]] tells an admission check to hold new queries until running ones
  * release memory
  */
final class MemoryPool(val capacityBytes: Long = Long.MaxValue):
  private val reserved = AtomicLong(0L)

  /** Bytes the open queries hold */
  def reservedBytes: Long = reserved.get()

  /** Whether the open queries hold more than the capacity */
  def isOverCapacity: Boolean = reserved.get() > capacityBytes

  /** A context for one query, failing it past `limitBytes` */
  def newQuery(limitBytes: Long = Long.MaxValue): MemoryContext = MemoryContext(this, limitBytes)

  private[exec] def add(bytes: Long): Unit = reserved.addAndGet(bytes)

  override def toString: String =
    s"MemoryPool(reserved=${MemoryContext.mb(reservedBytes)}, capacity=${
        if capacityBytes == Long.MaxValue then
          "unbounded"
        else
          MemoryContext.mb(capacityBytes)
      })"

/**
  * The bytes one query holds in its large in-memory structures (join build tables, buffered
  * results), reserved from a [[MemoryPool]]. A reservation past `limitBytes` fails the query with
  * RESOURCE_EXHAUSTED. Reservations are kept until [[close]], which returns them all to the pool:
  * the holders keep their rows until the query's operators are dropped. Thread-safe: build steps
  * reserve from worker threads.
  *
  * Operators capture the context of the thread that builds them ([[MemoryContext.current]]), set by
  * `WvletEngine.execute` and re-entered by the planner, so no constructor takes one
  */
final class MemoryContext private[exec] (pool: MemoryPool, val limitBytes: Long):
  private val reserved = AtomicLong(0L)

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
    if pool != null && bytes > 0 then
      val total = reserved.addAndGet(bytes)
      pool.add(bytes)
      if total > limitBytes then
        throw StatusCode
          .RESOURCE_EXHAUSTED
          .newException(
            s"Query exceeded its memory limit of ${MemoryContext.mb(
                limitBytes
              )} (reserved ${MemoryContext.mb(total)})"
          )

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

  /** Return every reservation to the pool. Idempotent */
  def close(): Unit =
    if pool != null then
      val r = reserved.getAndSet(0L)
      pool.add(-r)

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

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
  * The estimated bytes that scan parts in flight may hold at once, across every [[MorselOp]] of the
  * process (`bound_scan_decode`). A part decoding a Parquet row group holds its compressed chunks,
  * decompressed pages, and decoded vectors, and its output stays until the consumer takes it; with
  * a part per thread and twice the parallelism in flight, a small heap ran out of memory in the
  * scans alone.
  *
  * A [[MorselOp]] acquires a part's bytes before submitting it, and the part's task releases them
  * when it finishes. Acquiring never blocks: past the budget, the operator stops submitting until
  * one of its own parts is taken, and an operator with no part in flight always gets one, so every
  * scan makes progress. The bytes count toward no query's limit or spill threshold.
  */
final class ScanDecodeBudget(val capacityBytes: Long):
  private val inFlight = AtomicLong(0L)
  private val peak     = AtomicLong(0L)

  /** The estimated bytes of the parts in flight */
  def inFlightBytes: Long = inFlight.get()

  /** The most bytes in flight at once */
  def peakBytes: Long = peak.get()

  /**
    * Acquire `bytes` for a part: false, acquiring nothing, when they would pass the budget, unless
    * `force` (the operator has no part in flight)
    */
  def tryAcquire(bytes: Long, force: Boolean): Boolean =
    var done = false
    var ok   = false
    while !done do
      val cur = inFlight.get()
      if !force && cur > 0 && cur + bytes > capacityBytes then
        done = true
      else if inFlight.compareAndSet(cur, cur + bytes) then
        val now = cur + bytes
        var p   = peak.get()
        while now > p && !peak.compareAndSet(p, now) do
          p = peak.get()
        done = true
        ok = true
    ok

  /** Return the bytes of a part whose batches were taken (or dropped) */
  def release(bytes: Long): Unit =
    if bytes > 0 then
      inFlight.addAndGet(-bytes)

  /** Forget the peak (for tests and benches) */
  def resetPeak(): Unit = peak.set(inFlight.get())

  override def toString: String =
    s"ScanDecodeBudget(${inFlightBytes} of ${capacityBytes} bytes in flight, peak ${peakBytes})"

end ScanDecodeBudget

object ScanDecodeBudget:
  /** The process's budget, [[SpillCompat.defaultScanDecodeBudgetBytes]] */
  lazy val process: ScanDecodeBudget = ScanDecodeBudget(SpillCompat.defaultScanDecodeBudgetBytes)

  /**
    * The estimated bytes of decoding `columns` leaf column chunks of a Parquet row group of `rows`
    * rows, of `compressed` and `uncompressed` bytes: the compressed chunks, the decompressed pages,
    * and the decoded vectors, 8 bytes per value (dictionary pages decode to much more than their
    * bytes) plus the uncompressed bytes again for string data
    */
  def rowGroupBytes(compressed: Long, uncompressed: Long, rows: Long, columns: Int): Long =
    compressed + 2 * uncompressed + 8L * rows * columns

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
  * The default spill threshold with headroom (`spill_headroom`): the threshold counts only tracked
  * holders, while scans and per-thread working sets take memory that grows with the worker threads,
  * not with the heap. Half the heap minus a per-thread allowance leaves room for that on small
  * heaps and barely moves large ones; at least an eighth of the heap, so a small heap with many
  * threads still holds some rows before spilling.
  */
object SpillThreshold:
  /**
    * The untracked bytes allowed per worker thread by default. 16 MB rather than a full row group's
    * decode bytes: on the calibrated accounting (#2598), 32 MB made TPC-DS q67 spill 1 GB at a 2 GB
    * heap and run 2.9x slower, where 16 MB keeps it in memory
    */
  val defaultThreadAllowanceBytes: Long = 16L << 20

  /**
    * max(maxHeap / 8, maxHeap / 2 − threads × threadAllowance) bytes; Long.MaxValue for an
    * unbounded heap
    */
  def withHeadroom(maxHeap: Long, threads: Int, threadAllowance: Long): Long =
    if maxHeap == Long.MaxValue then
      Long.MaxValue
    else
      val allowance = math.max(1, threads).toLong * math.max(0L, threadAllowance)
      math.max(maxHeap / 8, maxHeap / 2 - allowance)

end SpillThreshold

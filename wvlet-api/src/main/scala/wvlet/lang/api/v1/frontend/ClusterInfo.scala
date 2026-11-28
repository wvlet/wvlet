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
package wvlet.lang.api.v1.frontend

import java.time.Instant

/**
  * Query counts of the server against its admission limits.
  *
  * @param finished
  *   Finished queries the server still keeps (within its retention)
  */
case class QueryCounts(running: Int, queued: Int, finished: Int, maxRunning: Int, maxQueued: Int)

/**
  * The memory pool shared by the in-process engine's queries.
  *
  * @param capacityBytes
  *   The soft capacity past which queued queries wait; None when unbounded
  * @param queryLimitBytes
  *   The memory one query may hold before it fails; None without a limit
  * @param openQueries
  *   Engine statements holding a memory context now
  * @param killedQueries
  *   Queries the low-memory killer has failed since the server started
  * @param lowMemoryKillerDelayMillis
  *   How long the pool may stay over capacity before the killer acts; None when it is off
  */
case class MemoryPoolInfo(
    reservedBytes: Long,
    capacityBytes: Option[Long],
    queryLimitBytes: Option[Long],
    openQueries: Int,
    killedQueries: Long,
    lowMemoryKillerDelayMillis: Option[Long]
)

/**
  * Load of a thread pool.
  *
  * @param busy
  *   Threads running a task now
  * @param waitingTasks
  *   Submitted tasks no thread has started yet
  */
case class ThreadPoolLoad(threads: Int, busy: Int, waitingTasks: Int)

/** The JVM heap (or the platform's equivalent) */
case class HeapInfo(usedBytes: Long, committedBytes: Long, maxBytes: Long)

/**
  * What the server is doing now: its query counts, memory, and thread load.
  *
  * @param queryWorkers
  *   The query service's workers: one thread per running query
  * @param engineThreads
  *   The in-process engine's task threads, shared by every engine query
  */
case class ClusterInfo(
    version: String,
    serverId: String,
    startedAt: Instant,
    upTimeMillis: Long,
    processors: Int,
    queries: QueryCounts,
    memoryPool: MemoryPoolInfo,
    queryWorkers: ThreadPoolLoad,
    engineThreads: ThreadPoolLoad,
    heap: HeapInfo
)

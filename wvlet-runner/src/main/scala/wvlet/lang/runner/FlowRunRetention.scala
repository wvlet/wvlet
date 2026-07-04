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

import wvlet.lang.runner.connector.DBConnector
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * Retention sweep over recorded flow runs, used by the scheduler daemon (and `--once` runs) to
  * keep the run store and the run-scoped `__wv_flow_*` tables from growing without bound.
  *
  * A sweep performs two steps:
  *
  *   1. *Finalize* stale running records: a record still in the `running` state whose liveness
  *      lease has expired belongs to a dead process and is marked as failed, so it becomes subject
  *      to retention like any other terminal run.
  *   1. *Delete* terminal records beyond the retention policy — a per-flow `keep_runs: N` cap (the
  *      N most recent terminal runs are kept) and/or an age TTL — dropping their run-scoped tables.
  *      The most recent terminal run of each flow is always kept, because cross-flow dependencies
  *      (`depends on X`, `if X.failed`) read it through `latestRunOf`.
  */
object FlowRunRetention extends LogSupport:

  /** The outcome of a retention sweep */
  case class SweepSummary(finalizedStale: Int, deletedRuns: Int)

  /**
    * Run one retention sweep.
    *
    * @param keepRunsOf
    *   Per-flow `keep_runs:` cap (None keeps an unlimited number of runs)
    * @param ttlMillis
    *   Age limit of terminal records, measured against their finish time (falling back to the start
    *   time)
    */
  def sweep(
      store: FlowRunStore,
      connector: DBConnector,
      keepRunsOf: String => Option[Int] = _ => None,
      ttlMillis: Option[Long] = None,
      nowMillis: Long = System.currentTimeMillis()
  ): SweepSummary =
    val all = store.list()

    val finalized =
      all
        .filter(_.isStaleAt(nowMillis))
        .map { r =>
          warn(s"Finalizing stale run ${r.runId} of flow ${r.flowName} as failed (lease expired)")
          val failed = r.copy(
            state = FlowRunRecord.STATE_FAILED,
            finishedAtMillis = Some(nowMillis),
            leaseExpiresAtMillis = None
          )
          store.save(failed)
          r.runId -> failed
        }
        .toMap

    val terminal = all.map(r => finalized.getOrElse(r.runId, r)).filter(_.isTerminal)
    var deleted  = 0
    terminal
      .groupBy(_.flowName)
      .foreach { (flowName, runs) =>
        val newestFirst = runs.sortBy(_.startedAtMillis)(using Ordering[Long].reverse)
        val keep        = keepRunsOf(flowName)
        // Rank 1 (the latest terminal run) is never deleted
        newestFirst
          .zipWithIndex
          .drop(1)
          .foreach { (r, idx) =>
            val rank    = idx + 1
            val overCap = keep.exists(rank > _)
            val expired = ttlMillis.exists(ttl =>
              r.finishedAtMillis.getOrElse(r.startedAtMillis) < nowMillis - ttl
            )
            if overCap || expired then
              try
                FlowExecutor.dropRunTables(connector, r.runId, r.stages.map(_.name))
                store.delete(r.runId)
                deleted += 1
              catch
                case NonFatal(e) =>
                  warn(s"Failed to remove run ${r.runId} of flow ${flowName}: ${e.getMessage}")
          }
      }

    if finalized.nonEmpty || deleted > 0 then
      info(
        s"Retention sweep: finalized ${finalized.size} stale run(s), deleted ${deleted} old run(s)"
      )
    SweepSummary(finalized.size, deleted)

  end sweep

end FlowRunRetention

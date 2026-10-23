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
package wvlet.lang.runner.activity

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
  * Executes an activity by dispatching it to remote workers through a [[TaskQueue]] and awaiting
  * the outcome. Liveness propagates to the caller: a task waiting for a worker counts as alive, and
  * each worker heartbeat is forwarded. A lost worker yields [[ActivityResult.Uncertain]] once its
  * lease expires.
  *
  * Attempts outlive the orchestrator process (they are durable in the queue's store), so an attempt
  * recovered by the queue after a restart can be resumed ([[canResume]]).
  *
  * @param activityType
  *   The activity type this executor dispatches
  * @param queue
  *   The task queue workers poll
  * @param pollIntervalMillis
  *   How often a waiting caller reports liveness while the task waits for a worker
  */
class TaskQueueActivityExecutor(
    activityType: String,
    queue: TaskQueue,
    pollIntervalMillis: Long = 1000L
) extends ActivityExecutor:

  override def name: String = activityType

  override def execute(request: ActivityRequest, heartbeat: () => Unit): ActivityResult =
    val task = PendingTask(
      activityType = request.activityType,
      invocationId = request.invocationId,
      attemptId = request.attemptId,
      input = request.input,
      config = request.config,
      enqueuedAtMillis = System.currentTimeMillis()
    )
    val future   = queue.dispatch(task, heartbeat)
    val interval = request.config.heartbeatMillis.map(h => (h / 2).max(1L)).getOrElse(
      pollIntervalMillis
    ).min(pollIntervalMillis)
    var result: Option[ActivityResult] = None
    while result.isEmpty do
      try
        result = Some(future.get(interval, TimeUnit.MILLISECONDS))
      catch
        case _: TimeoutException =>
          // Waiting in the queue is not a liveness failure: the queue holds the task
          if queue.isPending(request.attemptId) then
            heartbeat()
    result.get

  override def cancel(attemptId: String): CancelAck =
    if queue.requestCancel(attemptId) then
      CancelAck.Acknowledged
    else
      // The worker learns about the cancellation on its next heartbeat; its report (or the lease
      // expiry) decides the final state
      CancelAck.Unknown

  override def canResume(attemptId: String): Boolean = queue.isKnown(attemptId)

end TaskQueueActivityExecutor

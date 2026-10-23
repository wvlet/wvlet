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
package wvlet.lang.server

import wvlet.lang.api.v1.task.TaskApi
import wvlet.lang.api.v1.task.TaskApi.*
import wvlet.lang.runner.activity.TaskQueue
import wvlet.uni.http.rpc.RPCStatus
import wvlet.uni.log.LogSupport

/**
  * Server-side implementation of the task-queue worker protocol. Translates between the HTTP wire
  * types ([[TaskApi]]) and the internal [[TaskQueue]] state machine.
  */
class TaskApiImpl(taskQueue: TaskQueue) extends TaskApi with LogSupport:

  override def poll(request: PollRequest): PollResponse =
    if request.activityTypes.isEmpty then
      throw RPCStatus.INVALID_REQUEST_U1.newException("activityTypes must not be empty")

    val timeoutMillis = request.timeoutMillis.getOrElse(0L).max(0L)
    taskQueue.poll(request.activityTypes, request.workerId, timeoutMillis) match
      case Some(task) =>
        val stringInput = task
          .input
          .map { (k, v) =>
            k -> v.toString
          }
        PollResponse(
          hasTask = true,
          activityType = task.activityType,
          invocationId = task.invocationId,
          attemptId = task.attemptId,
          input = stringInput,
          timeoutMillis = task.config.timeoutMillis,
          heartbeatMillis = task.config.heartbeatMillis
        )
      case None =>
        PollResponse.empty()

  override def reportResult(request: TaskResultRequest): TaskResultResponse =
    if request.attemptId.isEmpty then
      throw RPCStatus.INVALID_REQUEST_U1.newException("attemptId must not be empty")

    val validStatuses = Set("completed", "failed", "cancelled")
    if !validStatuses.contains(request.status) then
      throw RPCStatus
        .INVALID_REQUEST_U1
        .newException(
          s"Invalid status '${request.status}'; must be one of: ${validStatuses.mkString(", ")}"
        )

    val output =
      if request.output.nonEmpty then
        Some(serializeOutput(request.output))
      else
        None

    val accepted = taskQueue.reportResult(request.attemptId, request.status, output, request.error)

    if accepted then
      TaskResultResponse(accepted = true, message = "Result accepted")
    else
      TaskResultResponse(accepted = false, message = "Attempt not found or fenced (stale)")

  override def heartbeat(request: HeartbeatRequest): HeartbeatResponse =
    if request.attemptId.isEmpty then
      throw RPCStatus.INVALID_REQUEST_U1.newException("attemptId must not be empty")

    taskQueue.heartbeat(request.attemptId) match
      case "continue" =>
        HeartbeatResponse.Continue
      case "cancel" =>
        HeartbeatResponse.Cancel
      case "unknown" =>
        throw RPCStatus.NOT_FOUND_U5.newException(s"Attempt '${request.attemptId}' is not active")

  private def serializeOutput(output: Map[String, String]): String =
    def escapeJson(s: String): String = s
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
    val entries = output.map { (k, v) =>
      s""""${escapeJson(k)}":"${escapeJson(v)}""""
    }
    s"{${entries.mkString(",")}}"

end TaskApiImpl

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
import wvlet.lang.runner.activity.ActivityJson
import wvlet.lang.runner.activity.ItemRecord
import wvlet.lang.runner.activity.ItemState
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
        Some(ActivityJson.encode(request.output))
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

  override def listItems(request: ItemListRequest): ItemListResponse =
    if request.runId.isEmpty || request.stageName.isEmpty then
      throw RPCStatus.INVALID_REQUEST_U1.newException("runId and stageName must not be empty")
    if request.offset < 0 || request.limit <= 0 || request.limit > TaskApi.MaxPageSize then
      throw RPCStatus
        .INVALID_REQUEST_U1
        .newException(
          s"offset must be >= 0 and limit must be within 1..${TaskApi.MaxPageSize}"
        )
    request
      .state
      .foreach { s =>
        if ItemState.fromString(s).isEmpty then
          throw RPCStatus.INVALID_REQUEST_U1.newException(s"Unknown item state: ${s}")
      }
    val store = taskQueue.store
    val items = store.listItemsPage(
      request.runId,
      request.stageName,
      request.state,
      request.offset,
      request.limit
    )
    ItemListResponse(
      items = items.map(toItemInfo),
      counts = store.countItemsByState(request.runId, request.stageName),
      nextOffset =
        if items.size == request.limit then
          Some(request.offset + items.size)
        else
          None
    )

  override def getItem(request: ItemRequest): ItemInfo = taskQueue
    .store
    .getItem(request.itemId)
    .map(toItemInfo)
    .getOrElse(throw RPCStatus.NOT_FOUND_U5.newException(s"Item '${request.itemId}' is not found"))

  override def listActiveAttempts(request: ActiveAttemptListRequest): List[AttemptInfo] =
    if request.limit <= 0 || request.limit > TaskApi.MaxPageSize then
      throw RPCStatus
        .INVALID_REQUEST_U1
        .newException(s"limit must be within 1..${TaskApi.MaxPageSize}")
    taskQueue
      .activeAttempts
      .sortBy(_.leaseExpiresAtMillis)
      .take(request.limit)
      .map(a => AttemptInfo(a.attemptId, a.itemId, a.workerId, a.leaseExpiresAtMillis, a.cancelRequested))

  private def toItemInfo(r: ItemRecord): ItemInfo = ItemInfo(
    itemId = r.itemId,
    runId = r.runId,
    stageName = r.stageName,
    itemKey = r.itemKey,
    state = r.state,
    attemptId = r.attemptId,
    attempts = r.attempts,
    activityType = r.activityType,
    workerId = r.workerId,
    output = r.output,
    error = r.error,
    startedAtMillis = r.startedAtMillis,
    finishedAtMillis = r.finishedAtMillis,
    leaseExpiresAtMillis = r.leaseExpiresAtMillis
  )

end TaskApiImpl

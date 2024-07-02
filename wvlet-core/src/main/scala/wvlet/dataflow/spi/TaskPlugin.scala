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
package wvlet.dataflow.spi

import wvlet.airframe.rx.Cancelable
import wvlet.airframe.ulid.ULID
import wvlet.dataflow.api.v1.TaskApi.{Tags, TaskBody, TaskId}
import wvlet.dataflow.api.v1.TaskRequest
import wvlet.log.LogSupport

case class TaskInput(
    taskId: TaskId,
    // Task plugin name
    taskPlugin: String,
    // A method name to execute in the plugin
    methodName: String,
    // taskBody is a serializable key:String -> value:Any pairs
    taskBody: TaskBody,
    // Tags attached to the task
    tags: Tags = Tags.empty,
    // Idempotent key for preventing duplicated task creation
    idempotentKey: ULID = ULID.newULID
)

object TaskInput:
  def apply(taskId: TaskId, request: TaskRequest): TaskInput =
    TaskInput(
      taskId,
      request.taskPlugin,
      request.methodName,
      request.taskBody,
      request.tags,
      request.idempotentKey
    )

trait TaskPlugin extends LogSupport:
  def pluginName: String
  def run(input: TaskInput): Cancelable

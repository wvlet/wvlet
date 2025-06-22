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
package wvlet.lang.api

enum StatusType:
  // Success status
  case Success
  // User or query errors, which is not retryable
  case UserError
  // Internal errors, which is usually retryable
  case InternalError
  // Insufficient resources to complete the task. Users can retry after the resource is available
  case ResourceExhausted

/**
  * The standard error code for throwing exceptions
  * @param statusType
  */
enum StatusCode(statusType: StatusType):

  case OK extends StatusCode(StatusType.Success)
  // Used for successful exit by throwing an Exception
  case EXIT_SUCCESSFULLY extends StatusCode(StatusType.Success)

  // User errors
  case SYNTAX_ERROR               extends StatusCode(StatusType.UserError)
  case UNEXPECTED_TOKEN           extends StatusCode(StatusType.UserError)
  case UNCLOSED_MULTILINE_LITERAL extends StatusCode(StatusType.UserError)

  case INVALID_ARGUMENT      extends StatusCode(StatusType.UserError)
  case SCHEMA_NOT_FOUND      extends StatusCode(StatusType.UserError)
  case TABLE_NOT_FOUND       extends StatusCode(StatusType.UserError)
  case CATALOG_NOT_FOUND     extends StatusCode(StatusType.UserError)
  case COLUMN_NOT_FOUND      extends StatusCode(StatusType.UserError)
  case NOT_A_RELATION        extends StatusCode(StatusType.UserError)
  case FILE_NOT_FOUND        extends StatusCode(StatusType.UserError)
  case SCHEMA_ALREADY_EXISTS extends StatusCode(StatusType.UserError)
  case TABLE_ALREADY_EXISTS  extends StatusCode(StatusType.UserError)
  case UNAUTHENTICATED       extends StatusCode(StatusType.UserError)
  case PERMISSION_DENIED     extends StatusCode(StatusType.UserError)

  case NOT_IMPLEMENTED              extends StatusCode(StatusType.UserError)
  case NON_RETRYABLE_INTERNAL_ERROR extends StatusCode(StatusType.UserError)
  case COMPILATION_FAILURE          extends StatusCode(StatusType.UserError)
  case UNEXPECTED_STATE             extends StatusCode(StatusType.UserError)
  case QUERY_EXECUTION_FAILURE      extends StatusCode(StatusType.UserError)

  case TEST_FAILED extends StatusCode(StatusType.UserError)

  case INTERNAL_ERROR      extends StatusCode(StatusType.InternalError)
  case SERIALIZATION_ERROR extends StatusCode(StatusType.InternalError)

  case RESOURCE_EXHAUSTED extends StatusCode(StatusType.ResourceExhausted)

  def isUserError: Boolean     = statusType == StatusType.UserError
  def isInternalError: Boolean = statusType == StatusType.InternalError
  def isSuccess: Boolean       = statusType == StatusType.Success

  def name: String                                  = this.toString
  def newException(msg: String): WvletLangException = WvletLangException(this, msg)
  def newException(msg: String, cause: Throwable): WvletLangException = WvletLangException(
    this,
    msg,
    SourceLocation.NoSourceLocation,
    cause
  )

  def newException(msg: String, sourceLocation: SourceLocation): WvletLangException =
    val baseMsg   = s"[${this.name}] ${msg}"
    val locString = s"${sourceLocation.locationString}"
    val column    = sourceLocation.position.map(_.column).getOrElse(1)
    val line      = sourceLocation.codeLineAt
    val err =
      if line.isEmpty then
        s"${baseMsg} (${locString})"
      else
        var pos = 0
        for i <- 0 until column.min(line.length) do
          if line(i) == '\t' then
            pos += 4
          else
            pos += 1
        s"${baseMsg}\n${line} (${locString})\n${" " * (pos - 1)}^\n"

    WvletLangException(this, err, sourceLocation)

end StatusCode

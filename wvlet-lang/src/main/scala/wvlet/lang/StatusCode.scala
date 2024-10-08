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
package wvlet.lang

import wvlet.lang.compiler.parser.Span
import wvlet.lang.compiler.{Context, SourceLocation}
import wvlet.lang.model.NodeLocation

enum StatusType:
  case Success
  case UserError
  case SystemError
  case ExternalError

enum StatusCode(statusType: StatusType):
  def isUserError: Boolean     = statusType == StatusType.UserError
  def isSystemError: Boolean   = statusType == StatusType.SystemError
  def isExternalError: Boolean = statusType == StatusType.ExternalError
  def isSuccess: Boolean       = statusType == StatusType.Success

  def name: String                                  = this.toString
  def newException(msg: String): WvletLangException = WvletLangException(this, msg)
  def newException(msg: String, cause: Throwable): WvletLangException = WvletLangException(
    this,
    msg,
    None,
    cause
  )

  def newException(msg: String, sourceLocation: SourceLocation): WvletLangException =
    val baseMsg   = s"[${this.name}] ${msg}"
    val locString = s"${sourceLocation.locationString}"
    val column    = sourceLocation.nodeLocation.map(_.column).getOrElse(1)
    val line      = sourceLocation.codeLineAt
    val err =
      if line.isEmpty then
        s"${baseMsg} (${locString})"
      else
        s"${baseMsg}\n${line} (${locString})\n${" " * (column - 1)}^\n"

    WvletLangException(this, err, Some(sourceLocation))

  def newException(msg: String, span: Span)(using ctx: Context): WvletLangException =
    if span.isEmpty then
      newException(msg)
    else
      newException(msg, span.sourceLocation)

  case OK extends StatusCode(StatusType.Success)

  case SYNTAX_ERROR     extends StatusCode(StatusType.UserError)
  case UNEXPECTED_TOKEN extends StatusCode(StatusType.UserError)

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

  case TEST_FAILED extends StatusCode(StatusType.UserError)

end StatusCode

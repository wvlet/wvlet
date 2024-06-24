package com.treasuredata.flow.lang

import com.treasuredata.flow.lang.compiler.SourceLocation

enum StatusType:
  case Success
  case UserError
  case SystemError
  case ExternalError

enum StatusCode(statusType: StatusType):
  def newException(msg: String): FlowLangException = FlowLangException(this, msg)
  def newException(msg: String, cause: Throwable): FlowLangException = FlowLangException(
    this,
    msg,
    None,
    cause
  )

  def newException(msg: String, sourceLocation: SourceLocation): FlowLangException =
    val err = s"${msg}:${sourceLocation.locationString}"
    FlowLangException(this, err, Some(sourceLocation))

  case OK                      extends StatusCode(StatusType.Success)
  case SYNTAX_ERROR            extends StatusCode(StatusType.UserError)
  case INVALID_ARGUMENT        extends StatusCode(StatusType.UserError)
  case DATABASE_NOT_FOUND      extends StatusCode(StatusType.UserError)
  case TABLE_NOT_FOUND         extends StatusCode(StatusType.UserError)
  case COLUMN_NOT_FOUND        extends StatusCode(StatusType.UserError)
  case NOT_A_RELATION          extends StatusCode(StatusType.UserError)
  case FILE_NOT_FOUND          extends StatusCode(StatusType.UserError)
  case DATABASE_ALREADY_EXISTS extends StatusCode(StatusType.UserError)
  case TABLE_ALREADY_EXISTS    extends StatusCode(StatusType.UserError)
  case UNAUTHENTICATED         extends StatusCode(StatusType.UserError)
  case PERMISSION_DENIED       extends StatusCode(StatusType.UserError)

  case NOT_IMPLEMENTED              extends StatusCode(StatusType.UserError)
  case NON_RETRYABLE_INTERNAL_ERROR extends StatusCode(StatusType.UserError)

package com.treasuredata.flow.lang

enum StatusType:
  case Success
  case UserError
  case SystemError
  case ExternalError

enum StatusCode(statusType: StatusType):
  def newException(msg: String): FlowLangException                   = FlowLangException(this, msg)
  def newException(msg: String, cause: Throwable): FlowLangException = FlowLangException(this, msg, cause)

  case OK               extends StatusCode(StatusType.Success)
  case SYNTAX_ERROR     extends StatusCode(StatusType.UserError)
  case COLUMN_NOT_FOUND extends StatusCode(StatusType.UserError)

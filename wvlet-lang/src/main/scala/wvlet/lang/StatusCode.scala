package wvlet.lang

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
    val err = s"[${this.name}] ${msg} (${sourceLocation.locationString})"
    WvletLangException(this, err, Some(sourceLocation))

  def newException(msg: String, nodeLocation: Option[NodeLocation])(using
      ctx: Context
  ): WvletLangException =
    nodeLocation match
      case Some(nodeLoc) =>
        val loc  = nodeLoc.toSourceLocation
        val line = loc.codeLineAt
        val err =
          if line.isEmpty then
            msg
          else
            s"${msg}\n[code]\n${line}\n${" " * (nodeLoc.column - 1)}^"
        newException(err, loc)
      case _ =>
        newException(msg)

  case OK               extends StatusCode(StatusType.Success)
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
  case UNEXPECTED_STATE             extends StatusCode(StatusType.UserError)

end StatusCode

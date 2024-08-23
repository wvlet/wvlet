package wvlet.lang

import wvlet.lang.compiler.SourceLocation

class WvletLangException(
    val statusCode: StatusCode,
    message: String,
    sourceLocation: Option[SourceLocation] = None,
    cause: Throwable = null
) extends Exception(message, cause)

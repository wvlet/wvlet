package com.treasuredata.flow.lang

class FlowLangException(
    val statusCode: StatusCode,
    message: String,
    sourceLocation: Option[SourceLocation] = None,
    cause: Throwable = null
) extends Exception(message, cause) {}

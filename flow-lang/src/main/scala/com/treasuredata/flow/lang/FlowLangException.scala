package com.treasuredata.flow.lang

import com.treasuredata.flow.lang.compiler.SourceLocation

class FlowLangException(
    val statusCode: StatusCode,
    message: String,
    sourceLocation: Option[SourceLocation] = None,
    cause: Throwable = null
) extends Exception(message, cause)

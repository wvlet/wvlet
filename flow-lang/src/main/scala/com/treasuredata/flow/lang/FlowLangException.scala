package com.treasuredata.flow.lang

class FlowLangException(val statusCode: StatusCode, message: String, cause: Throwable = null)
    extends Exception(message, cause) {}

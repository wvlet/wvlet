package wvlet.lang.native

import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogSupport
import wvlet.lang.api.{WvletLangException, SourceLocation, LinePosition}

import scala.scalanative.unsafe.*
import scala.scalanative.libc.stdlib

object WvcLib extends LogSupport:

  // Response types for JSON serialization
  case class CompileResponse(
      success: Boolean,
      sql: Option[String] = None,
      error: Option[CompileError] = None
  )

  case class CompileError(
      code: String,
      statusType: String,
      message: String,
      location: Option[ErrorLocation] = None
  )

  case class ErrorLocation(
      path: String,
      fileName: String,
      line: Int,
      column: Int,
      lineContent: Option[String] = None
  )

  /**
    * Run WvcMain with the given arguments
    * @param argJson
    *   json string representing command line arguments ["arg1", "arg2", ...]
    * @return
    */
  @exported("wvlet_compile_main")
  def compile_main(argJson: CString): Int =
    try
      val json = fromCString(argJson)
      val args = MessageCodec.of[Array[String]].fromJson(json)
      WvcMain.main(args)
      0
    catch
      case e: Throwable =>
        warn(e)
        1

  /**
    * Compile a Wvlet query and return the SQL as a string
    * @param argJson
    *   json string representing command line arguments ["arg1", "arg2", ...]
    * @return
    *   generated SQL as a CString (allocated on heap, managed by GC)
    */
  @exported("wvlet_compile_query")
  def compile_query(argJson: CString): CString =
    try
      val json     = fromCString(argJson)
      val args     = MessageCodec.of[Array[String]].fromJson(json)
      val (sql, _) = WvcMain.compileWvletQuery(args)

      // Allocate string on heap using malloc (managed by Boehm GC)
      val len    = sql.length + 1
      val buffer = stdlib.malloc(len).asInstanceOf[CString]
      var i      = 0
      while i < sql.length do
        buffer(i) = sql.charAt(i).toByte
        i += 1
      buffer(sql.length) = 0.toByte
      buffer
    catch
      case e: Throwable =>
        warn(e)
        // Return empty string on error
        val buffer = stdlib.malloc(1).asInstanceOf[CString]
        buffer(0) = 0.toByte
        buffer

  /**
    * Compile a Wvlet query and return the result as JSON
    * @param argJson
    *   json string representing command line arguments ["arg1", "arg2", ...]
    * @return
    *   JSON string with compilation result: Success: {"success": true, "sql": "..."} Error:
    *   {"success": false, "error": {"code": "...", "statusType": "...", "message": "...",
    *   "location": {...}}} Status types: Success, UserError, InternalError, ResourceExhausted
    */
  @exported("wvlet_compile_query_json")
  def compile_query_json(argJson: CString): CString =
    try
      val json = fromCString(argJson)
      val args = MessageCodec.of[Array[String]].fromJson(json)

      val response =
        try
          val (sql, _) = WvcMain.compileWvletQuery(args)
          CompileResponse(success = true, sql = Some(sql))
        catch
          case e: WvletLangException =>
            // Determine status type based on StatusCode methods
            val statusTypeStr =
              if e.statusCode.isUserError then
                "UserError"
              else if e.statusCode.isInternalError then
                "InternalError"
              else if e.statusCode.isSuccess then
                "Success"
              else
                "ResourceExhausted"

            val locationOpt =
              if e.sourceLocation != SourceLocation.NoSourceLocation then
                Some(
                  ErrorLocation(
                    path = e.sourceLocation.path,
                    fileName = e.sourceLocation.fileName,
                    line = e.sourceLocation.position.line,
                    column = e.sourceLocation.position.column,
                    lineContent =
                      if e.sourceLocation.codeLineAt.nonEmpty then
                        Some(e.sourceLocation.codeLineAt)
                      else
                        None
                  )
                )
              else
                None

            val error = CompileError(
              code = e.statusCode.name,
              statusType = statusTypeStr,
              message = e.getMessage,
              location = locationOpt
            )
            CompileResponse(success = false, error = Some(error))
          case e: Throwable =>
            val error = CompileError(
              code = "INTERNAL_ERROR",
              statusType = "InternalError",
              message = Option(e.getMessage).getOrElse(e.getClass.getName)
            )
            CompileResponse(success = false, error = Some(error))

      // Convert response to JSON
      val responseJson = MessageCodec.of[CompileResponse].toJson(response)

      // Allocate string on heap using malloc (managed by Boehm GC)
      val len    = responseJson.length + 1
      val buffer = stdlib.malloc(len).asInstanceOf[CString]
      var i      = 0
      while i < responseJson.length do
        buffer(i) = responseJson.charAt(i).toByte
        i += 1
      buffer(responseJson.length) = 0.toByte
      buffer
    catch
      case e: Throwable =>
        warn(e)
        // Return error response as JSON even if JSON serialization fails
        val errorJson =
          s"""{"success":false,"error":{"code":"JSON_ERROR","statusType":"InternalError","message":"${e
              .getMessage}"}}"""
        val len    = errorJson.length + 1
        val buffer = stdlib.malloc(len).asInstanceOf[CString]
        var i      = 0
        while i < errorJson.length do
          buffer(i) = errorJson.charAt(i).toByte
          i += 1
        buffer(errorJson.length) = 0.toByte
        buffer

end WvcLib

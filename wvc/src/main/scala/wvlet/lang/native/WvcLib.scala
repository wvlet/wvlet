package wvlet.lang.native

import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogSupport
import wvlet.lang.api.WvletLangException
import wvlet.lang.api.SourceLocation
import wvlet.lang.api.LinePosition
import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.compile.CompileResponse
import wvlet.lang.api.v1.compile.CompileError
import wvlet.lang.api.v1.compile.ErrorLocation

import scala.scalanative.unsafe.*
import scala.scalanative.libc.stdlib

object WvcLib extends LogSupport:

  /**
    * Helper function to allocate a string on heap as CString
    */
  private def toCString(str: String): CString =
    val len    = str.length + 1
    val buffer = stdlib.malloc(len).asInstanceOf[CString]
    var i      = 0
    while i < str.length do
      buffer(i) = str.charAt(i).toByte
      i += 1
    buffer(str.length) = 0.toByte
    buffer

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
      toCString(sql)
    catch
      case e: Throwable =>
        warn(e)
        // Return empty string on error
        toCString("")

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
              statusCode = e.statusCode,
              message = e.getMessage,
              location = locationOpt
            )
            CompileResponse(success = false, error = Some(error))
          case e: Throwable =>
            val error = CompileError(
              statusCode = StatusCode.INTERNAL_ERROR,
              message = Option(e.getMessage).getOrElse(e.getClass.getName)
            )
            CompileResponse(success = false, error = Some(error))

      // Convert response to JSON
      val responseJson = MessageCodec.of[CompileResponse].toJson(response)
      toCString(responseJson)
    catch
      case e: Throwable =>
        warn(e)
        // Return error response as JSON even if JSON serialization fails
        val errorResponse = CompileResponse(
          success = false,
          error = Some(
            CompileError(
              statusCode = StatusCode.COMPILATION_FAILURE,
              message = Option(e.getMessage).getOrElse(
                "Failed to serialize compilation result to JSON"
              )
            )
          )
        )
        // Use MessageCodec for consistent JSON formatting
        val errorJson = MessageCodec.of[CompileResponse].toJson(errorResponse)
        toCString(errorJson)

end WvcLib

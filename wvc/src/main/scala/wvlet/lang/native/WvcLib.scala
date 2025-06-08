package wvlet.lang.native

import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogSupport

import scala.scalanative.unsafe.*
import scala.scalanative.libc.stdlib

object WvcLib extends LogSupport:

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

end WvcLib

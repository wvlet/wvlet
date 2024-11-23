package wvlet.lang.native

import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogSupport

import scala.scalanative.unsafe.*

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
      trace(s"args: ${json}")
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
    *   generated SQL as a CString
    */
  @exported("wvlet_compile_query")
  def compile_query(argJson: CString): CString = 
    try
      val json = fromCString(argJson)
      val args = MessageCodec.of[Array[String]].fromJson(json)
      val (sql, _) = WvcMain.compileWvletQuery(args)
      
      val buffer = stackalloc[CChar](sql.length + 1)
      var i = 0
      while (i < sql.length) {
        buffer(i) = sql.charAt(i).toByte
        i += 1
      }
      buffer(sql.length) = 0.toByte
      buffer.asInstanceOf[CString]
    catch
      case e: Throwable =>
        warn(e)
        stackalloc[CChar](1).asInstanceOf[CString]

package wvlet.lang.compiler.analyzer.trino

import wvlet.uni.http.Http
import wvlet.uni.http.JVMHttpChannelFactory

/**
  * Per-platform init point. Registers the JVM HTTP channel factory with uni so subsequent
  * `Http.client` calls work without each caller having to remember to do this. Idempotent.
  */
trait TrinoCompat:
  def installHttpFactory(): Unit = Http.setDefaultChannelFactory(JVMHttpChannelFactory)

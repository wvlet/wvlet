package wvlet.lang.compiler.analyzer.trino

import wvlet.uni.http.Http
import wvlet.uni.http.JSHttpChannelFactory

/**
  * Per-platform init point. Registers uni's JS HTTP channel factory; the sync channel uses
  * worker_threads + Atomics.wait under Node and throws on browsers (no recovery path for sync HTTP
  * in the browser).
  */
trait TrinoCompat:
  def installHttpFactory(): Unit = Http.setDefaultChannelFactory(JSHttpChannelFactory)

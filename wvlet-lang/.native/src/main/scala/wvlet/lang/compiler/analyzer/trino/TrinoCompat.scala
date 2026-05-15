package wvlet.lang.compiler.analyzer.trino

import wvlet.uni.http.Http
import wvlet.uni.http.NativeHttpChannelFactory

/**
  * Per-platform init point. Registers uni's Native HTTP channel factory (libcurl-backed).
  */
trait TrinoCompat:
  def installHttpFactory(): Unit = Http.setDefaultChannelFactory(NativeHttpChannelFactory)

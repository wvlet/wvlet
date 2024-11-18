package wvlet.lang.compiler

import wvlet.log.LogSupport

object SourceIO extends IOCompat with LogSupport:
  private val ignoredFolders: Set[String] = Set("spec", "target")

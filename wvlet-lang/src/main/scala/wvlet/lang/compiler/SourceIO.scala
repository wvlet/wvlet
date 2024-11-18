package wvlet.lang.compiler

import wvlet.log.LogSupport

object SourceIO extends IOCompat with LogSupport:
  val ignoredFolders: Set[String] = Set("spec", "target")

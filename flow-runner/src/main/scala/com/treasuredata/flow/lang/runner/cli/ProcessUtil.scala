package com.treasuredata.flow.lang.runner.cli

object ProcessUtil:

  def launchInteractiveProcess(cmd: String*): Process =
    val proc = new ProcessBuilder(cmd*)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .start()
    proc

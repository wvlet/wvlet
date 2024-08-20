package com.treasuredata.flow.lang.runner.cli

import wvlet.log.LogSupport

import java.io.{BufferedWriter, OutputStreamWriter}

object Clipboard extends LogSupport:
  private val isMacOS: Boolean = sys.props.get("os.name").exists(_.toLowerCase.contains("mac"))

  def saveToClipboard(str: String): Unit =
    // Save query and result to clipboard (only for Mac OS, now)
    if isMacOS then
      val proc = ProcessUtil.launchInteractiveProcess("pbcopy")
      val out  = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()))
      out.write(str)
      out.flush()
      out.close()
      proc.waitFor()
      info(s"Clipped the output to the clipboard")
    else
      warn("clip command is not supported other than Mac OS")

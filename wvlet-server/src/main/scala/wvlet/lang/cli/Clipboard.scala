/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.cli

import wvlet.lang.runner.ProcessUtil
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

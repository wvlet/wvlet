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

import wvlet.uni.log.LogSupport

/**
  * Copies text to the system clipboard with `pbcopy` (macOS)
  */
object Clipboard extends LogSupport:

  def saveToClipboard(str: String): Unit =
    // `sh` feeds the text file to pbcopy's stdin; $1 keeps the path out of the shell syntax
    if TextCommand.run(str, "sh", "-c", "command -v pbcopy >/dev/null && pbcopy < \"$1\"", "sh")
    then
      info(s"Clipped the output to the clipboard")
    else
      warn("clip needs pbcopy (macOS), which could not be run")

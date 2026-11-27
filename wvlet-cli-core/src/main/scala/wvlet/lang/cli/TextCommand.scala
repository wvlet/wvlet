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

import wvlet.uni.io.IO
import wvlet.uni.io.ProcessConfig
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * Runs a terminal command on a text, e.g., `less` or `pbcopy`, on JVM, Node.js, and Native. The
  * text is passed as a temporary file appended to the command line, because uni's cross-platform
  * `IO.run` has no stdin input; the command shares the terminal's stdin and stdout.
  */
object TextCommand extends LogSupport:

  /**
    * Run `command... <text file>`. Returns false when the command is missing or fails
    */
  def run(text: String, command: String*): Boolean =
    try
      val file = IO.createTempFile(prefix = "wvlet-repl", suffix = ".txt")
      try
        IO.writeString(file, text)
        IO.run(command :+ file.path, ProcessConfig.default.withInheritIO(true)).isSuccess
      finally
        IO.deleteIfExists(file)
    catch
      case NonFatal(e) =>
        debug(e)
        false

end TextCommand

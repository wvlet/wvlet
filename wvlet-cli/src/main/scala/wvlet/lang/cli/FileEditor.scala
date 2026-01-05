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

import wvlet.lang.compiler.WorkEnv
import wvlet.log.LogSupport

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption

/**
  * Utility for file editing operations in the REPL.
  *
  * Provides functionality to edit .wv files using external editors, create new files, view file
  * contents, and save query results to files.
  */
object FileEditor extends LogSupport:

  /**
    * Detects the preferred editor from environment variables or falls back to platform defaults.
    *
    * @return
    *   The editor command to use
    */
  def detectEditor: String =
    sys.env
      .get("EDITOR")
      .orElse(sys.env.get("VISUAL"))
      .getOrElse {
        // Platform-specific defaults
        val os = sys.props.getOrElse("os.name", "").toLowerCase
        if os.contains("windows") then
          "notepad"
        else
          // Try to find a common editor
          val editors = Seq("nano", "vi", "vim")
          editors
            .find { editor =>
              try
                val proc = Runtime.getRuntime.exec(Array("which", editor))
                proc.waitFor() == 0
              catch
                case _: Exception =>
                  false
            }
            .getOrElse("vi")
      }

  /**
    * Opens a file in the external editor and waits for it to close.
    *
    * @param filePath
    *   Path to the file to edit
    * @param workEnv
    *   Working environment for resolving relative paths
    * @return
    *   true if the editor exited successfully, false otherwise
    */
  def editFile(filePath: String, workEnv: WorkEnv): Boolean =
    val file       = resolveFile(filePath, workEnv)
    val editor     = detectEditor
    val editorArgs = parseEditorCommand(editor, file.getAbsolutePath)
    try
      info(s"Opening ${file.getPath} with ${editor}")
      val proc = new ProcessBuilder(editorArgs*)
        .inheritIO()
        .start()
      val exitCode = proc.waitFor()
      if exitCode == 0 then
        info(s"Editor closed")
        true
      else
        error(s"Editor exited with code ${exitCode}")
        false
    catch
      case e: Exception =>
        error(s"Failed to launch editor: ${e.getMessage}")
        false

  /**
    * Creates a new file (with optional template content) and opens it in the editor.
    *
    * @param filePath
    *   Path to the new file
    * @param workEnv
    *   Working environment for resolving relative paths
    * @param template
    *   Optional template content for the new file
    * @return
    *   true if the file was created and opened successfully
    */
  def newFile(filePath: String, workEnv: WorkEnv, template: Option[String] = None): Boolean =
    val file = resolveFile(filePath, workEnv)
    if file.exists() then
      warn(s"File already exists: ${file.getPath}. Opening existing file.")
      editFile(filePath, workEnv)
    else
      try
        // Create parent directories if needed
        Option(file.getParentFile).foreach(_.mkdirs())
        val content = template.getOrElse(defaultTemplate(filePath))
        Files.writeString(file.toPath, content)
        info(s"Created new file: ${file.getPath}")
        editFile(filePath, workEnv)
      catch
        case e: Exception =>
          error(s"Failed to create file: ${e.getMessage}")
          false

  /**
    * Reads and returns the contents of a file.
    *
    * @param filePath
    *   Path to the file to read
    * @param workEnv
    *   Working environment for resolving relative paths
    * @return
    *   The file contents, or None if the file doesn't exist or can't be read
    */
  def readFile(filePath: String, workEnv: WorkEnv): Option[String] =
    val file = resolveFile(filePath, workEnv)
    if !file.exists() then
      error(s"File not found: ${file.getPath}")
      None
    else if file.isDirectory then
      error(s"Path is a directory: ${file.getPath}")
      None
    else
      try Some(Files.readString(file.toPath))
      catch
        case e: Exception =>
          error(s"Failed to read file: ${e.getMessage}")
          None

  /**
    * Saves content to a file.
    *
    * @param filePath
    *   Path to the file to write
    * @param content
    *   Content to write
    * @param workEnv
    *   Working environment for resolving relative paths
    * @param append
    *   If true, append to existing file; otherwise overwrite
    * @return
    *   true if successful
    */
  def saveFile(filePath: String, content: String, workEnv: WorkEnv, append: Boolean = false): Boolean =
    val file = resolveFile(filePath, workEnv)
    try
      // Create parent directories if needed
      Option(file.getParentFile).foreach(_.mkdirs())
      if append then
        Files.writeString(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      else
        Files.writeString(file.toPath, content)
      info(s"Saved to: ${file.getPath}")
      true
    catch
      case e: Exception =>
        error(s"Failed to save file: ${e.getMessage}")
        false

  /**
    * Lists .wv files in the work directory.
    *
    * @param workEnv
    *   Working environment
    * @param recursive
    *   If true, search recursively
    * @return
    *   List of .wv file paths relative to workEnv
    */
  def listWvFiles(workEnv: WorkEnv, recursive: Boolean = false): List[String] =
    val workDir = new File(workEnv.path)
    if !workDir.exists() || !workDir.isDirectory then
      Nil
    else
      def collectFiles(dir: File, prefix: String): List[String] =
        val files = Option(dir.listFiles()).getOrElse(Array.empty)
        files.toList.flatMap { f =>
          val relativePath = if prefix.isEmpty then f.getName else s"${prefix}/${f.getName}"
          if f.isDirectory && recursive then
            collectFiles(f, relativePath)
          else if f.isFile && f.getName.endsWith(".wv") then
            List(relativePath)
          else
            Nil
        }
      collectFiles(workDir, "").sorted

  /**
    * Resolves a file path relative to the work environment.
    */
  private def resolveFile(filePath: String, workEnv: WorkEnv): File =
    val f = new File(filePath)
    if f.isAbsolute then
      f
    else
      new File(workEnv.path, filePath)

  /**
    * Parses an editor command that may contain arguments (e.g., "code --wait").
    */
  private def parseEditorCommand(editor: String, filePath: String): Seq[String] =
    // Split by whitespace, but handle the case where editor might have spaces
    val parts = editor.split("\\s+").toSeq
    parts :+ filePath

  /**
    * Generates a default template for new .wv files.
    */
  private def defaultTemplate(filePath: String): String =
    val fileName = new File(filePath).getName.stripSuffix(".wv")
    s"""-- ${fileName}.wv
       |-- Created with wv CLI
       |
       |""".stripMargin

end FileEditor

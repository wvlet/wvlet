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
package wvlet.lang.skill

import wvlet.lang.api.StatusCode
import wvlet.uni.io.FileSystem
import wvlet.uni.io.IOPath
import wvlet.uni.io.WriteMode
import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.option

/**
  * The AI-agent skill (plugins/wvlet/skills/wvlet) embedded in the compiler, so an agent can read
  * the guide that matches the installed wvlet version with `wvlet skill`, without network access.
  */
object Skill:
  /**
    * Folder name of the skill, used by `install`
    */
  val name = "wvlet"

  /**
    * Relative paths of the skill files, e.g., `SKILL.md`, `references/syntax.md`
    */
  def fileNames: Seq[String] = BundledSkill.files.keys.toSeq

  /**
    * Find a skill file by its path, file name, or base name: `references/syntax.md`, `syntax.md`,
    * and `syntax` all find the syntax reference.
    */
  def find(fileName: String): Option[String] =
    val key                            = fileName.stripPrefix("./")
    def baseName(path: String): String = path.split('/').last.stripSuffix(".md")
    BundledSkill
      .files
      .get(key)
      .orElse(
        BundledSkill
          .files
          .collectFirst {
            case (k, v) if k.endsWith(s"/${key}") =>
              v
          }
      )
      .orElse(
        BundledSkill
          .files
          .collectFirst {
            case (k, v) if baseName(k).equalsIgnoreCase(baseName(key)) =>
              v
          }
      )

  /**
    * Read a skill file, or fail with the list of available files
    */
  def read(fileName: String): String = find(fileName).getOrElse {
    throw StatusCode
      .INVALID_ARGUMENT
      .newException(
        s"Unknown skill file: ${fileName}. Available files: ${fileNames.mkString(", ")}"
      )
  }

  /**
    * Write the skill to `<dir>/wvlet/`, replacing existing files, and return the written paths. For
    * Claude Code, install into `.claude/skills` of a project or `~/.claude/skills`.
    */
  def install(dir: String): Seq[String] =
    val root = IOPath.parse(dir).resolve(name)
    BundledSkill
      .files
      .map { (path, content) =>
        val target = root.resolve(path)
        target.parent.foreach(FileSystem.createDirectoryIfNotExists)
        FileSystem.writeString(target, content, WriteMode.Create)
        target.path
      }
      .toSeq

end Skill

/**
  * Options of the `wvlet skill` command, shared by the JVM, Node.js, and Native CLIs
  */
case class SkillOption(
    @argument(description = "Skill file to print, e.g., syntax or references/testing.md")
    file: Option[String] = None,
    @option(prefix = "--list", description = "List the skill files")
    list: Boolean = false,
    @option(
      prefix = "--install",
      description = "Write the skill to <dir>/wvlet, e.g., .claude/skills or ~/.claude/skills"
    )
    install: Option[String] = None
):
  def withFile(file: String): SkillOption   = copy(file = Some(file))
  def noFile(): SkillOption                 = copy(file = None)
  def withList(list: Boolean): SkillOption  = copy(list = list)
  def withInstall(dir: String): SkillOption = copy(install = Some(dir))
  def noInstall(): SkillOption              = copy(install = None)

object SkillOption:
  /**
    * Print SKILL.md (or the named file), list the files, or install the skill
    */
  def execute(opt: SkillOption): Unit =
    opt.install match
      case Some(dir) =>
        Skill.install(dir).foreach(p => println(s"wrote ${p}"))
      case None if opt.list =>
        Skill.fileNames.foreach(println)
      case None =>
        print(Skill.read(opt.file.getOrElse("SKILL.md")))

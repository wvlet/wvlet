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
import wvlet.uni.weaver.Weaver
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
    def baseName(path: String): String = path.split('/').lastOption.getOrElse("").stripSuffix(".md")
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

end Skill

/**
  * Options of the `wvlet skill` command, shared by the JVM, Node.js, and Native CLIs.
  *
  * {{{
  * wvlet skill [show] [file]   print SKILL.md or a reference (syntax, models, testing, cli)
  * wvlet skill files           list the files of the skill
  * wvlet skill install         install for coding agents (--agent claude,codex,pi; --global; --dir)
  * wvlet skill list            show where the skill is installed and whether it is current
  * wvlet skill remove --yes    remove the skill wvlet installed
  * }}}
  */
case class SkillOption(
    @argument(description = "show [file] | files | install | list | remove")
    args: Seq[String] = Nil,
    @option(
      prefix = "-a,--agent",
      description = "Agents to install for: claude, codex, pi (default: all)"
    )
    agents: Seq[String] = Nil,
    @option(prefix = "-g,--global", description = "Install under the home directory")
    global: Boolean = false,
    @option(prefix = "--dir", description = "Install into <dir>/wvlet instead of agent folders")
    dir: Option[String] = None,
    @option(
      prefix = "--force",
      description = "Replace folders wvlet did not write or that were edited"
    )
    force: Boolean = false,
    @option(prefix = "--dry-run", description = "Show what remove would delete")
    dryRun: Boolean = false,
    @option(prefix = "-y,--yes", description = "Confirm remove")
    yes: Boolean = false,
    @option(prefix = "--json", description = "Print results as JSON")
    json: Boolean = false
):
  def withArgs(args: Seq[String]): SkillOption     = copy(args = args)
  def withAgents(agents: Seq[String]): SkillOption = copy(agents = agents)
  def withGlobal(global: Boolean): SkillOption     = copy(global = global)
  def withDir(dir: String): SkillOption            = copy(dir = Some(dir))
  def noDir(): SkillOption                         = copy(dir = None)
  def withForce(force: Boolean): SkillOption       = copy(force = force)
  def withDryRun(dryRun: Boolean): SkillOption     = copy(dryRun = dryRun)
  def withYes(yes: Boolean): SkillOption           = copy(yes = yes)
  def withJson(json: Boolean): SkillOption         = copy(json = json)

end SkillOption

object SkillOption:
  /**
    * Run a `wvlet skill` subcommand and return its output
    */
  def run(opt: SkillOption, installer: SkillInstaller): String =
    def report(results: Seq[SkillResult]): String =
      if opt.json then
        summon[Weaver[Seq[SkillResult]]].toJson(results) + "\n"
      else if results.isEmpty then
        "wvlet skill is not installed\n"
      else
        results
          .map { r =>
            val version = r.version.map(v => s" (${v})").getOrElse("")
            val detail  = r.detail.map(d => s": ${d}").getOrElse("")
            s"${r.status}\t${r.agents.mkString(",")}\t${r.path}${version}${detail}"
          }
          .mkString("", "\n", "\n")

    val agents = SkillAgent.parse(opt.agents)
    opt.args match
      case Seq("install") =>
        report(installer.install(agents, opt.global, opt.dir, opt.force))
      case Seq("list") =>
        report(installer.list())
      case Seq("remove") =>
        if !opt.yes && !opt.dryRun then
          throw StatusCode
            .INVALID_ARGUMENT
            .newException("wvlet skill remove needs --yes (or --dry-run to preview)")
        report(installer.remove(agents, opt.global, opt.dir, opt.force, opt.dryRun))
      case Seq("files") =>
        Skill.fileNames.mkString("", "\n", "\n")
      case Seq() | Seq("show") =>
        Skill.read("SKILL.md")
      case Seq("show", file) =>
        Skill.read(file)
      case Seq(file) =>
        Skill.read(file)
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Unexpected arguments: ${other.mkString(" ")}")

  end run

  /**
    * Run a `wvlet skill` subcommand and print its output
    */
  def execute(opt: SkillOption): Unit = print(
    run(opt, SkillInstaller(wvlet.lang.BuildInfo.version))
  )

end SkillOption

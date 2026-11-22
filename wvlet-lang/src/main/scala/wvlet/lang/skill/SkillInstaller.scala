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
import wvlet.uni.weaver.Weaver

import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

/**
  * A coding agent that reads Agent Skills from a folder
  *
  * @param id
  *   the name used with `--agent`
  * @param aliases
  *   other accepted names
  */
enum SkillAgent(val id: String, val aliases: Seq[String]):
  /** Claude Code: `.claude/skills`, or `$CLAUDE_CONFIG_DIR/skills` (`~/.claude/skills`) */
  case Claude extends SkillAgent("claude", Seq("claude-code"))

  /** Codex: the shared `.agents/skills` folder */
  case Codex extends SkillAgent("codex", Nil)

  /** Pi: the shared `.agents/skills` folder */
  case Pi extends SkillAgent("pi", Nil)

  def names: Seq[String] = id +: aliases

object SkillAgent:
  /**
    * Parse agent names; each value may hold several names separated by commas
    */
  def parse(values: Seq[String]): Seq[SkillAgent] =
    values
      .flatMap(_.split(","))
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
      .map { name =>
        SkillAgent
          .values
          .find(_.names.contains(name))
          .getOrElse {
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"Unknown agent: ${name}. Supported agents: ${SkillAgent
                    .values
                    .map(_.id)
                    .mkString(", ")}"
              )
          }
      }
      .distinct

/**
  * What an install or remove did to one skill folder
  */
enum SkillStatus:
  case installed,
    updated,
    unchanged,
    removed,
    skipped,
    missing

/**
  * The outcome for one skill folder. `detail` explains a `skipped` status.
  */
case class SkillResult(
    agents: Seq[String],
    path: String,
    status: String,
    version: Option[String] = None,
    detail: Option[String] = None
) derives Weaver

/**
  * Recorded in each installed folder, so later runs touch only folders wvlet wrote and that nobody
  * edited since
  */
case class SkillMarker(version: String, hash: String, files: Seq[String]) derives Weaver

/**
  * Installs the bundled skill for coding agents, following the layout of `tdx skill`: a project
  * install writes `.agents/skills/wvlet` (read by Codex and Pi) and `.claude/skills/wvlet` (Claude
  * Code); `global` writes the same folders under the home directory.
  *
  * Every agent folder gets its own copy rather than a symlink to the shared one. The skill is
  * small, and copies behave the same on every platform (Windows needs privileges for symlinks, and
  * file deletion through a link differs between JVM, Node.js, and Native).
  *
  * @param baseDir
  *   the project folder for a project install
  * @param homeDir
  *   the home folder for a global install
  * @param claudeConfigDir
  *   `$CLAUDE_CONFIG_DIR` when it is set
  */
class SkillInstaller(
    version: String,
    baseDir: IOPath = IOPath.parse("."),
    homeDir: IOPath = FileSystem.homeDirectory,
    claudeConfigDir: Option[IOPath] = sys.env.get("CLAUDE_CONFIG_DIR").map(IOPath.parse)
):
  import SkillInstaller.*

  /**
    * The skills folder an agent reads
    */
  def skillsDir(agent: SkillAgent, global: Boolean): IOPath =
    val root =
      if global then
        homeDir
      else
        baseDir
    agent match
      case SkillAgent.Claude if global =>
        claudeConfigDir.getOrElse(homeDir.resolve(".claude")).resolve("skills")
      case SkillAgent.Claude =>
        root.resolve(".claude/skills")
      case _ =>
        root.resolve(".agents/skills")

  /**
    * Agent folders grouped by location, since Codex and Pi share one
    */
  private def targets(agents: Seq[SkillAgent], global: Boolean): Seq[(IOPath, Seq[SkillAgent])] =
    agents
      .groupBy(a => skillsDir(a, global).resolve(Skill.name).path)
      .toSeq
      .sortBy(_._1)
      .map { (path, as) =>
        IOPath.parse(path) -> as
      }

  /**
    * Install or update the skill for the given agents, or into `<dir>/wvlet` when `dir` is given
    */
  def install(
      agents: Seq[SkillAgent],
      global: Boolean = false,
      dir: Option[String] = None,
      force: Boolean = false
  ): Seq[SkillResult] = locations(agents, global, dir).map { (root, names) =>
    installInto(root, names, force)
  }

  /**
    * Remove the skill wvlet installed for the given agents. Folders wvlet did not write, or that
    * were edited after install, are kept unless `force` is set.
    */
  def remove(
      agents: Seq[SkillAgent],
      global: Boolean = false,
      dir: Option[String] = None,
      force: Boolean = false,
      dryRun: Boolean = false
  ): Seq[SkillResult] = locations(agents, global, dir).map { (root, names) =>
    inspect(root) match
      case Missing =>
        SkillResult(names, root.path, SkillStatus.missing.toString)
      case problem: NotManaged if !force =>
        SkillResult(names, root.path, SkillStatus.skipped.toString, detail = Some(problem.reason))
      case state =>
        if !dryRun then
          FileSystem.deleteRecursively(root)
        SkillResult(names, root.path, SkillStatus.removed.toString, state.version)
  }

  /**
    * Report the skill folders of every agent, in the project and in the home directory
    */
  def list(): Seq[SkillResult] =
    for
      global     <- Seq(false, true)
      (root, as) <- targets(SkillAgent.values.toSeq, global)
      names = as.map(_.id)
      state = inspect(root)
      if state != Missing
    yield state match
      case Managed(marker) =>
        val status =
          if marker.hash == bundleHash then
            SkillStatus.unchanged
          else
            SkillStatus.updated
        SkillResult(
          names,
          root.path,
          if status == SkillStatus.unchanged then
            "current"
          else
            "outdated"
          ,
          Some(marker.version)
        )
      case problem: NotManaged =>
        SkillResult(
          names,
          root.path,
          SkillStatus.skipped.toString,
          problem.version,
          Some(problem.reason)
        )
      case Missing =>
        SkillResult(names, root.path, SkillStatus.missing.toString)

  private def locations(
      agents: Seq[SkillAgent],
      global: Boolean,
      dir: Option[String]
  ): Seq[(IOPath, Seq[String])] =
    dir match
      case Some(d) =>
        Seq(IOPath.parse(expandHome(d)).resolve(Skill.name) -> Seq("dir"))
      case None =>
        val selected =
          if agents.isEmpty then
            SkillAgent.values.toSeq
          else
            agents
        // Name every agent reading a folder, since Codex and Pi share one
        val all = targets(SkillAgent.values.toSeq, global).toMap
        targets(selected, global).map((root, _) => root -> all(root).map(_.id))

  private def installInto(root: IOPath, names: Seq[String], force: Boolean): SkillResult =
    def write(oldFiles: Seq[String]): Unit =
      // Drop files a previous version installed but this version no longer ships
      oldFiles
        .filterNot(BundledSkill.files.contains)
        .foreach(f => FileSystem.delete(root.resolve(f)))
      for (path, content) <- BundledSkill.files do
        val target = root.resolve(path)
        target.parent.foreach(FileSystem.createDirectoryIfNotExists)
        FileSystem.writeString(target, content, WriteMode.Create)
      FileSystem.createDirectoryIfNotExists(root)
      val marker = SkillMarker(version, bundleHash, BundledSkill.files.keys.toSeq)
      FileSystem.writeString(
        root.resolve(markerFile),
        Weaver.of[SkillMarker].toJson(marker),
        WriteMode.Create
      )

    inspect(root) match
      case Missing =>
        write(Nil)
        SkillResult(names, root.path, SkillStatus.installed.toString, Some(version))
      case Managed(marker) if marker.hash == bundleHash =>
        if marker.version != version then
          write(marker.files)
        SkillResult(names, root.path, SkillStatus.unchanged.toString, Some(version))
      case Managed(marker) =>
        write(marker.files)
        SkillResult(names, root.path, SkillStatus.updated.toString, Some(version))
      case problem: NotManaged if force =>
        write(problem.files)
        SkillResult(names, root.path, SkillStatus.updated.toString, Some(version))
      case problem: NotManaged =>
        SkillResult(
          names,
          root.path,
          SkillStatus.skipped.toString,
          problem.version,
          Some(s"${problem.reason}; use --force to replace it")
        )

    end match

  end installInto

  private def inspect(root: IOPath): FolderState =
    if !FileSystem.exists(root) then
      Missing
    else
      val markerPath = root.resolve(markerFile)
      if !FileSystem.isFile(markerPath) then
        NotManaged("not installed by wvlet", None, Nil)
      else
        try
          val marker = Weaver.of[SkillMarker].fromJson(FileSystem.readString(markerPath))
          if hashOf(root, marker.files) == marker.hash then
            Managed(marker)
          else
            NotManaged("edited after install", Some(marker.version), marker.files)
        catch
          case NonFatal(e) =>
            NotManaged(s"unreadable ${markerFile}", None, Nil)

end SkillInstaller

object SkillInstaller:
  /**
    * The file in an installed folder that records what wvlet wrote
    */
  val markerFile = ".wvlet-skill.json"

  private sealed trait FolderState:
    def version: Option[String]

  private case object Missing extends FolderState:
    def version: Option[String] = None

  private case class Managed(marker: SkillMarker) extends FolderState:
    def version: Option[String] = Some(marker.version)

  private case class NotManaged(reason: String, version: Option[String], files: Seq[String])
      extends FolderState

  private def hash(files: Seq[(String, String)]): String =
    MurmurHash3.orderedHash(files.sortBy(_._1).map((p, c) => s"${p}\u0000${c}")).toHexString

  /**
    * Hash of the bundled skill files
    */
  val bundleHash: String = hash(BundledSkill.files.toSeq)

  private def hashOf(root: IOPath, files: Seq[String]): String = hash(
    files.map { f =>
      val p = root.resolve(f)
      f -> (
        if FileSystem.isFile(p) then
          FileSystem.readString(p)
        else
          ""
      )
    }
  )

  private[skill] def expandHome(dir: String): String =
    if dir == "~" || dir.startsWith("~/") then
      s"${FileSystem.homeDirectory.path}${dir.drop(1)}"
    else
      dir

end SkillInstaller

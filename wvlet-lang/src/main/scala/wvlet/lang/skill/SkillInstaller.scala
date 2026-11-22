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
  * The outcome for one skill folder.
  *
  * @param status
  *   `install`: installed, updated, unchanged, or skipped. `remove`: removed, would-remove,
  *   missing, or skipped. `list`: current, outdated, unmanaged, or edited.
  * @param detail
  *   why a folder was skipped
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
  * Only files wvlet wrote are ever deleted: the paths a marker lists must be bundled-style relative
  * paths inside the folder, and files the user added keep the folder from being removed.
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
    claudeConfigDir: Option[IOPath] = SkillEnv.get("CLAUDE_CONFIG_DIR").map(IOPath.parse)
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
    agents.groupBy(a => skillsDir(a, global).resolve(Skill.name)).toSeq.sortBy(_._1.path)

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
    * Remove the skill wvlet installed for the given agents. Folders wvlet did not write, that were
    * edited after install, or that hold files the user added are kept unless `force` is set; even
    * then only the files wvlet wrote are deleted.
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
        SkillResult(names, root.path, "missing")
      case problem: NotManaged if !force =>
        SkillResult(names, root.path, "skipped", problem.version, Some(problem.reason))
      case state =>
        if dryRun then
          SkillResult(names, root.path, "would-remove", state.version)
        else
          deleteInstalled(root, state.files)
          SkillResult(names, root.path, "removed", state.version)
  }

  /**
    * Report the skill folders of the given agents (all when empty), in the project and in the home
    * directory, or in `<dir>/wvlet` when `dir` is given
    */
  def list(
      agents: Seq[SkillAgent] = Nil,
      global: Option[Boolean] = None,
      dir: Option[String] = None
  ): Seq[SkillResult] =
    val scopes = global.map(Seq(_)).getOrElse(Seq(false, true))
    val roots  = scopes
      .flatMap(g => locations(agents, g, dir))
      // A project at the home directory has the same folders in both scopes
      .distinctBy((root, _) => root.path.stripPrefix("./"))
    for
      (root, names) <- roots
      state = inspect(root)
      if state != Missing
    yield state match
      case Managed(marker, _) =>
        val status =
          if marker.hash == bundleHash then
            "current"
          else
            "outdated"
        SkillResult(names, root.path, status, Some(marker.version))
      case problem: NotManaged =>
        val status =
          if problem.files.isEmpty then
            "unmanaged"
          else
            "edited"
        SkillResult(names, root.path, status, problem.version, Some(problem.reason))
      case Missing =>
        SkillResult(names, root.path, "missing")

  end list

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
      val marker = SkillMarker(version, bundleHash, BundledSkill.files.keys.toSeq)
      FileSystem.writeString(
        root.resolve(markerFile),
        Weaver.of[SkillMarker].toJson(marker),
        WriteMode.Create
      )

    inspect(root) match
      case Missing =>
        write(Nil)
        SkillResult(names, root.path, "installed", Some(version))
      case Managed(marker, _) if marker.hash == bundleHash =>
        if marker.version != version then
          write(marker.files)
        SkillResult(names, root.path, "unchanged", Some(version))
      case Managed(marker, _) =>
        write(marker.files)
        SkillResult(names, root.path, "updated", Some(version))
      case problem: NotManaged if force =>
        write(problem.files)
        SkillResult(names, root.path, "updated", Some(version))
      case problem: NotManaged =>
        SkillResult(
          names,
          root.path,
          "skipped",
          problem.version,
          Some(s"${problem.reason}; use --force to replace it")
        )

    end match

  end installInto

  /**
    * Delete the files wvlet wrote and the marker, then the folders they leave empty
    */
  private def deleteInstalled(root: IOPath, files: Seq[String]): Unit =
    (files :+ markerFile).foreach(f => FileSystem.delete(root.resolve(f)))
    val dirs = files
      .flatMap(f => f.split('/').inits.drop(1).filter(_.nonEmpty).map(_.mkString("/")))
      .distinct
      .sortBy(-_.length)
    for d <- dirs :+ "" do
      val p =
        if d.isEmpty then
          root
        else
          root.resolve(d)
      if FileSystem.isDirectory(p) && FileSystem.list(p).isEmpty then
        FileSystem.delete(p)

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
          if !marker.files.forall(isSafePath) then
            NotManaged(s"${markerFile} lists paths outside the skill folder", None, Nil)
          else if hashOf(root, marker.files) != marker.hash then
            NotManaged("edited after install", Some(marker.version), marker.files)
          else
            val extra = filesUnder(root).filterNot((marker.files :+ markerFile).toSet)
            if extra.nonEmpty then
              NotManaged(
                s"has files wvlet did not write: ${extra.mkString(", ")}",
                Some(marker.version),
                marker.files
              )
            else
              Managed(marker, marker.files)
        catch
          case NonFatal(_) =>
            NotManaged(s"unreadable ${markerFile}", None, Nil)

  /**
    * Relative paths of every file under the folder
    */
  private def filesUnder(root: IOPath): Seq[String] =
    def walk(dir: IOPath, prefix: String): Seq[String] = FileSystem
      .list(dir)
      .flatMap { p =>
        val name = s"${prefix}${p.fileName}"
        if FileSystem.isDirectory(p) then
          walk(p, s"${name}/")
        else
          Seq(name)
      }
    walk(root, "")

end SkillInstaller

object SkillInstaller:
  /**
    * The file in an installed folder that records what wvlet wrote
    */
  val markerFile = ".wvlet-skill.json"

  private sealed trait FolderState:
    def version: Option[String]
    def files: Seq[String]

  private case object Missing extends FolderState:
    def version: Option[String] = None
    def files: Seq[String]      = Nil

  private case class Managed(marker: SkillMarker, files: Seq[String]) extends FolderState:
    def version: Option[String] = Some(marker.version)

  private case class NotManaged(reason: String, version: Option[String], files: Seq[String])
      extends FolderState

  /**
    * A marker entry must be a relative path inside the folder, like the bundled ones
    */
  private def isSafePath(path: String): Boolean =
    path.nonEmpty && !path.startsWith("/") && !path.contains('\\') && !path.contains(':') &&
      path.split('/').forall(part => part.nonEmpty && part != "." && part != "..")

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

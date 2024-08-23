package wvlet.lang.compiler

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/**
  * An abstraction over local files or files in remote GitHub repositories
  */
trait VirtualFile:
  def name: String
  def path: String

  def exists: Boolean
  def isDirectory: Boolean
  def listFiles: Seq[VirtualFile]

case class LocalFile(name: String, path: String) extends VirtualFile:
  override def exists: Boolean      = Files.exists(Path.of(path))
  override def isDirectory: Boolean = exists && Files.isDirectory(Path.of(path))
  override def listFiles: Seq[VirtualFile] =
    if isDirectory then
      Files
        .list(Path.of(path))
        .toList
        .asScala
        .map(p => LocalFile(p.getFileName.toString, p.toString))
        .toSeq
    else
      Seq.empty

/**
  * TODO: Download GitHub archive to .cache/wvlet/repository/github/${owner}/${repo}/${ref} and
  * provide file paths
  * @param owner
  * @param repo
  * @param ref
  */
case class GitHubArchive(owner: String, repo: String, ref: String) extends VirtualFile:
  def name: String                  = s"github:${owner}/${repo}@${ref}"
  override def path: String         = s"https://github.com/${owner}/${repo}"
  override def exists: Boolean      = ???
  override def isDirectory: Boolean = true
  override def listFiles            = ???

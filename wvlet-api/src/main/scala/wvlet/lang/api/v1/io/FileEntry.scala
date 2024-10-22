package wvlet.lang.api.v1.io

import scala.annotation.tailrec

sealed trait FileEntry:
  def isFile: Boolean = !isDirectory
  def isDirectory: Boolean

object FileEntry:
  case class LocalFile(name: String, size: Long) extends FileEntry:
    override def isDirectory: Boolean = false

  case class Directory(name: String) extends FileEntry:
    override def isDirectory: Boolean = true

  private def isSafeRelativePath(path: String): Boolean =
    @tailrec
    def loop(pos: Int, path: List[String]): Boolean =
      if pos < 0 then
        false
      else if path.isEmpty then
        true
      else if path.head == ".." then
        loop(pos - 1, path.tail)
      else
        loop(pos + 1, path.tail)

    loop(0, path.split("/").toList)

case class FileList(path: String, files: List[FileEntry])

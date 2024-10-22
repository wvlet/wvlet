package wvlet.lang.api.v1.io

import wvlet.lang.api.StatusCode

import scala.annotation.tailrec

case class FileEntry(
    name: String,
    path: String,
    isDirectory: Boolean,
    size: Long,
    lastUpdatedAtMills: Long
):
  def isFile: Boolean = !isDirectory

object FileEntry:
  def validateRelativePath(path: String): Unit =
    if !isSafeRelativePath(path) then
      throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid path: ${path}")

  def isSafeRelativePath(path: String): Boolean =
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

package wvlet.lang.api.v1.frontend

import wvlet.airframe.http.{RPC, RxRouter, RxRouterProvider}
import wvlet.lang.api.v1.io.{FileEntry, FileList}

@RPC
trait FileApi:
  import FileApi.*

  /**
    * Return the file list in the given path
    * @param relativePath
    * @return
    */
  def fileList(request: FileListRequest): FileList

  def readFile(request: ReadFileRequest): String
  def saveFile(request: SaveFileRequest): Unit

object FileApi extends RxRouterProvider:
  override def router: RxRouter = RxRouter.of[FileApi]

  case class FileListRequest(relativePath: String):
    FileEntry.validateRelativePath(relativePath)

  case class ReadFileRequest(relativePath: String):
    FileEntry.validateRelativePath(relativePath)

  case class SaveFileRequest(relativePath: String, content: String):
    FileEntry.validateRelativePath(relativePath)

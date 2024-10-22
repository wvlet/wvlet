package wvlet.lang.api.v1.frontend

import wvlet.airframe.http.{RPC, RxRouter, RxRouterProvider}
import wvlet.lang.api.v1.io.FileEntry

@RPC
trait FileApi:
  import FileApi.*

  /**
    * Return the file list in the given path
    * @param relativePath
    * @return
    */
  def listFiles(request: FileRequest): List[FileEntry]
  def getFile(request: FileRequest): FileEntry

  /**
    * Get a list of FileEntry along with the given path
    * @param request
    * @return
    */
  def getPath(request: FileRequest): List[FileEntry]
  def readFile(request: FileRequest): FileEntry

  def saveFile(request: SaveFileRequest): Unit

object FileApi extends RxRouterProvider:
  override def router: RxRouter = RxRouter.of[FileApi]

  case class FileRequest(relativePath: String):
    FileEntry.validateRelativePath(relativePath)

  case class SaveFileRequest(relativePath: String, content: String):
    FileEntry.validateRelativePath(relativePath)

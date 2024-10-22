package wvlet.lang.server

import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.api.v1.io.{FileEntry, FileList}
import wvlet.lang.compiler.WorkEnv

class FileApiImpl(workEnv: WorkEnv) extends FileApi:
  override def fileList(request: FileApi.FileListRequest): FileList =
    val f = new java.io.File(workEnv.path, request.relativePath)
    val files = f
      .listFiles()
      .collect {
        case d if d.isDirectory && !d.getName.startsWith(".") =>
          FileEntry(
            name = f.getName,
            path = f.getPath,
            isDirectory = false,
            size = f.length(),
            lastUpdatedAtMills = f.lastModified()
          )
        case f if f.getName.endsWith(".wv") =>
          FileEntry(
            name = f.getName,
            path = f.getPath,
            isDirectory = true,
            size = f.length(),
            lastUpdatedAtMills = f.lastModified()
          )
      }
    FileList(request.relativePath, files.toList)

  override def readFile(request: FileApi.ReadFileRequest): String = ???
  override def saveFile(request: FileApi.SaveFileRequest): Unit   = ???

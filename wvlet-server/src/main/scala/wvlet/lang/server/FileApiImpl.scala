package wvlet.lang.server

import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.api.v1.io.FileEntry
import wvlet.lang.compiler.WorkEnv
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

class FileApiImpl(workEnv: WorkEnv) extends FileApi with LogSupport:
  private def toFileEntry(f: java.io.File): FileEntry = FileEntry(
    name = f.getName,
    path = f.getPath,
    exists = f.exists(),
    isDirectory = f.isDirectory,
    size = f.length(),
    lastUpdatedAtMillis = f.lastModified()
  )

  private def getFile(path: String): java.io.File = new java.io.File(workEnv.path, path)

  override def listFiles(request: FileApi.FileRequest): List[FileEntry] =
    val f                          = getFile(request.relativePath)
    val files: Array[java.io.File] = Option(f.listFiles()).getOrElse(Array.empty)
    val entries = files.collect {
      case d if d.isDirectory && !d.getName.startsWith(".") =>
        toFileEntry(d)
      case f if f.getName.endsWith(".wv") =>
        toFileEntry(f)
    }
    entries.toList

  override def getFile(request: FileApi.FileRequest): FileEntry =
    val f = getFile(request.relativePath)
    toFileEntry(f)

  override def getPath(request: FileApi.FileRequest): List[FileEntry] =
    val paths: Array[String] = request.relativePath.split("/")
    (1 to paths.size)
      .map { i =>
        val p = paths.take(i).mkString("/")
        val f = getFile(p)
        toFileEntry(f)
      }
      .toList

  override def readFile(request: FileApi.FileRequest): FileEntry =
    val f = getFile(request.relativePath)
    toFileEntry(f).copy(content = Some(IOUtil.readAsString(f)))

  override def saveFile(request: FileApi.SaveFileRequest): Unit = ???

end FileApiImpl

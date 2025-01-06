package wvlet.lang.compiler

import wvlet.log.LogSupport

object SourceIO extends IOCompat with LogSupport:
  val ignoredFolders: Set[String] = Set("spec", "target")

  def listSourceFiles(path: String): Seq[VirtualFile] = listSourceFiles(LocalFile(path))

  def listSourceFiles(path: VirtualFile, level: Int = 0): Seq[VirtualFile] =
    val lst = Seq.newBuilder[VirtualFile]
    if path.isDirectory && !ignoredFolders.contains(path.name) then
      val filesInDir       = path.listFiles
      val hasAnySourceFile = filesInDir.exists(_.isSourceFile)
      // Improve the scan performance by scanning only directories containing any source files
      if hasAnySourceFile then
        for f <- filesInDir do
          lst ++= listSourceFiles(f, level + 1)
    else if path.isSourceFile then
      lst += path
    lst.result()

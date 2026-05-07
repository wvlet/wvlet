package wvlet.lang.compiler

import wvlet.uni.log.LogSupport

/**
  * Cross-platform file I/O for the wvlet compiler. All concrete file/system access lives in the
  * platform-specific [[SourceIOCompat]] trait:
  *   - JVM and Native back the methods with `wvlet.uni.io.IO` (full file I/O)
  *   - Scala.js returns safe stubs (`false` / `Nil` / `0` / throwing for gzip) so that the browser
  *     playground can link the compiler without dragging Node's `os` / `fs` / `path` / `zlib`
  *     modules into the bundle. A future Node-targeted JS consumer can wire in a real filesystem by
  *     calling `wvlet.uni.io.FileSystemInit.init()` from its own entry point.
  */
object SourceIO extends SourceIOCompat with LogSupport:
  val ignoredFolders: Set[String] = Set("spec", "target")

  def listSourceFiles(path: String): Seq[VirtualFile] = listSourceFiles(LocalFile(path))

  def listSourceFiles(path: VirtualFile, level: Int = 0): Seq[VirtualFile] =
    val lst = Seq.newBuilder[VirtualFile]
    if path.isDirectory && !ignoredFolders.contains(path.name) then
      val filesInDir       = path.listFiles
      val hasAnySourceFile = filesInDir.exists(_.isSourceFile)
      // Improve scan performance by descending only into directories that contain a source file
      if hasAnySourceFile then
        for f <- filesInDir do
          lst ++= listSourceFiles(f, level + 1)
    else if path.isSourceFile then
      lst += path
    lst.result()

end SourceIO

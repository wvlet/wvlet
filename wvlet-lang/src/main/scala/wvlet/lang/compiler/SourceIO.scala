package wvlet.lang.compiler

import wvlet.uni.log.LogSupport

/**
  * Cross-platform file I/O for the wvlet compiler. All concrete file/system access lives in the
  * platform-specific [[SourceIOCompat]] trait, which on every supported platform (JVM, Node.js,
  * Native) delegates to `wvlet.uni.io.IO`. Browser embedding is not a supported target —
  * `wvlet-lang.js` is built with `ModuleKind.CommonJSModule` so uni's Node module imports
  * (`os` / `fs` / `path` / `zlib`) resolve correctly.
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

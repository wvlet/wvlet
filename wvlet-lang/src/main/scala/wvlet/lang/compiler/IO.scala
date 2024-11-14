package wvlet.lang.compiler

import wvlet.log.LogSupport

object SourceIO extends IOCompat with LogSupport:
  private val ignoredFolders: Set[String] = Set("spec", "target")

  def listFiles(path: String, level: Int): Seq[String] =
    val f = new java.io.File(path)
    if f.isDirectory then
      if level == 1 && ignoredFolders.contains(f.getName) then
        Seq.empty
      else
        val files         = f.listFiles()
        val hasAnyWvFiles = files.exists(_.getName.endsWith(".wv"))
        if hasAnyWvFiles then
          // Only scan sub-folders if there is any .wv files
          files flatMap { file =>
            listFiles(file.getPath, level + 1)
          }
        else
          Seq.empty
    else if f.isFile && f.getName.endsWith(".wv") then
      Seq(f.getPath)
    else
      Seq.empty

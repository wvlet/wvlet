import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.util.zip.ZipInputStream
import sbt.*

/**
  * Trino stopped publishing io.trino:trino-delta-lake to Maven Central after 476, while the other
  * Trino artifacts (trino-testing, trino-hive, ...) are still published. The connector jar is only
  * shipped inside the plugin zip of each GitHub release, so extract it from there and cache it next
  * to the coursier cache (which CI already caches).
  */
object TrinoPlugins:

  def deltaLakeJar(trinoVersion: String, log: Logger): File =
    val cacheRoot = sys
      .env
      .get("COURSIER_CACHE")
      .map(file)
      .getOrElse(Path.userHome / ".cache" / "coursier" / "v1")
    val releaseUrl =
      s"https://github.com/trinodb/trino/releases/download/${trinoVersion}/trino-delta-lake-${trinoVersion}.zip"
    val jarName = s"io.trino_trino-delta-lake-${trinoVersion}.jar"
    val target  =
      cacheRoot / "https" / "github.com" / "trinodb" / "trino" / "releases" / "download" /
        trinoVersion / jarName
    if !target.exists() then
      log.info(s"Extracting ${jarName} from ${releaseUrl}")
      IO.createDirectory(target.getParentFile)
      val tmp = target.getParentFile / s"${jarName}.tmp"
      val in  = ZipInputStream(URI(releaseUrl).toURL.openStream())
      try
        Iterator
          .continually(in.getNextEntry)
          .takeWhile(_ != null)
          .find(_.getName.endsWith(s"/${jarName}")) match
          case Some(_) =>
            Files.copy(in, tmp.toPath, StandardCopyOption.REPLACE_EXISTING)
          case None =>
            sys.error(s"${jarName} is not found in ${releaseUrl}")
      finally in.close()
      Files.move(tmp.toPath, target.toPath, StandardCopyOption.ATOMIC_MOVE)
    target

end TrinoPlugins

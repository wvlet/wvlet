/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.catalog

import wvlet.uni.cloud.parquet.ColumnStatistics
import wvlet.uni.cloud.parquet.format.FileMetaData
import wvlet.uni.cloud.parquet.Parquet
import wvlet.uni.cloud.parquet.ParquetFile
import wvlet.uni.io.FileSystem

/**
  * Parsed footers of local Parquet files, shared by every [[ParquetSource]] of the process, so that
  * planning and each scan of the same file don't read and parse its footer again (the
  * `parquet_footer_cache` optimization, after DuckDB's `parquet_metadata_cache`).
  *
  * An entry is valid while the file's size and last-modified time stay the same. Both are read
  * before the footer, so a rewrite during the read can't be filed under the new file's key. A
  * symbolic link, a file without a last-modified time, or one modified within [[RecentWriteMillis]]
  * of the read (mtime can be coarse), is opened without caching its footer. At most [[MaxEntries]]
  * files are kept, evicting the least recently used.
  */
object ParquetFooterCache:

  /** Up to how many files keep their footer */
  val MaxEntries: Int = 256

  /** A file modified this recently is parsed again on every open */
  val RecentWriteMillis: Long = 10000

  /**
    * The footer of one version of a file (its `size` and `lastModified` in epoch milliseconds),
    * with the column statistics of its first [[ParquetFile]], computed once
    */
  final class Entry private[ParquetFooterCache] (
      val size: Long,
      val lastModified: Long,
      val metadata: FileMetaData
  ):
    @volatile
    private var stats: Map[String, ColumnStatistics] = null

    /**
      * The statistics of each leaf column, from `f` (a file opened from this entry) the first time
      */
    def columnStatistics(f: ParquetFile): Map[String, ColumnStatistics] =
      var s = stats
      if s == null then
        s = f.columnStatistics
        stats = s
      s

  /**
    * A file opened through the cache: `entry` is set when its footer is cached, `hit` when reused
    */
  case class Opened(file: ParquetFile, entry: Option[Entry], hit: Boolean):
    /** The file's column statistics, kept with its cache entry when there is one */
    def columnStatistics: Map[String, ColumnStatistics] = entry
      .map(_.columnStatistics(file))
      .getOrElse(file.columnStatistics)

  // Access-ordered, so that the eldest entry is the least recently used
  private val entries =
    new java.util.LinkedHashMap[String, Entry](16, 0.75f, true):
      override def removeEldestEntry(e: java.util.Map.Entry[String, Entry]): Boolean =
        size() > MaxEntries

  /** Open the local Parquet file at `path`, reusing its cached footer when the file is unchanged */
  def open(path: String): Opened =
    // Stat before reading the footer, so that the key describes the bytes read or an older version
    val stat =
      try
        val info = FileSystem.info(path)
        // A symbolic link's stat describes the link, not the file it points to
        if info.isFile then
          info.lastModified.map(t => (info.size, t.toEpochMilli))
        else
          None
      catch
        case _: Exception =>
          None
    stat match
      case None =>
        Opened(Parquet.open(path), None, false)
      case Some((size, mtime)) =>
        val cached = entries.synchronized(entries.get(path))
        if cached != null && cached.size == size && cached.lastModified == mtime then
          Opened(Parquet.open(path, cached.metadata), Some(cached), true)
        else
          // Parse outside the lock, so that opening other files doesn't wait
          val f = Parquet.open(path)
          if System.currentTimeMillis() - mtime < RecentWriteMillis then
            entries.synchronized(entries.remove(path))
            Opened(f, None, false)
          else
            val e = Entry(size, mtime, f.metadata)
            entries.synchronized(entries.put(path, e))
            Opened(f, Some(e), false)

  end open

  /** Whether `path` has a cached footer (for tests) */
  def contains(path: String): Boolean = entries.synchronized(entries.containsKey(path))

  /** Drop every cached footer */
  def clear(): Unit = entries.synchronized(entries.clear())

end ParquetFooterCache

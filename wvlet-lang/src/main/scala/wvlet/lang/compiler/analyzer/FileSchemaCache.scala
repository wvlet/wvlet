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
package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.SourceIO
import wvlet.lang.model.RelationType

import java.util.concurrent.ConcurrentHashMap

/**
  * Per-compiler cache of schemas inferred for `from '<file>'` references.
  *
  * Inferring a file schema is the most expensive step of typing a query that reads a local file:
  * parquet/csv open a fresh DuckDB instance, and JSON parses (a sample of) the file. Interactive
  * front-ends (LSP, REPL) re-run the typer on every edit, so without a cache the same file is
  * re-inferred for every keystroke.
  *
  * Entries for local files are validated by the file's last-modified time, mirroring how DuckDB's
  * own `parquet_metadata_cache` / external file cache validate cached metadata by `last_modified`
  * (or ETag) before reuse. A local file that does not exist is never cached, so a file created
  * after a failed lookup is picked up by the next compile. Remote paths (`s3://`, `https://`) have
  * no cheap freshness probe and are cached for the lifetime of the compiler.
  */
class FileSchemaCache:
  private case class Entry(stamp: Long, schema: RelationType)

  private val cache = ConcurrentHashMap[String, Entry]()

  /**
    * Return the cached schema for `path` if it is still valid; otherwise run `infer`, cache its
    * result, and return it.
    */
  def getOrElseUpdate(path: String)(infer: => RelationType): RelationType =
    FileSchemaCache.stampOf(path) match
      case None =>
        infer
      case Some(stamp) =>
        Option(cache.get(path)).filter(_.stamp == stamp) match
          case Some(entry) =>
            entry.schema
          case None =>
            val schema = infer
            cache.put(path, Entry(stamp, schema))
            schema

  /** Number of cached entries (for tests and diagnostics) */
  def size: Int = cache.size

end FileSchemaCache

object FileSchemaCache:
  private val RemoteStamp = -1L

  /**
    * Freshness token for `path`: the local file's mtime, a constant for remote paths, or None if
    * the local file is missing (uncacheable)
    */
  private def stampOf(path: String): Option[Long] =
    if DataFilePath.isRemote(path) then
      Some(RemoteStamp)
    else
      Some(SourceIO.lastUpdatedAt(path)).filter(_ != 0L)

end FileSchemaCache

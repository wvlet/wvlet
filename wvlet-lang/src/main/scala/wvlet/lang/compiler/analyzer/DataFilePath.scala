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
import wvlet.lang.compiler.analyzer.DataFilePath.Compression
import wvlet.lang.compiler.analyzer.DataFilePath.Format

/**
  * Classifies a data-file path referenced by `from '<path>'` into its base format and optional
  * compression suffix, mirroring what DuckDB's replacement scan accepts (e.g. `data.jsonl.gz`,
  * `data.csv.zst`, `data.parquet`).
  *
  * @param format
  *   base file format, e.g. `json`, `jsonl`, `csv`, `parquet`
  * @param compression
  *   compression suffix (`gz`, `zst`), if any
  */
case class DataFilePath(format: Format, compression: Option[Compression]):
  /** True for line-delimited JSON (`.jsonl`, `.ndjson`) */
  def isJsonLines: Boolean = format == Format.JSONL || format == Format.NDJSON

  /** True for JSON-family files (`.json`, `.jsonl`, `.ndjson`) */
  def isJson: Boolean = format == Format.JSON || isJsonLines

  /** True if the file is gzip-compressed */
  def isGzip: Boolean = compression.contains(Compression.GZ)

  /**
    * JSON-family files that are uncompressed or gzip-compressed can be analyzed by the pure-Scala
    * [[JSONAnalyzer]], which works on every platform (including Scala.js without DuckDB). Other
    * formats and compressions (e.g. `.zst`) need DuckDB.
    */
  def canUseJsonAnalyzer: Boolean = isJson && (compression.isEmpty || isGzip)

object DataFilePath:
  /**
    * Base file formats readable by DuckDB. `compressible` formats may carry an extra
    * [[Compression]] suffix (e.g. `.csv.gz`); Parquet has its own internal compression.
    */
  enum Format(val extension: String, val compressible: Boolean):
    case JSON    extends Format("json", true)
    case JSONL   extends Format("jsonl", true)
    case NDJSON  extends Format("ndjson", true)
    case CSV     extends Format("csv", true)
    case TSV     extends Format("tsv", true)
    case PARQUET extends Format("parquet", false)

  /** Compression suffixes DuckDB auto-detects for CSV/JSON files */
  enum Compression(val extension: String):
    case GZ  extends Compression("gz")
    case ZST extends Compression("zst")

  private val formatByExtension: Map[String, Format] =
    Format.values.map(f => f.extension -> f).toMap

  private val compressionByExtension: Map[String, Compression] =
    Compression.values.map(c => c.extension -> c).toMap

  /**
    * Parse the file extension of `path`. Returns None if it is not a recognized data file, e.g.
    * `.wv`, `.sql`, or a path without an extension.
    */
  def parse(path: String): Option[DataFilePath] =
    // Drop URL query/fragment (e.g. S3 presigned URLs) before inspecting the extension
    val pathWithoutQuery = path.takeWhile(c => c != '?' && c != '#')
    val parts            = SourceIO.fileName(pathWithoutQuery).toLowerCase.split('.').toList
    parts.reverse match
      case ext :: format :: rest if compressionByExtension.contains(ext) && hasStem(rest) =>
        formatByExtension
          .get(format)
          .filter(_.compressible)
          .map(f => DataFilePath(f, compressionByExtension.get(ext)))
      case ext :: rest if hasStem(rest) =>
        formatByExtension.get(ext).map(f => DataFilePath(f, None))
      case _ =>
        None

  /** True for remote locations (`s3://`, `https://`, …) that are read by the query engine itself */
  def isRemote(path: String): Boolean = path.contains("://")

  /** The file name must have a non-empty stem before the extension (`.json` is a dotfile) */
  private def hasStem(stemParts: List[String]): Boolean = stemParts.exists(_.nonEmpty)

end DataFilePath

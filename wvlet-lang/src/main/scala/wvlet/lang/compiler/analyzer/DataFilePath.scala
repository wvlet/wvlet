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

/**
  * Classifies a data-file path referenced by `from '<path>'` into its base format and optional
  * compression suffix, mirroring what DuckDB's replacement scan accepts (e.g. `data.jsonl.gz`,
  * `data.csv.zst`, `data.parquet`).
  *
  * @param format
  *   base file format, e.g. `json`, `jsonl`, `csv`, `parquet`
  * @param compression
  *   compression suffix without the dot (`gz`, `zst`), if any
  */
case class DataFilePath(format: DataFilePath.Format, compression: Option[String]):
  /** True for JSON-family files (`.json`, `.jsonl`, `.ndjson`) */
  def isJson: Boolean = format.isJson

  /** True for line-delimited JSON (`.jsonl`, `.ndjson`) */
  def isJsonLines: Boolean =
    format == DataFilePath.Format.JSONL || format == DataFilePath.Format.NDJSON

object DataFilePath:
  enum Format(val extension: String, val isJson: Boolean):
    case JSON    extends Format("json", true)
    case JSONL   extends Format("jsonl", true)
    case NDJSON  extends Format("ndjson", true)
    case CSV     extends Format("csv", false)
    case TSV     extends Format("tsv", false)
    case PARQUET extends Format("parquet", false)

  /** Compression suffixes DuckDB auto-detects for CSV/JSON files */
  private val compressionSuffixes = Seq("gz", "zst")

  /**
    * Parse the file extension of `path`. Returns None if it is not a recognized data file, e.g.
    * `.wv`, `.sql`, or a path without an extension.
    */
  def parse(path: String): Option[DataFilePath] =
    val fileName = path.substring(path.lastIndexOf('/') + 1).toLowerCase
    val parts    = fileName.split('.').toList
    parts.reverse match
      case ext :: compression :: rest if compressionSuffixes.contains(ext) && hasStem(rest) =>
        Format.values.find(_.extension == compression).map(f => DataFilePath(f, Some(ext)))
      case ext :: rest if hasStem(rest) =>
        Format.values.find(_.extension == ext).map(f => DataFilePath(f, None))
      case _ =>
        None

  /** The file name must have a non-empty stem before the extension (`.json` is a dotfile) */
  private def hasStem(stemParts: List[String]): Boolean = stemParts.exists(_.nonEmpty)

  /** True if the path points to a data file DuckDB can scan directly */
  def isDataFile(path: String): Boolean = parse(path).isDefined

end DataFilePath

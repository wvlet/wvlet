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
package wvlet.lang.tablestore.format

import wvlet.lang.tablestore.{DataRow, TableStoreException}
import wvlet.uni.io.IO
import wvlet.uni.json.{JSON, JSONParseException}
import wvlet.uni.json.JSON.JSONObject

/** newline-delimited JSON — the raw ingest format */
object JsonlFile:
  def encode(rows: Seq[DataRow]): Array[Byte] =
    val sb = new StringBuilder
    rows.foreach { row =>
      // Compact form — pretty-printed values would span multiple lines and break the format
      sb.append(row.toJSON)
      sb.append('\n')
    }
    sb.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)

  /** Parse JSONL content into rows; blank lines are skipped */
  def decode(bytes: Array[Byte]): Seq[DataRow] =
    val text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
    text
      .linesIterator
      .zipWithIndex
      .flatMap { (line, idx) =>
        if line.trim.isEmpty then
          None
        else
          try
            JSON.parse(line) match
              case obj: JSONObject =>
                Some(obj)
              case other =>
                throw TableStoreException(
                  s"Expected a JSON object per line, got ${JSON.format(other)} at line ${idx + 1}"
                )
          catch
            case e: JSONParseException =>
              throw TableStoreException(s"Malformed JSONL at line ${idx + 1}: ${line.take(100)}", e)
      }
      .toSeq

  def read(path: String): Seq[DataRow] = decode(IO.readBytes(path))

  def write(path: String, rows: Seq[DataRow]): Unit = IO.writeBytes(path, encode(rows))
end JsonlFile

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

import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.uni.log.LogSupport

import scala.collection.immutable.ListMap

object JSONAnalyzer extends LogSupport:
  /**
    * Infer the schema of a JSON (`.json`), or newline-delimited JSON (`.jsonl`, `.ndjson`) file.
    * Gzip-compressed files (`.gz` suffix) are decompressed on the fly.
    */
  def analyzeJSONFile(path: String): RelationType =
    val dataFile = DataFilePath.parse(path).getOrElse(DataFilePath(DataFilePath.Format.JSON, None))
    analyzeJSONFile(path, dataFile)

  /** Same as [[analyzeJSONFile]] for a path whose extension has already been classified */
  def analyzeJSONFile(path: String, dataFile: DataFilePath): RelationType =
    val json =
      if dataFile.isGzip then
        SourceIO.readGzipAsString(path)
      else
        SourceIO.readAsString(path)
    analyzeJSONContent(json, dataFile.isJsonLines)

  /**
    * Maximum number of JSON Lines records inspected for schema inference. Type counts converge long
    * before this, and it bounds the parse cost for large `.jsonl` files
    */
  private val maxJsonLinesSample = 10000

  private[analyzer] def analyzeJSONContent(json: String, isJsonLines: Boolean): RelationType =
    debug(json)
    val jsonValue =
      if isJsonLines then
        // Each non-blank line is a standalone JSON value; treat them as one array of records
        val records = json
          .linesIterator
          .zipWithIndex
          .map((line, i) => (line.trim, i + 1))
          .filter(_._1.nonEmpty)
          .take(maxJsonLinesSample)
          .map { (line, lineNumber) =>
            try
              JSON.parse(line)
            catch
              case e: Exception =>
                throw StatusCode
                  .SYNTAX_ERROR
                  .newException(s"Invalid JSON at line ${lineNumber}: ${e.getMessage}", e)
          }
        JSONArray(records.toIndexedSeq)
      else
        JSON.parse(json)
    guessSchema(jsonValue)

  class TypeCountMap:
    private var map                       = Map.empty[DataType, Int]
    def mostFrequentType: DataType        = map.maxBy(_._2)._1
    override def toString: String         = map.toString()
    def observe(dataType: DataType): Unit =
      val count = map.getOrElse(dataType, 0)
      map = map.updated(dataType, count + 1)

  def guessSchema(json: JSONValue): RelationType =
    // json path -> (data type -> count)
    // Use ListMap to keep the order of the columns
    var schema = ListMap.empty[String, TypeCountMap]

    def traverse(path: String, v: JSONValue): Unit =
      v match
        case a: JSONArray =>
          a.v
            .foreach: x =>
              traverse(path, x)
        case o: JSONObject =>
          o.v
            .foreach: (k, v) =>
              val nextPath =
                if path.isEmpty then
                  k
                else
                  s"${path}.${k}"
              traverse(nextPath, v)
        case _ =>
          val dataType     = guessDataType(v)
          val typeCountMap = schema.getOrElse(path, TypeCountMap())
          typeCountMap.observe(dataType)
          schema = schema.updated(path, typeCountMap)

    traverse("", json)
    val dataTypes = schema.map: (k, typeMap) =>
      val mostFrequentType = typeMap.mostFrequentType
      NamedType(Name.termName(k), mostFrequentType)

    SchemaType(None, Name.typeName(RelationType.newRelationTypeName), dataTypes.toList)

  end guessSchema

  private def guessDataType(v: JSONValue): DataType =
    v match
      case JSONNull =>
        DataType.NullType
      case b: JSONBoolean =>
        DataType.BooleanType
      case s: JSONString =>
        DataType.StringType
      case i: JSONLong =>
        DataType.LongType
      case d: JSONDouble =>
        DataType.DoubleType
      case _ =>
        DataType.AnyType

end JSONAnalyzer

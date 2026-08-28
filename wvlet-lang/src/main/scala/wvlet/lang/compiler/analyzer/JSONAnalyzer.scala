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
import wvlet.uni.json.JSONContext
import wvlet.uni.json.JSONScanner
import wvlet.uni.json.JSONSource
import wvlet.uni.json.JSONValueBuilder
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.uni.log.LogSupport

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

object JSONAnalyzer extends LogSupport:

  /**
    * Maximum number of records inspected when inferring a schema: the elements of a top-level JSON
    * array, or the lines of a JSON Lines file. Matches DuckDB's `read_json(sample_size)` default so
    * that wvlet's compile-time schema agrees with the schema DuckDB itself binds at query time.
    */
  val DefaultSampleSize: Int = 20480

  /**
    * Infer the schema of a JSON (`.json`), or newline-delimited JSON (`.jsonl`, `.ndjson`) file.
    * Gzip-compressed files (`.gz` suffix) are decompressed on the fly.
    */
  def analyzeJSONFile(path: String): RelationType =
    val dataFile = DataFilePath.parse(path).getOrElse(DataFilePath(DataFilePath.Format.JSON, None))
    analyzeJSONFile(path, dataFile)

  /**
    * Same as [[analyzeJSONFile]] for a path whose extension has already been classified. Only the
    * first `sampleSize` records are inspected; the rest of the file is skipped.
    */
  def analyzeJSONFile(
      path: String,
      dataFile: DataFilePath,
      sampleSize: Int = DefaultSampleSize
  ): RelationType =
    // The whole file is loaded (a bounded prefix read via uni's readChunks is a possible
    // follow-up). Loading bytes is cheap compared to building JSON values for every record,
    // which is what the sampling avoids.
    val bytes =
      if dataFile.isGzip then
        SourceIO.readGzipAsBytes(path)
      else
        SourceIO.readAsBytes(path)
    if dataFile.isJsonLines then
      analyzeJsonLines(String(bytes, StandardCharsets.UTF_8), sampleSize)
    else
      guessSchema(parseSample(JSONSource.fromBytes(bytes), sampleSize))

  private[analyzer] def analyzeJSONContent(
      json: String,
      isJsonLines: Boolean,
      sampleSize: Int = DefaultSampleSize
  ): RelationType =
    if isJsonLines then
      analyzeJsonLines(json, sampleSize)
    else
      guessSchema(parseSample(JSONSource.fromString(json), sampleSize))

  private def analyzeJsonLines(json: String, sampleSize: Int): RelationType =
    // Each non-blank line is a standalone JSON value; treat them as one array of records
    val records = json
      .linesIterator
      .zipWithIndex
      .map((line, i) => (line.trim, i + 1))
      .filter(_._1.nonEmpty)
      .take(sampleSize)
      .map { (line, lineNumber) =>
        try
          JSON.parse(line)
        catch
          case e: Exception =>
            throw StatusCode
              .SYNTAX_ERROR
              .newException(s"Invalid JSON at line ${lineNumber}: ${e.getMessage}", e)
      }
    guessSchema(JSONArray(records.toIndexedSeq))

  /**
    * Parse `source`, keeping at most `sampleSize` elements of a top-level array. Scanning stops as
    * soon as the sample is complete, so records past the limit are never materialized.
    */
  private def parseSample(source: JSONSource, sampleSize: Int): JSONValue =
    val root = SamplingBuilder(sampleSize)
    try
      JSONScanner.scan(source, root)
      root.result
    catch
      case SampleLimitReached =>
        root.result

  /** Control-flow signal for aborting the scanner once the sample is complete */
  private object SampleLimitReached extends ControlThrowable

  /**
    * Root builder (the equivalent of `JSONValueBuilder.singleContext`) that hands the top-level
    * array to a [[LimitedArrayContext]]. Nested containers are unaffected: the scanner creates
    * their contexts from the enclosing context, never from this root.
    */
  private class SamplingBuilder(limit: Int) extends JSONValueBuilder:
    private var holder: Option[JSONValue]                 = None
    private var topLevelArray: LimitedArrayContext | Null = null

    override def add(v: JSONValue): Unit = holder = Some(v)

    // When the root is an array, read it from the array context so that an aborted scan (which
    // never closes the context) still yields the sampled prefix
    override def result: JSONValue =
      topLevelArray match
        case null =>
          holder.getOrElse(throw IllegalStateException("no JSON value was scanned"))
        case ctx =>
          ctx.result

    override def arrayContext(s: JSONSource, start: Int): JSONContext[JSONValue] =
      val ctx = LimitedArrayContext(limit)
      topLevelArray = ctx
      ctx

  /**
    * Array context that aborts the scan once `limit` elements have been added. Extends
    * [[JSONValueBuilder]] so that element contexts (objects, nested arrays) and scalar events
    * created by the inherited builder methods all report their values through `add`.
    */
  private class LimitedArrayContext(limit: Int) extends JSONValueBuilder:
    private val sampled = ArrayBuffer.empty[JSONValue]

    override def isObjectContext: Boolean = false

    override def add(v: JSONValue): Unit =
      if sampled.size >= limit then
        throw SampleLimitReached
      sampled += v

    override def result: JSONValue = JSONArray(sampled.toIndexedSeq)

    // The root reads this context's result directly, so nothing to publish on close
    override def closeContext(s: JSONSource, end: Int): Unit = ()

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

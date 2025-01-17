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

import wvlet.airframe.json.JSON
import wvlet.airframe.json.JSON.*
import wvlet.lang.compiler.{SourceIO, Name}
import wvlet.lang.model.DataType.{NamedType, SchemaType}
import wvlet.lang.model.{DataType, RelationType}
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

object JSONAnalyzer extends LogSupport:
  def analyzeJSONFile(path: String): RelationType =
    val json = SourceIO.readAsString(path)
    debug(json)
    val jsonValue = JSON.parse(json)

    guessSchema(jsonValue)

  class TypeCountMap:
    private var map                = Map.empty[DataType, Int]
    def mostFrequentType: DataType = map.maxBy(_._2)._1
    override def toString: String  = map.toString()
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

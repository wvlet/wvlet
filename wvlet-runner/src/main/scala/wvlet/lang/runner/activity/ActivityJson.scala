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
package wvlet.lang.runner.activity

import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString

/**
  * JSON encoding of activity inputs and outputs as flat objects of string values: the shape the
  * task-queue wire protocol carries and the item store persists
  */
object ActivityJson:

  /** Encode a map as a JSON object, rendering each value with `toString` */
  def encode(values: Map[String, Any]): String =
    JSONObject(values.toSeq.map((k, v) => k -> JSONString(v.toString))).toJSON

  /**
    * Decode a JSON object into a map of strings. Non-string values keep their JSON rendering; a
    * non-object document decodes to an empty map
    */
  def decode(json: String): Map[String, String] =
    JSON.parse(json) match
      case JSONObject(entries) =>
        entries
          .map {
            case (k, JSONString(s)) =>
              k -> s
            case (k, other) =>
              k -> other.toJSON
          }
          .toMap
      case _ =>
        Map.empty

end ActivityJson

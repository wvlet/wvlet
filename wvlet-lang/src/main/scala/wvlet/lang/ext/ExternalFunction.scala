/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.ext

import wvlet.uni.json.JSON.JSONObject

/**
  * A code-backed function that implements a Wvlet `def ... = native` declaration. The `.wv` file
  * declares only the signature; the implementation is found by [[name]] at execution time.
  */
trait ExternalFunction:
  /** The function name, matching the name of the `def` declared in Wvlet */
  def name: String

/**
  * A scalar function: evaluated once per input row. Arguments arrive in declaration order as
  * plain values (String, Long, Double, Boolean, or null), and the result is converted to JSON
  */
trait ScalarFunction extends ExternalFunction:
  def eval(args: Seq[Any]): Any

/**
  * A table function: receives the input rows and returns a single result object.
  *
  *   - `rows` (array of objects) becomes the output relation; `rows_path` may point to a JSON-lines
  *     file instead for large outputs
  *   - every other top-level field is metadata: logged and recorded, never part of the relation
  *   - an object with neither `rows` nor `rows_path` is itself a one-row relation
  */
trait TableFunction extends ExternalFunction:
  /**
    * @param args
    *   the call arguments by parameter name
    * @param input
    *   the input rows, streamed; empty for `from f(args)`
    */
  def apply(args: JSONObject, input: Iterator[JSONObject]): JSONObject

/**
  * Service-provider interface for plugin jars. Implementations are discovered with
  * `java.util.ServiceLoader` (register the class name in
  * `META-INF/services/wvlet.lang.ext.FunctionProvider`)
  */
trait FunctionProvider:
  def functions: Seq[ExternalFunction]

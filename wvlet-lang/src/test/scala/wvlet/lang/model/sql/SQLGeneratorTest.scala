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
package wvlet.lang.model.sql

import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.model.plan.Query
import wvlet.airspec.AirSpec

class SQLGeneratorTest extends AirSpec:
  pending("context-based sql generation for FunctionApply needs to be supported")
  test("generate SQL from behavior.wv"):
    val plan = WvletParser(
      CompilationUnit(SourceFile.fromResource("spec/cdp_behavior/src/behavior.wv"))
    ).parse()
    plan.traverse { case q: Query =>
      val sql = SQLGenerator.toSQL(q)
      debug(q)
      debug(sql)
    }

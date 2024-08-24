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
package wvlet.lang.compiler.parser

import wvlet.lang.compiler.CompilationUnit
import wvlet.airspec.AirSpec

class StdLibTest extends AirSpec:
  test("parse stdlib") {
    val units = CompilationUnit.fromPath("wvlet-stdlib/module/standard")
    units.foreach { u =>
      test(s"Parse ${u.sourceFile.fileName}") {
        val plan = WvletParser(u).parse()
        debug(plan.pp)
      }
    }
  }

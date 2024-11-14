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

import wvlet.lang.compiler.{CompilationUnit, SourceFile}
import wvlet.airspec.AirSpec
import wvlet.lang.model.plan.ModelDef
import wvlet.lang.compiler.ContextUtil.*

class WvletParserTest extends AirSpec:
  test("parse"):
    WvletParser(CompilationUnit.fromString("from A select _")).parse()

  test("model span"):
    given unit: CompilationUnit = CompilationUnit.fromString(s"""model A =
        |  from x
        |end
        |""".stripMargin)
    val plan = WvletParser(unit).parse()

    var found = false
    plan.traverseOnce { case m: ModelDef =>
      found = true
      val startLoc = unit.nodeLocationAt(m.span)
      startLoc.line shouldBe 1
      startLoc.column shouldBe 1
      val endLoc = unit.endNodeLocationAt(m.span)
      endLoc.line shouldBe 3
      endLoc.column shouldBe 4
    }
    found shouldBe true

  test("parse basic queries"):
    val plans = ParserPhase.parseSourceFolder("spec/basic")
    plans.foreach: p =>
      debug(p.pp)

  test("parse cdp_simple queries"):
    val plans = ParserPhase.parseSourceFolder("spec/cdp_simple")
    plans.foreach: p =>
      debug(p.pp)

  test("parse cdp_behavior queries"):
    val plans = ParserPhase.parseSourceFolder("spec/cdp_behavior")
    plans.foreach: p =>
      debug(p.pp)

  test("tpch") {
    val plans = ParserPhase.parseSourceFolder("spec/tpch")
    plans.foreach: p =>
      debug(p.pp)
  }

end WvletParserTest

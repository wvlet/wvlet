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

import wvlet.lang.compiler.{Compiler, Name}
import wvlet.lang.model.plan.{Query, Subscribe}
import wvlet.airspec.AirSpec

class AnalyzerTest extends AirSpec:

  test("analyze stdlib") {
    val result     = Compiler.default("spec/empty").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)

    val units = result.context.global.getAllCompilationUnits
    units shouldNotBe empty
    val files = units.map(_.sourceFile.fileName)
    files shouldContain "int.wv"
    files shouldContain "string.wv"
  }

  test("analyze cdp_simple plan") {
    val result     = Compiler.default("spec/cdp_simple").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      trace(p.pp)
  }

  test("analyze basic") {
    val result     = Compiler.default("spec/basic").compile()
    val typedPlans = result.typedPlans
    typedPlans.map: p =>
      debug(p.pp)
    debug(result.context.scope.getAllEntries)

    val tpe = result.context.scope.lookupSymbol(Name.typeName("person"))
    debug(tpe.get)
  }

end AnalyzerTest

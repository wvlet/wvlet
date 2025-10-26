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
import wvlet.lang.model.plan.PackageDef
import wvlet.lang.model.plan.TableRef
import wvlet.lang.model.plan.Query
import wvlet.airspec.AirSpec

class ParserTest extends AirSpec:
  test("parse package") {
    val p = WvletParser(CompilationUnit.fromWvletString("""package org.app.example
        |from A
        |""".stripMargin))
    val plan = p.parse()
    debug(plan)
    plan shouldMatch { case p: PackageDef =>
      p.statements shouldMatch { case List(Query(t: TableRef, _)) =>
        t.name.fullName shouldBe "A"
      }
    }
  }

  test("parse json scan") {
    val p = WvletParser(CompilationUnit.fromWvletString("""-- sample query
        |from 'sample.json'""".stripMargin))
    val plan = p.parse()
    debug(plan)
  }

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
import wvlet.lang.model.plan.CallTool
import wvlet.lang.model.plan.PackageDef
import wvlet.lang.model.plan.TableRef
import wvlet.lang.model.plan.Query
import wvlet.uni.test.UniTest

class ParserTest extends UniTest:
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

  test("should parse a call statement with named tool arguments") {
    val p = WvletParser(
      CompilationUnit.fromWvletString(
        """call slack.post_message(channel: '#reports', text: 'hello')"""
      )
    )
    val plan = p.parse()
    debug(plan)
    plan shouldMatch { case p: PackageDef =>
      p.statements shouldMatch { case List(Query(c: CallTool, _)) =>
        c.connectorName.fullName shouldBe "slack"
        c.toolName.fullName shouldBe "post_message"
        c.args.flatMap(_.name.map(_.name)) shouldBe List("channel", "text")
      }
    }
  }

  test("should parse a call statement with no arguments followed by query operators") {
    val p = WvletParser(CompilationUnit.fromWvletString("""call slack.refresh()
        |select status""".stripMargin))
    val plan = p.parse()
    debug(plan)
    var found = false
    plan.traverse { case c: CallTool =>
      found = true
      c.args shouldBe Nil
    }
    found shouldBe true
  }

end ParserTest

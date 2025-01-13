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
package wvlet.lang.cli

import wvlet.airspec.AirSpec

class WvletREPLMainTest extends AirSpec:
  test("detect sbt") {
    WvletMain.isInSbt shouldBe true
  }

  test("--version") {
    WvletREPLMain.main("--version")
  }

  test("help") {
    WvletREPLMain.main("-c 'help'")
  }

  test("model in the working folder") {
    WvletREPLMain.main("-w spec/basic/model -c 'from person_filter(2)'")
  }

  test("def new model") {
    WvletREPLMain.main(
      "-w spec/basic/model -c 'model m(v:int) = from person where id = v end' -c 'from m(1)'"
    )
  }

  test("show models") {
    WvletREPLMain.main("-w spec/basic/model -c 'show models'")
    WvletREPLMain.main("-w spec/basic/model -c 'show models limit 1'")
    WvletREPLMain.main("-w spec/basic/model -c 'show models' -c 'show models limit 5'")
  }

  test("select group index") {
    WvletREPLMain.main("-w spec/basic/model -c 'from person group by age / 10 select _1'")
  }

  test("clip") {
    WvletREPLMain.main("-w spec/basic/model -c 'from person' -c 'clip'")
  }

  test("clip-result") {
    WvletREPLMain.main("-w spec/basic/model -c 'from person' -c 'clip-result'")
  }

  test("clip-query") {
    WvletREPLMain.main("-w spec/basic/model -c 'from person' -c 'clip-query'")
  }

  test("rows") {
    WvletREPLMain.main("-w spec/basic/model -c 'rows 2' -c 'from person'")
  }

  test("col-width") {
    WvletREPLMain.main("-w spec/basic/model -c 'col-width 10' -c 'from person'")
  }

  test("run test") {
    WvletREPLMain.main("""-c 'select 1 test 1 should be 1'""")
  }

  test("run execute") {
    WvletREPLMain.main("""-c 'execute sql"select 1"'""")
  }

end WvletREPLMainTest

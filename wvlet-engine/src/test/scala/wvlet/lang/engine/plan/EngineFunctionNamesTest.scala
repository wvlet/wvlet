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
package wvlet.lang.engine.plan

import wvlet.lang.compiler.WvletEngineFunctionNames
import wvlet.uni.test.UniTest

/**
  * The compiler checks calls compiled for the `wvlet` dialect against a generated copy of the
  * engine's function names; this test fails when the copy drifts from [[EngineFunctions.allNames]]
  */
class EngineFunctionNamesTest extends UniTest:

  test("the compiler's engine function list should match the names the engine binds") {
    val expected = EngineFunctions.allNames
    val actual   = WvletEngineFunctionNames.names
    val missing  = (expected -- actual).toSeq.sorted
    val extra    = (actual -- expected).toSeq.sorted
    if missing.nonEmpty || extra.nonEmpty then
      val cmd = "./sbt \"engineJVM/Test/runMain wvlet.lang.engine.EngineFunctionNamesGenerator\""
      fail(
        s"WvletEngineFunctionNames is out of date (missing: ${missing.mkString(", ")}; " +
          s"stale: ${extra.mkString(", ")}). Regenerate it with: ${cmd}"
      )
  }

  test("every engine function name should be lowercase") {
    EngineFunctions.allNames.filter(n => n != n.toLowerCase) shouldBe Set.empty
  }

end EngineFunctionNamesTest

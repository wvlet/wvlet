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
package wvlet.lang.runner.cli

import wvlet.airspec.AirSpec

class WvletCliTest extends AirSpec:
  test("help") {
    WvletCli.main("--help")
  }

  test("q1"):
    WvletCli.main("run spec/basic/src/q1.wv")

  test("q2"):
    WvletCli.main("run spec/basic/src/q2.wv")

  test("query"):
    WvletCli.main("run spec/basic/src/query.wv")

  test("model1-q1"):
    WvletCli.main("run spec/model1/src/q1.wv")

  for i <- 1 to 22 do
    test(s"tpch-q${i}.wv"):
      info(s"Running tpch-q${i}.wv")
      WvletCli.main(s"run --tpch spec/tpch/src/q${i}.wv")

end WvletCliTest

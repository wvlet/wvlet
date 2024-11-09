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

class WvletRunTest extends AirSpec:
  test("help") {
    WvletMain.main("run --help")
  }

  test("q1") {
    WvletMain.main("run -w spec/basic -f q1.wv")
  }

  test("q2") {
    WvletMain.main("run -w spec/basic -f q2.wv")
  }

  test("query") {
    WvletMain.main("run -w spec/basic -f query.wv")
  }

  test("model1-q1") {
    WvletMain.main("run -w spec/basic -f model/model1.wv")
  }

end WvletRunTest
